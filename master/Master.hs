{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Lib
import System.IO as IO
import Network
import FarmHash as HASH

import Data.ByteString.Lazy as B
import Data.ByteString.Lazy.Char8 as C8
import Data.Tuple.Utils

import qualified Data.List as L
import qualified Data.Map.Strict as Map
import qualified Data.Set as S
import qualified Data.Sequence as Seq
import qualified Data.Foldable as Foldable

import Control.Lens

import Data.Maybe

import Control.Concurrent.MState
import Control.Monad.State

import Control.Exception
import Control.Monad.Reader
import Control.Monad.Catch as Catch
import Control.Concurrent
import Control.Concurrent.Chan
import Control.Concurrent.Thread.Delay

import System.Directory as DIR

import qualified KVProtocol (getMessage, sendMessage, kV_TIMEOUT_MICRO)
import KVProtocol hiding (getMessage, sendMessage, connectToMaster)

import Debug.Trace

import qualified Utils as U

type SlvId = Int

-- type KVMap a = Map.Map KVTxnId (Set.Set a)

--https://hackage.haskell.org/package/lrucache-1.2.0.0/docs/Data-Cache-LRU.html
data MasterState = MasterState {
                  socket :: Socket
                , channel :: Chan KVMessage
                , cfg :: Lib.Config
                  --the votemap contains as a key the txn id, and has as a value
                  --the set of slaves that have responded with a READY vote
                , _txs :: Map.Map KVTxnId TX
                }
                   
data TXState = VOTE | ACK | RESPONSE
  deriving (Show, Eq, Ord)

data TX = TX {
  _txState :: TXState,
  _responded :: S.Set SlvId,
  _timeout :: KVTime,
  message :: KVMessage
}
 deriving (Show)

data KVSlave = KVSlave { slvID :: Int , host :: HostName,  port :: PortID }

instance Show (MasterState) where
  show (MasterState skt _ cfg txs) = 
    show skt ++ show cfg ++ show txs

makeLenses ''MasterState
makeLenses ''TX

-- Entry Point
main :: IO ()
main = Lib.parseArguments >>= \args -> case args of
  Nothing -> Lib.printUsage -- an error occured 
  Just c -> do
    --INITIALIZE THE DATABASE STATE--
    --DANGEROUS CODE BELOW--
    DIR.removeDirectoryRecursive "database"
    DIR.createDirectory "database"
    DIR.createDirectory "database/logs"
    ---------------------------------
    ms <- MasterState <$> listenOn (Lib.masterPortId c) <*> newChan
    execMState initMaster $ ms c Map.empty
    return ()

-- Initialization for Master Node
initMaster :: MState MasterState IO ()
initMaster = do
  forkM_ $ listen
  forkM_ $ timeoutThread
  processMessages

-- Listen and write to thread-safe channel
listen :: MState MasterState IO ()
listen = get >>= \s -> do
  liftIO $ do
    let process = either (IO.putStr . show . (++ "\n")) (writeChan $ channel s)
    Catch.bracket
      (accept $ socket s) 
      (hClose . fst3)
      ((>>= process) . KVProtocol.getMessage . fst3)
  listen

processMessages :: MState MasterState IO ()
processMessages = get >>= \s -> do
  message <- liftIO $ readChan $ channel s
  forkM_ $ processMessage message
  processMessages

processMessage :: KVMessage -> MState MasterState IO ()
processMessage (KVVote _ _ VoteAbort _) = undefined
--TODO, when receive an abort send an abort to everyone.

processMessage (KVVote tid sid VoteReady request) = do
  slaveResponded tid sid
  commit <- isComplete tid
  timeout <- timedOut tid

  when (commit || timeout) $ modifyM_ $ \s -> do
    let tx = (txState .~ ACK) <$> (responded .~ S.empty) <$> lookupTX tid s
    updateTX tid tx s

    -- let tx = lookupTX tid s
    --     tx' = fromJust tx
    -- if (isJust tx) then updateTX tid (tx' { responded = S.empty, txState = ACK }) s else s

  if commit then sendDecisionToRing (KVDecision tid DecisionCommit request) 
  else when timeout $ sendDecisionToRing (KVDecision tid DecisionAbort request)

processMessage kvMsg@(KVResponse tid sid _) = do
  slaveResponded tid sid
  --complete <- isComplete tid
  --when complete $ do

  --forward first response to client, delete from map
  complete <- isComplete tid
  if complete then sendMsgToClient kvMsg >>= (\_ -> clearTX tid) else (liftIO $ return ())

  -- TODO: add timeout for get reqs
  -- addAck tid sid
  -- complete <- ackComplete tid
  -- if complete then clearAcksTX tid else return ()

processMessage kvMsg@(KVRequest tid req) = do
  now <- liftIO U.currentTimeMicro
  let txstate = case req of
        GetReq{} -> RESPONSE
        PutReq{} -> VOTE
        DelReq{} -> VOTE
  --Communicating with shard for the first time, keep track of this in the timeout map.
  modifyM_ $ \s -> addTX tid (TX txstate S.empty now kvMsg) s
  sendMsgToRing kvMsg

processMessage (KVAck tid (Just sid) maybeSuccess) = do  
  slaveResponded tid sid
  complete <- isComplete tid
  when complete $ do
    clearTX tid
    sendMsgToClient $ KVAck tid Nothing maybeSuccess

processMessage kvMsg@(KVRegistration txn_id hostName portId) = do
  let cfgTuple = (hostName, PortNumber $ toEnum portId)
  s <- get
  let oldConfig = cfg s
      oldClientCfg = Lib.clientConfig oldConfig       
      newClientCfg = oldClientCfg ++ [cfgTuple]
      newConfig = oldConfig { clientConfig = newClientCfg }
      clientId = Prelude.length newClientCfg - 1

  modifyM_ $ \s -> do s { cfg = newConfig }
  clientH <- liftIO $ uncurry connectTo cfgTuple
  liftIO $ KVProtocol.sendMessage clientH $ KVAck (clientId, snd txn_id) (Just clientId) Nothing
  liftIO $ hClose clientH

processMessage _ = undefined

timeoutThread :: MState MasterState IO ()
timeoutThread = get >>= \s -> do
  --liftIO $ IO.putStrLn "[!] TIMING OUT..."
--  liftIO $ traceIO "Timing out! in timeout thread!"
  -- TODO: timeout transactions individually
   -- mapM_ (\(txn_id,(ts,msg)) -> do
   --         if (now - KVProtocol.kV_TIMEOUT_MICRO >= ts)
   --         then sendDecisionToRing (KVDecision txn_id DecisionAbort (request msg))
   --         else liftIO $ return ()
   --       ) Map.toList $ txs s 
  now <- liftIO $ U.currentTimeMicro
  mapM_ (\(tid, tx) ->
          if (now - KVProtocol.kV_TIMEOUT_MICRO >= tx ^. timeout) 
          then do
            if (tx ^. txState == ACK)
              then sendDecisionToRing (KVDecision tid DecisionAbort (request $ message tx))
            else sendMsgToClient (KVResponse tid (-1) (KVFailure (C8.pack "Timeout"))) -- else is VOTE or RESPONSE
          else return ()
        ) $ Map.toList $ s ^. txs
  --todo, may need to put this into modifyM_ 

  -- mapM clearTX (L.map fst timedOutTxns)

  liftIO $ delay (KVProtocol.kV_TIMEOUT_MICRO)
  timeoutThread

slaveResponded :: KVTxnId -> SlvId -> MState MasterState IO ()
slaveResponded tid slvId = modifyM_ $ \s ->
  updateTX tid (lookupTX tid s <&> responded %~ S.insert slvId) s

      -- if isJust tx then tx' &  else s


  --     tx' = fromJust tx
  -- if isJust tx then updateTX tid (tx' { responded = S.insert slvId (responded tx') }) s else s 

-- todo: change bakc to Ord a => KVMap a later
isComplete :: KVTxnId -> MState MasterState IO Bool
isComplete tid = get >>= \s -> pure $ (S.size <$> (^. responded) <$> lookupTX tid s ) == Just 2

  -- let tx = lookupTX tid s
  --     tx' = fromJust tx
  -- if isJust tx then return $ S.size (responded tx') == 2 else return False

timedOut :: KVTxnId -> MState MasterState IO Bool
timedOut tid = get >>= \s -> liftIO $ do
  let tx = lookupTX tid s
      tx' = fromJust tx
  now <- U.currentTimeMicro
  if isJust tx
  then return $ now - KVProtocol.kV_TIMEOUT_MICRO >= tx' ^. timeout
  else return False

clearTX :: KVTxnId -> MState MasterState IO ()
clearTX tid = modifyM_ $ txs %~ Map.delete tid

-- \s -> s { txs = Map.delete tid $ txs s }
 -- clearTX tid = modifyM_ $ \s -> s { txs = Map.delete tid $ txs s }

addTX :: KVTxnId -> TX -> MasterState -> MasterState
addTX tid tx = txs %~ Map.insert tid tx
  -- s { txs = Map.insert tid tx (txs s) }

-- containsTX :: KVTxnId -> MasterState -> Bool
-- containsTX tid s = if isNothing (Map.lookup tid $ txs s) then False else True

lookupTX :: KVTxnId -> MasterState -> Maybe TX
lookupTX tid s = Map.lookup tid $ s ^. txs

-- updateTX, mutates the state, must be called within a mutateM_ block
updateTX :: KVTxnId -> Maybe TX -> MasterState -> MasterState
updateTX _ Nothing = id
updateTX tid (Just tx) = txs %~ Map.insert tid tx
-- updateTX tid tx s = s { txs = Map.insert tid tx (txs s) }

clearResponded :: KVTxnId -> MasterState -> MasterState
clearResponded tid s = updateTX tid ((responded .~ S.empty) <$> lookupTX tid s) s

  -- let tx = lookupTX tid s
  --     tx' = fromJust tx
  -- if isJust tx then updateTX tid (tx' { responded = S.empty }) s else s

sendDecisionToRing :: KVMessage -> MState MasterState IO ()
sendDecisionToRing msg@(KVDecision tid decision req) = get >>= \s -> do
  -- Note: this should not already exist in map TODO?
  -- modifyM_ $ \s -> clearResponded tid s
  shard <- consistentHashing msg
--  mapM_ (\n -> forkM_ $ forwardToSlaveRetry n msg KVProtocol.kV_TIMEOUT_MICRO) shard
  let tx = lookupTX (txn_id msg) s
      tx' = fromJust tx

  if isJust tx
  then mapM_ (\n ->
    if (not . S.member (slvID n) $ tx' ^. responded)
    then forkM_ $ forwardToSlave n msg else return ()) shard
  else return ()

consistentHashing :: KVMessage -> MState MasterState IO [KVSlave]
consistentHashing (KVRequest _ req)    = consistentHashing_ req
consistentHashing (KVDecision _ _ req) = consistentHashing_ req

consistentHashing_ :: KVRequest -> MState MasterState IO [KVSlave]
consistentHashing_ request = get >>= \s -> do
    let ring = (slaveConfig $ cfg s)
        current = getHash request `mod` (L.length ring)
        successor = (current + 1) `mod` (L.length ring)
        shard = [uncurry (\h p -> KVSlave current h p) (ring !! current),
                 uncurry (\h p -> KVSlave successor h p) (ring !! successor)]
    return shard
    where getHash (GetReq _ k)   = fromIntegral $ HASH.hash32 (B.toStrict k)
          getHash (PutReq _ k _) = fromIntegral $ HASH.hash32 (B.toStrict k)
          getHash (DelReq _ k)   = fromIntegral $ HASH.hash32 (B.toStrict k) 


sendMsgToRing :: KVMessage -> MState MasterState IO ()
sendMsgToRing msg = consistentHashing msg >>= mapM_ (\n -> forkM_ $ forwardToSlave n msg)

forwardToSlaveRetry :: KVSlave -> KVMessage -> Int -> MState MasterState IO ()
forwardToSlaveRetry slv msg timeout = get >>= \s -> do
  -- We can stop resending if we've received an ack from the node
  let tx = lookupTX (txn_id msg) s
      tx' = fromJust tx
      forwardNeeded = not . S.member (slvID slv) $ tx' ^. responded
  if isJust tx && forwardNeeded then do
    forwardToSlave slv msg
    -- TODO: adjust delay (works with values as low as 10 as far as I can tell 2/3/2016 -NJT)
    traceShowM $ "sleep for " ++ (show timeout)
    liftIO $ threadDelay $ timeout
    traceShowM "recurse forwardtoslave"
    forwardToSlaveRetry slv msg (timeout * 2)
  else do
    traceShowM "tx doesn't exist"
    return ()

forwardToSlave :: KVSlave -> KVMessage -> MState MasterState IO ()
forwardToSlave slv msg = do
  traceShowM "forwardtoslave called"
  result <- tryConnect slv msg
  case result of
    Just h -> do
      liftIO $ do
        KVProtocol.sendMessage h msg
        hClose h
    Nothing -> do
      traceShowM "unable to connect"      
      liftIO $ return ()

tryConnect :: KVSlave -> KVMessage -> MState MasterState IO (Maybe Handle)
tryConnect slv msg = do
  result <- liftIO $ Catch.try $ connectTo (host slv) (port slv)
  let tid = txn_id msg
  case result of
    Left (e :: SomeException) -> do
      -- the connection time out, meaning that the transaction should be aborted.
      -- mark this in the timeout map so the timeout thread behaves appropriately.
      modifyM_ $ \s -> updateTX tid (lookupTX tid s <&> timeout .~ 0) s
      return Nothing

      --   let tx = lookupTX (txn_id msg) s
      --       tx' = fromJust tx
      --   if isJust tx then updateTX (txn_id msg) (tx' { timeout = 0 } ) s else s
      -- return Nothing

    Right h -> liftIO $ return (Just h)

sendMsgToClient :: KVMessage -> MState MasterState IO ()
sendMsgToClient msg = get >>= \s -> liftIO $ do
  let clientId = fst (txn_id msg)
      clientCfgList = Lib.clientConfig $ cfg s

  if (clientId > Prelude.length clientCfgList - 1) then return ()
  else do
    -- TODO, what if client disconnected? need to add timeout logic here (or just stop if
    -- exception is thrown while trying to connectTo the client.
    let clientCfg = Lib.clientConfig (cfg s) !! clientId
    clientH <- uncurry connectTo clientCfg
    KVProtocol.sendMessage clientH msg
    hClose clientH
