{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Main where

import Lib
import System.IO as IO
import Network

import Data.ByteString.Lazy as B
import Data.ByteString.Lazy.Char8 as C8
import Data.Tuple.Utils

import qualified Data.List as L
import qualified Data.Map.Strict as Map
import qualified Data.Set as S
import qualified Data.Sequence as Seq
import qualified Data.Foldable as Foldable

import Data.Maybe

import Control.Concurrent.MState
import Control.Monad.State

import Control.Exception
import Control.Monad.Reader
import Control.Monad.Catch as Catch
import Control.Concurrent
import Control.Concurrent.Chan

import qualified KVProtocol (getMessage, sendMessage, kV_TIMEOUT)
import KVProtocol hiding (getMessage, sendMessage, connectToMaster)

import Debug.Trace

import qualified Utils as U

type Time = Int
type SlvId = Int

-- type KVMap a = Map.Map KVTxnId (Set.Set a)

--https://hackage.haskell.org/package/lrucache-1.2.0.0/docs/Data-Cache-LRU.html
data MasterState = MasterState {
                  socket :: Socket
                , channel :: Chan KVMessage
                , cfg :: Lib.Config
                  --the votemap contains as a key the txn id, and has as a value
                  --the set of slaves that have responded with a READY vote
                , txs :: Map.Map KVTxnId TX
                -- , voteMap :: Map.Map KVTxnId (Set.Set Int)
                -- , voteMap :: KVMap Int
                --   --the ackMap contains as a key the txn id, and has as a value
                --   --the set of slaves that have responded with an ACK
                --   --
                --   --when the last ACK is received, the master removes the txn_id
                --   --from both Maps and sends an acknowledgement to the client.
                -- , ackMap  :: KVMap Int
                --   --timeout map. Map from transaction id to when the server first
                --   --sent a KVRequest to a shard
                -- , timeoutMap :: Map.Map KVTxnId (Int, KVMessage)
                }
                   
data TXState = VOTE | ACK | RESPONSE
  deriving (Show, Eq, Ord)

data TX = TX {
  txState :: TXState,
  responded :: S.Set SlvId,
  timeout :: Time,
  message :: KVMessage
}
 deriving (Show)

data KVSlave = KVSlave { slvID :: Int , host :: HostName,  port :: PortID }

instance Show (MasterState) where
  show (MasterState skt _ cfg txs) = 
    show skt ++ show cfg ++ show txs

-- Entry Point
main :: IO ()
main = Lib.parseArguments >>= \args -> case args of
  Nothing -> Lib.printUsage -- an error occured 
  Just c -> do
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
  exists <- get >>= \s -> return $ containsTX tid s

  when exists $ do
    slaveResponded tid sid
    commit <- isComplete tid
    timeout <- timedOut tid

    when (commit || timeout) $ modifyM_ $ \s -> do
      let tx = lookupTX tid s    
      updateTX tid (tx { responded = S.empty, txState = ACK }) s

    if commit then sendDecisionToRing (KVDecision tid DecisionCommit request) 
    else when timeout $ sendDecisionToRing (KVDecision tid DecisionAbort request)

processMessage kvMsg@(KVResponse tid sid _) = do
  exists <- get >>= \s -> return $ containsTX tid s

  when exists $ do
    slaveResponded tid sid
    --complete <- isComplete tid
    --when complete $ do

    --forward first response to client, delete from map
    sendMsgToClient kvMsg
    clearTX tid

  -- TODO: add timeout for get reqs
  -- addAck tid sid
  -- complete <- ackComplete tid
  -- if complete then clearAcksTX tid else return ()

processMessage kvMsg@(KVRequest tid req) = do
  now <- liftIO U.currentTimeInt
  let txstate = case req of
        GetReq{} -> RESPONSE
        PutReq{} -> VOTE
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

addTX :: KVTxnId -> TX -> MasterState -> MasterState
addTX tid tx s = s { txs = Map.insert tid tx (txs s) }

containsTX :: KVTxnId -> MasterState -> Bool
containsTX tid s = if isNothing (Map.lookup tid $ txs s) then False else True

lookupTX :: KVTxnId -> MasterState -> TX
lookupTX tid s = fromJust (Map.lookup tid $ txs s)

-- updateTX, mutates the state, must be called within a mutateM_ block
updateTX :: KVTxnId -> TX -> MasterState -> MasterState
updateTX tid tx s = s { txs = Map.insert tid tx (txs s) }

clearResponded :: KVTxnId -> MasterState -> MasterState
clearResponded tid s = do
  let tx = lookupTX tid s
  updateTX tid (tx { responded = S.empty }) s

timeoutThread :: MState MasterState IO ()
timeoutThread = get >>= \s -> do
--  liftIO $ traceIO "Timing out! in timeout thread!"
  -- TODO: timeout transactions individually
   -- mapM_ (\(txn_id,(ts,msg)) -> do
   --         if (now - KVProtocol.kV_TIMEOUT >= ts)
   --         then sendDecisionToRing (KVDecision txn_id DecisionAbort (request msg))
   --         else liftIO $ return ()
   --       ) Map.toList $ txs s 
  now <- liftIO $ U.currentTimeInt
  timedOutTxns <- filterM (\(tid, tx) -> 
                            if (now - KVProtocol.kV_TIMEOUT >= timeout tx) 
                            then do
                              if (txState tx == VOTE)
                              then sendDecisionToRing (KVDecision tid DecisionAbort (request $ message tx))
                              else
                                sendMsgToClient (KVResponse tid (-1) (KVFailure (C8.pack "Timeout")))
                              return True
                            else 
                              return False
                          ) $ Map.toList $ txs s
  --todo, may need to put this into modifyM_ 
  mapM clearTX (L.map fst timedOutTxns)

  liftIO $ threadDelay 10000000
  timeoutThread

slaveResponded :: KVTxnId -> SlvId -> MState MasterState IO ()
slaveResponded tid slvId = modifyM_ $ \s -> do
  let tx = lookupTX tid s
  updateTX tid (tx { responded = S.insert slvId (responded tx) }) s

-- todo: change bakc to Ord a => KVMap a later
isComplete :: KVTxnId -> MState MasterState IO Bool
isComplete tid = get >>= \s -> do
  let tx = lookupTX tid s
  return $ S.size (responded tx) == Prelude.length (Lib.slaveConfig $ cfg s)

timedOut :: KVTxnId -> MState MasterState IO Bool
timedOut tid = get >>= \s -> liftIO $ do
  let tx = lookupTX tid s
  now <- U.currentTimeInt
  return $ now - KVProtocol.kV_TIMEOUT >= timeout tx

clearTX :: KVTxnId -> MState MasterState IO ()
clearTX tid = modifyM_ $ \s -> s { txs = Map.delete tid $ txs s }

sendDecisionToRing :: KVMessage -> MState MasterState IO ()
sendDecisionToRing msg@(KVDecision tid decision req) = do
  -- Note: this should not already exist in map TODO?
  -- modifyM_ $ \s -> clearResponded tid s
  shard <- consistentHashing msg
  mapM_ (\n -> forkM_ $ forwardToSlaveRetry n msg KVProtocol.kV_TIMEOUT) shard

--TODO: something smarter
consistentHashing :: KVMessage -> MState MasterState IO [KVSlave]
consistentHashing msg = get >>= \s -> do
  let shard = L.mapAccumL (\i (h, p) -> (i+1, KVSlave i h p)) 0 (slaveConfig $ cfg s)
  return $ snd shard

sendMsgToRing :: KVMessage -> MState MasterState IO ()
sendMsgToRing msg = consistentHashing msg >>= mapM_ (\n -> forkM_ $ forwardToSlave n msg)

forwardToSlaveRetry :: KVSlave -> KVMessage -> Int -> MState MasterState IO ()
forwardToSlaveRetry slv msg timeout = get >>= \s -> do
  -- We can stop resending if we've received an ack from the node

  let exists = containsTX (txn_id msg) s
  when exists $ do 
    let forwardNeeded = not . S.member (slvID slv) $ responded $ lookupTX (txn_id msg) s
    when forwardNeeded $ do
      forwardToSlave slv msg
      -- TODO: adjust delay (works with values as low as 10 as far as I can tell 2/3/2016 -NJT)
      liftIO $ threadDelay $ timeout * 10000
      forwardToSlaveRetry slv msg (timeout * 2)

forwardToSlave :: KVSlave -> KVMessage -> MState MasterState IO ()
forwardToSlave slv msg = do
  result <- tryConnect slv msg
  case result of
    Just h -> do
      liftIO $ do
        KVProtocol.sendMessage h msg
        hClose h
    Nothing -> liftIO $ return ()

tryConnect :: KVSlave -> KVMessage -> MState MasterState IO (Maybe Handle)
tryConnect slv msg = do
  result <- liftIO $ Catch.try $ connectTo (host slv) (port slv)
  case result of
    Left (e :: SomeException) -> do
      -- the connection time out, meaning that the transaction should be aborted.
      -- mark this in the timeout map so the timeout thread behaves appropriately.
      modifyM_ $ \s -> do
        let tx = lookupTX (txn_id msg) s
        updateTX (txn_id msg) (tx { timeout = 0 } ) s
      return Nothing

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
