{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Main where

import Lib
import System.IO as IO
import Network

import Data.ByteString.Lazy as B
import Data.ByteString.Char8 as C8
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
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

type KVMap a = Map.Map KVTxnId (Set.Set a)

--https://hackage.haskell.org/package/lrucache-1.2.0.0/docs/Data-Cache-LRU.html
data MasterState = MasterState {
                  --add an mvar in here
                  socket :: Socket
                , channel :: Chan KVMessage
                , cfg :: Lib.Config
                  --the votemap contains as a key the txn id, and has as a value
                  --the set of slaves that have responded with a READY vote
                -- , voteMap :: Map.Map KVTxnId (Set.Set Int)
                , voteMap :: KVMap Int
                  --the ackMap contains as a key the txn id, and has as a value
                  --the set of slaves that have responded with an ACK
                  --
                  --when the last ACK is received, the master removes the txn_id
                  --from both Maps and sends an acknowledgement to the client.
                , ackMap  :: KVMap Int
                  --timeout map. Map from transaction id to when the server first
                  --sent a KVRequest to a shard
                , timeoutMap :: Map.Map KVTxnId (Int, KVMessage)
                }

type KVSGetter a = MasterState -> a
type KVSSetter a = a -> MasterState -> MasterState

setVoteMap, setAckMap :: KVSSetter (KVMap Int)
setVoteMap v s = s { voteMap = v }
setAckMap a s = s { ackMap = a }

instance Show (MasterState) where
  show (MasterState skt _ cfg voteMap ackMap timeoutMap) = 
    show skt ++ show cfg ++ show voteMap ++ show ackMap ++ show timeoutMap

-- Entry Point
main :: IO ()
main = Lib.parseArguments >>= \args -> case args of
  Nothing -> Lib.printUsage -- an error occured 
  Just c -> do
    ms <- MasterState <$> listenOn (Lib.masterPortId c) <*> newChan
    execMState initMaster $ ms c Map.empty Map.empty Map.empty
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
      (hClose . Lib.fst')
      ((>>= process) . KVProtocol.getMessage . Lib.fst')
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
  addToState tid sid voteMap setVoteMap
  commit <- isComplete tid voteMap
  timeout <- timedOut tid voteMap

  if commit then sendDecisionToRing (KVDecision tid DecisionCommit request) 
  else when timeout $ sendDecisionToRing (KVDecision tid DecisionAbort request)

processMessage kvMsg@(KVResponse tid sid _) = do
  addToState tid sid ackMap setAckMap
  complete <- isComplete tid ackMap
  when complete $ do
    sendMsgToClient kvMsg
    clearTX tid

  -- TODO: add tiemout for get reqs
  -- addAck tid sid
  -- complete <- ackComplete tid
  -- if complete then clearAcksTX tid else return ()

processMessage kvMsg@(KVRequest txn_id req) = do
  now <- liftIO U.currentTimeInt
  --Communicating with shard for the first time, keep track of this in the timeout map.
  modifyM_ $ \s -> s { timeoutMap = Map.insert txn_id (now, kvMsg) (timeoutMap s) }
  sendMsgToRing kvMsg

processMessage (KVAck tid (Just sid) maybeSuccess) = do
  addToState tid sid ackMap $ \a s -> s { ackMap = a }
  complete <- isComplete tid ackMap
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
--  liftIO $ traceIO "Timing out! in timeout thread!"

  now <- liftIO $ U.currentTimeInt
  let tm = Map.toList $ timeoutMap s

  mapM_ (\(txn_id,(ts,msg)) -> do
          if (now - KVProtocol.kV_TIMEOUT >= ts)
          then sendDecisionToRing (KVDecision txn_id DecisionAbort (request msg))
          else liftIO $ return ()
        ) tm

  liftIO $ threadDelay 10000000
  timeoutThread

addToState :: Ord a => KVTxnId -> a -> KVSGetter (KVMap a) -> KVSSetter (KVMap a)
           -> MState MasterState IO ()
addToState tid val f update = modifyM_ $ \s -> do
  let set = U.insertS val $ Map.lookup tid $ f s
  update (Map.insert tid set $ f s) s

-- todo: change bakc to Ord a => KVMap a later
isComplete :: Ord a => KVTxnId -> KVSGetter (KVMap a)
           -> MState MasterState IO Bool
isComplete tid f = get >>= \s -> do
  let set = fromJust $ Map.lookup tid $ f s
  return $ Set.size set == Prelude.length (Lib.slaveConfig $ cfg s)

timedOut :: KVTxnId -> KVSGetter (KVMap a) -> MState MasterState IO Bool
timedOut tid f = get >>= \s -> liftIO $ do
  now <- U.currentTimeInt
  let (ts, _) = fromJust $ Map.lookup tid (timeoutMap s)
  return $ now - KVProtocol.kV_TIMEOUT >= ts

clearTX :: KVTxnId -> MState MasterState IO ()
clearTX tid = get >>= \s -> do
  put $ s { ackMap = Map.delete tid $ ackMap s, 
            voteMap = Map.delete tid $ voteMap s,
            timeoutMap = Map.delete tid $ timeoutMap s }
  liftIO $ traceIO $ "cleared txid" ++ show tid

sendDecisionToRing :: KVMessage -> MState MasterState IO ()
sendDecisionToRing msg@(KVDecision txn_id decision req) = do
  -- Note: this should not already exist in map
  modifyM_ $ \s -> s { ackMap = Map.insert txn_id Set.empty (ackMap s) }
  shard <- consistentHashing msg
  mapM_ (\node -> forkM_ $ forwardToNodeWithRetry node msg KVProtocol.kV_TIMEOUT) shard 

forwardToNodeWithRetry :: (Int,HostName, PortID) -> KVMessage -> Int
                          -> MState MasterState IO ()
forwardToNodeWithRetry (myId, hostName, portId) msg timeout = do
  s <- get
  let acks = Map.lookup (txn_id msg) (ackMap s)

    --if there are no acks in the map, then all of the nodes in the ring have responded
    --if the set of acks contains the current slave id, then we can also stop resending,
    --as this node has responded

  -- if (acks == Nothing || Set.member myId (fromJust acks)) then do
  --   liftIO $ putMVar mvar state
  -- else do
  --   liftIO $ putMVar mvar state

  if (acks == Nothing || Set.member myId (fromJust acks)) then return ()
  else do
    forwardToNode (myId, hostName, portId) msg
    liftIO $ threadDelay $ timeout * 10000 -- TODO: adjust delay (works with values as low as 10 as far as I can tell 2/3/2016 -NJT)
    forwardToNodeWithRetry (myId, hostName, portId) msg (timeout * 2)

consistentHashing :: KVMessage -> MState MasterState IO ([(Int,HostName, PortID)])
consistentHashing msg = get >>= \s -> liftIO $ do
  let config = cfg s
      slaves = slaveConfig config
  --TODO: something smarter
  return $ Foldable.toList $ Seq.mapWithIndex (\i (h,p) -> (i,h,p)) (Seq.fromList slaves)

sendMsgToRing :: KVMessage -> MState MasterState IO ()
sendMsgToRing msg = do
  shard <- consistentHashing msg
  mapM_ (\node -> forkM_ $ forwardToNode node msg) shard 

forwardToNode :: (Int,HostName, PortID)
              -> KVMessage
               -> MState MasterState IO ()
forwardToNode (myId, hostName, portId) msg = do
  result <- tryConnect hostName portId msg
  case result of
    Just h -> do
      liftIO $ do
        KVProtocol.sendMessage h msg
        hClose h
    Nothing -> liftIO $ return ()

tryConnect :: HostName
           -> PortID
           -> KVMessage
           -> MState MasterState IO (Maybe Handle)
tryConnect name portId msg = do
  result <- liftIO $ Catch.try $ connectTo name portId
  case result of
    Left (e :: SomeException) -> do
      -- the connection time out, meaning that the transaction should be aborted.
      -- mark this in the timeout map so the timeout thread behaves appropriately.
      modifyM_ $ \s -> s { timeoutMap = Map.insert (txn_id msg) (0,msg) (timeoutMap s) }
      return Nothing

    Right h -> liftIO $ return (Just h)

sendMsgToClient :: KVMessage -> MState MasterState IO ()
sendMsgToClient msg = get >>= \s -> liftIO $ do
  let clientId = fst (txn_id msg)
      clientCfgList = Lib.clientConfig $ cfg s

  if (clientId > Prelude.length clientCfgList - 1) then return ()
  else do
    let clientCfg = Lib.clientConfig (cfg s) !! clientId
    clientH <- uncurry connectTo clientCfg
    KVProtocol.sendMessage clientH msg
    hClose clientH
