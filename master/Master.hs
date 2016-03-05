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

import Control.Exception
import Control.Monad.Reader
import Control.Monad.Catch as Catch
import Control.Concurrent
import Control.Concurrent.Chan

import qualified KVProtocol (getMessage, sendMessage, kV_TIMEOUT)
import KVProtocol hiding (getMessage, sendMessage, connectToMaster)

import Debug.Trace

import qualified Utils as U

--https://hackage.haskell.org/package/lrucache-1.2.0.0/docs/Data-Cache-LRU.html
data MasterState = MasterState {
                  --add an mvar in here
                  socket :: Socket
                , channel :: Chan KVMessage
                , cfg :: Lib.Config
                  --the votemap contains as a key the txn id, and has as a value
                  --the set of slaves that have responded with a READY vote
                , voteMap :: Map.Map KVTxnId (Set.Set Int)
                  --the ackMap contains as a key the txn id, and has as a value
                  --the set of slaves that have responded with an ACK
                  --
                  --when the last ACK is received, the master removes the txn_id
                  --from both Maps and sends an acknowledgement to the client.
                , ackMap  :: Map.Map KVTxnId (Set.Set Int)
                  --timeout map. Map from transaction id to when the server first
                  --sent a KVRequest to a shard
                , timeoutMap :: Map.Map KVTxnId (Int, KVMessage)
                }

instance Show (MasterState) where
  show (MasterState skt _ cfg voteMap ackMap timeoutMap) = 
    show skt ++ show cfg ++ show voteMap ++ show ackMap ++ show timeoutMap

-- Entry Point
main :: IO ()
main = Lib.parseArguments >>= runKVMaster

-- Initialization for Master Node
runKVMaster :: Maybe Lib.Config -> IO ()
runKVMaster Nothing = Lib.printUsage -- an error occured 
runKVMaster (Just c) = do
  s <- MasterState <$> listenOn (Lib.masterPortId c) <*> newChan
  mvar <- newMVar (s c Map.empty Map.empty Map.empty)
  _ <- forkIO $ runReaderT listen mvar
  _ <- forkIO $ runReaderT timeoutThread mvar
  runReaderT processMessages mvar

-- Listen and write to thread-safe channel
listen :: ReaderT (MVar MasterState) IO ()
listen = ask >>= \mvar -> do
  liftIO $ do
    s <- readMVar mvar
    let process = either (IO.putStr . show . (++ "\n")) (writeChan (channel s))
    Catch.bracket
      (accept (socket s)) 
      (hClose . Lib.fst')
      ( (>>= process) . KVProtocol.getMessage . Lib.fst')
  listen

timeoutThread :: ReaderT (MVar MasterState) IO ()
timeoutThread = do
  mvar <- ask

--  liftIO $ traceIO "Timing out! in timeout thread!"

  state <- liftIO $ readMVar mvar
  now <- liftIO $ U.currentTimeInt

  let timeoutMap' = Map.toList $ timeoutMap state

  mapM_ (\(txn_id,(ts,msg)) -> do
          if (now - KVProtocol.kV_TIMEOUT >= ts)
          then sendDecisionToRing (KVDecision txn_id DecisionAbort (request msg))
          else liftIO $ return ()
        ) timeoutMap'

  liftIO $ threadDelay 10000000
  timeoutThread

addVote :: KVTxnId -> Int -> ReaderT (MVar MasterState) IO ()
addVote tid sid = ask >>= \mvar -> liftIO $ do
  s <- takeMVar mvar
  traceIO $ "adding vote to" ++ show tid
  let v = fromJust $ Map.lookup tid (voteMap s)
  putMVar mvar $ s { voteMap = Map.insert tid (Set.insert sid v) $ voteMap s }
  traceIO (show s)

votingComplete :: KVTxnId -> ReaderT (MVar MasterState) IO Bool
votingComplete tid = ask >>= \mvar -> liftIO $ do
  s <- readMVar mvar
  traceIO (show s)
  traceIO $ "voting complete"
  let v = fromJust $ Map.lookup tid $ voteMap s
  return $ Set.size v == Prelude.length (Lib.slaveConfig $ cfg s)

timedOut :: KVTxnId -> ReaderT (MVar MasterState) IO Bool
timedOut tid = ask >>= \mvar -> liftIO $ do
  s <- readMVar mvar
  now <- U.currentTimeInt
  let (ts, _) = fromJust $ Map.lookup tid (timeoutMap s)
  return $ now - KVProtocol.kV_TIMEOUT >= ts

addAck :: KVTxnId -> Int -> ReaderT (MVar MasterState) IO ()
addAck tid sid = ask >>= \mvar -> liftIO $ do
  s <- takeMVar mvar
  traceIO $ "adding ack to" ++ show tid ++ " for slave " ++ show sid
  -- use ackMap to keep track of which transactions on the GET pathway have been
  -- forwarded to the client
  let acks = U.insertS sid $ Map.lookup tid $ ackMap s  
  let m = if Set.size acks == (Prelude.length $ slaveConfig $ cfg s)
          then Map.delete tid $ ackMap s   -- erase this txn from map - all the slaves have sent a response to master
          else Map.insert tid acks $ ackMap s
  putMVar mvar $ s { ackMap = m }

-- TODO: change these maps to map to tuples so that we can keep track of whether they are 'complete'
ackComplete :: KVTxnId -> ReaderT (MVar MasterState) IO Bool
ackComplete tid = ask >>= \mvar -> liftIO $ do
 s <- readMVar mvar
 traceIO (show s)
 traceIO $ "voting complete"
 let v = fromJust $ Map.lookup tid $ ackMap s
 return $ Set.size v == Prelude.length (Lib.slaveConfig $ cfg s)

clearTX :: KVTxnId -> ReaderT (MVar MasterState) IO ()
clearTX tid = ask >>= \mvar -> liftIO $ do
  s <- takeMVar mvar
  putMVar mvar $ s { ackMap = Map.delete tid $ ackMap s, 
                     voteMap = Map.delete tid $ voteMap s,
                     timeoutMap = Map.delete tid $ timeoutMap s }

processMessage :: KVMessage -> ReaderT (MVar MasterState) IO ()
processMessage (KVVote _ _ VoteAbort _) = undefined

processMessage (KVVote tid sid VoteReady request) = do
  addVote tid sid
  commit <- votingComplete tid
  timeout <- timedOut tid
  if commit then sendDecisionToRing (KVDecision tid DecisionCommit request) 
  else if timeout then sendDecisionToRing (KVDecision tid DecisionAbort request)
       else return ()

processMessage kvMsg@(KVResponse tid sid _) = addAck tid sid

processMessage kvMsg@(KVRequest txn_id req) = ask >>= \mvar -> do
  liftIO $ do 
    --Communicating with shard for the first time, keep track of this in the
    --timeout map.
    state <- takeMVar mvar
    now <- U.currentTimeInt
    let tm = Map.insert txn_id (now, kvMsg) (timeoutMap state)
        vm = Map.insert txn_id Set.empty (voteMap state)
    putMVar mvar $ state { timeoutMap = tm, voteMap = vm }

  sendMsgToRing kvMsg

processMessage (KVAck tid (Just sid)) = ask >>= \mvar -> do
  addAck tid sid
  complete <- ackComplete tid
  if complete then do
    clearTX tid
    sendMsgToClient $ KVAck tid Nothing
  else return ()

processMessage kvMsg@(KVRegistration txn_id hostName portId) = do
  mvar <- ask
  liftIO $ do
    state <- takeMVar mvar
    let oldConfig = cfg state
        oldClientCfg = Lib.clientConfig oldConfig
        cfgTuple = (hostName, PortNumber $ toEnum portId)
        newClientCfg = oldClientCfg ++ [cfgTuple]
        newConfig = oldConfig { clientConfig = newClientCfg }
        clientId = Prelude.length newClientCfg - 1

    putMVar mvar $ state {cfg = newConfig} 

    clientH <- liftIO $ uncurry connectTo cfgTuple
    liftIO $ KVProtocol.sendMessage clientH $ KVAck (clientId, snd txn_id) $ Just clientId
    liftIO $ hClose clientH

processMessage _ = undefined

processMessages :: ReaderT (MVar MasterState) IO ()
processMessages = ask >>= \mvar -> do
  liftIO $ do
    state <- readMVar mvar
    message <- readChan (channel state)
    forkIO $ runReaderT (processMessage message) mvar
  processMessages

sendDecisionToRing :: KVMessage -> ReaderT (MVar MasterState) IO ()
sendDecisionToRing msg@(KVDecision txn_id decision req) = do
  mvar <- ask
  liftIO $ do
    state <- takeMVar mvar
    putMVar mvar $ state { ackMap = Map.insert txn_id Set.empty (ackMap state) } -- Note: this should not already exist in map
  
  shard <- consistentHashing msg

  liftIO $ mapM_ (\node -> do
                    forkIO $ runReaderT (forwardToNodeWithRetry node msg KVProtocol.kV_TIMEOUT) mvar
                  ) shard 

forwardToNodeWithRetry :: (Int,HostName, PortID)
                            -> KVMessage
                            -> Int
                            -> ReaderT (MVar MasterState) IO()
forwardToNodeWithRetry (myId, hostName, portId) msg timeout = do
  mvar <- ask
  state <- liftIO $ takeMVar mvar
  let acks = Map.lookup (txn_id msg) (ackMap state)

    --if there are no acks in the map, then all of the nodes in the ring have responded
    --if the set of acks contains the current slave id, then we can also stop resending,
    --as this node has responded
  if (acks == Nothing || Set.member myId (fromJust acks)) then do
    liftIO $ putMVar mvar state
  else do
    liftIO $ putMVar mvar state
    
    forwardToNode (myId, hostName, portId) msg
    liftIO $ threadDelay $ timeout * 10000 --TODO... why does MVAR behave incorrectly with small delay??? sounds bad.... race??? why?
                                    --no error when we don't recurse...
    forwardToNodeWithRetry (myId, hostName, portId) msg (timeout * 2)


consistentHashing :: KVMessage -> ReaderT (MVar MasterState) IO ([(Int,HostName, PortID)])
consistentHashing msg = do
  mvar <- ask
  liftIO $ do
    state <- readMVar mvar
    let config = cfg state
        slaves = slaveConfig config
    --TODO: something smarter
    return $ Foldable.toList $ Seq.mapWithIndex (\i (h,p) -> (i,h,p)) (Seq.fromList slaves)

--sendDecisionToRingWithRetry :: KVMessage -> Integer -> ReaderT (MVar MasterState) IO ()
--sendDecisionToRingWithRetry decision timeout = do
--  mvar <- ask
--  state <- liftIO $ readMVar mvar
--  let acks = Map.lookup (txn_id decision) (ackMap state)
--  if acks == Nothing then return ()
--  else sendDecisionToRingWithRetry decision (timeout * 2)


sendMsgToRing :: KVMessage -> ReaderT (MVar MasterState) IO ()
sendMsgToRing msg = do
  mvar <- ask

  shard <- consistentHashing msg

  liftIO $ mapM_ (\node -> do
                    forkIO $ runReaderT (forwardToNode node msg) mvar
                  ) shard 

forwardToNode :: (Int,HostName, PortID)
              -> KVMessage
              -> ReaderT (MVar MasterState) IO()
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
           -> ReaderT (MVar MasterState) IO(Maybe Handle)
tryConnect name portId msg = do
  result <- liftIO $ Catch.try $ connectTo name portId
  case result of
    Left (e :: SomeException) -> do
      mvar <- ask 
      liftIO $ do
        -- the connection time out, meaning that the transaction should be aborted.
        -- mark this in the timeout map so the timeout thread behaves appropriately.
        state <- takeMVar mvar
        let updatedTimeoutMap = Map.insert (txn_id msg) (0,msg) (timeoutMap state)
        putMVar mvar $ state { timeoutMap = updatedTimeoutMap }

        return Nothing

    Right h -> liftIO $ return (Just h)

--sendMsgToRing :: KVMessage                         --request to be forwarded
--              -> Lib.Config                        --ring configuration
--              -> IO [ThreadId]
--sendMsgToRing msg cfg = mapM (\(h,p) -> forkIO $ forwardToNode (h,p) msg) (Lib.slaveConfig cfg)

--forwardToNode :: (HostName, PortID) -> KVMessage -> IO()
--forwardToNode (name, portId) msg = do
--    slaveH <- handleError name portId
--    KVProtocol.sendMessage slaveH msg
--    hClose slaveH
--  where handleError name portId = do
--          result <- Catch.try $ connectTo name portId
--          case result of
--            Left (e :: SomeException) -> do 

--                    threadDelay 500000
--                    mandateConnection name portId
--            Right h -> return h


sendMsgToClient :: KVMessage -> ReaderT (MVar MasterState) IO ()
sendMsgToClient msg = ask >>= \mvar -> liftIO $ do
  s <- readMVar mvar
  let clientId = fst (txn_id msg)
      clientCfgList = Lib.clientConfig $ cfg s

  if (clientId > Prelude.length clientCfgList - 1) then return ()
  else do
    let clientCfg = Lib.clientConfig (cfg s) !! clientId
    clientH <- uncurry connectTo clientCfg
    KVProtocol.sendMessage clientH msg
    hClose clientH
