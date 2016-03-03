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

import qualified Utils

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
                , timeoutMap :: Map.Map KVTxnId Int
                }

instance Show (MasterState) where
  show (MasterState skt _ cfg voteMap ackMap timeoutMap) = 
    show skt ++ show cfg ++ show voteMap ++ show ackMap ++ show timeoutMap



main :: IO ()
main = do
  success <- Lib.parseArguments
  case success of
    Nothing     -> Lib.printUsage --an error occured
    Just config -> runKVMaster config

runKVMaster :: Lib.Config -> IO ()
runKVMaster cfg = do 
  s <- listenOn (Lib.masterPortId cfg)
  c <- newChan
  mvar <- newMVar (MasterState s c cfg Map.empty Map.empty Map.empty)
  forkIO $ runReaderT processMessages mvar
  runReaderT sendResponses mvar

sendResponse :: KVMessage -> ReaderT (MVar MasterState) IO ()
sendResponse (KVVote _ _ VoteAbort request) = undefined
sendResponse (KVVote txn_id slave_id VoteReady request) = do
  mvar <- ask
  state <- liftIO $ takeMVar mvar
  let oldVoteMap = voteMap state
      oldVotes = Map.lookup txn_id oldVoteMap
      oldVoteMap' = case oldVotes of
                  Nothing -> Map.insert txn_id Set.empty oldVoteMap
                  (Just _) -> oldVoteMap
      oldVotes' = Map.lookup txn_id oldVoteMap'
      newSet = Set.insert slave_id $ fromJust oldVotes'
      newVoteMap = Map.insert txn_id newSet oldVoteMap'
      timeoutMap' = timeoutMap state

  liftIO $ do
    let state' = state {voteMap = newVoteMap } 
    putMVar mvar state'

  now <- liftIO $ Utils.currentTimeInt

  liftIO $ traceIO (show state)

  if Set.size newSet == Prelude.length (Lib.slaveConfig $ cfg state)
  then sendDecisionToRing (KVDecision txn_id DecisionCommit request) 
  else 
    case Map.lookup txn_id timeoutMap' of
      Nothing -> liftIO $ return ()
      Just ts -> 
        if now - KVProtocol.kV_TIMEOUT >= ts
        then sendDecisionToRing (KVDecision txn_id DecisionAbort request)
        else
          liftIO $ return () --TODO:: A NICER WAY TO SAY THIS

sendResponse kvMsg@(KVResponse txn_id slave_id _) = do
  mvar <- ask
  liftIO $ do
    state <- takeMVar mvar
    -- use ackMap to keep track of which transactions on the GET pathway have been
    -- forwarded to the client
    let oldAckMap = ackMap state

    case Map.lookup txn_id oldAckMap of
      Nothing -> do
        let newAckMap = Map.insert txn_id (Set.singleton slave_id) oldAckMap

        putMVar mvar $ state {ackMap = newAckMap} --MasterState (socket state) (cfg state) (voteMap state) newAckMap

        sendMsgToClient kvMsg (cfg state)

      Just hasRespondedSet  -> do
        let hasRespondedSet' = Set.insert slave_id hasRespondedSet
            newAckMap' = if Set.size hasRespondedSet' == (Prelude.length $ slaveConfig $ cfg state)
                         --erase this txn from the ack map, all the slaves have
                         --sent a response to master
                         then Map.delete txn_id oldAckMap
                         else Map.insert txn_id hasRespondedSet' oldAckMap

        putMVar mvar $ state { ackMap = newAckMap'} --MasterState (socket state) (cfg state) (voteMap state) newAckMap'

sendResponse kvMsg@(KVRequest txn_id _) = do
  mvar <- ask
  liftIO $ do 

    --Communicating with shard for the first time, keep track of this in the
    --timeout map
    state <- takeMVar mvar
    now <- Utils.currentTimeInt
    let updatedTimeoutMap = Map.insert txn_id now (timeoutMap state)
    putMVar mvar $ state { timeoutMap = updatedTimeoutMap }

  sendMsgToRing kvMsg
  return ()

sendResponse kvMsg@(KVRegistration txn_id hostName portId) = do
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

sendResponse (KVAck txn_id (Just slave_id)) = do
  mvar <- ask
  liftIO $ do
    state <- takeMVar mvar

    let acks = ackMap state --should already exist in map TODO... bug?
               -- if Map.member txn_id (ackMap state) then ackMap state else Map.insert txn_id Set.empty (ackMap state)
        slvIds = fromJust $ Map.lookup txn_id acks -- ack map was added on decision send

    if not (Set.member slave_id slvIds) && Set.size slvIds == Prelude.length (Lib.slaveConfig $ cfg state) - 1
    then do --the ACK we are processing is the last ack from the ring
      putMVar mvar $ state { ackMap = Map.delete txn_id acks, 
                             voteMap = Map.delete txn_id $ voteMap state,
                             timeoutMap = Map.delete txn_id $ timeoutMap state
                           }
      liftIO $ sendMsgToClient (KVAck txn_id Nothing) (cfg state) -- todo: these should just not return anything

    else putMVar mvar $ state { ackMap = Map.insert txn_id (Set.insert slave_id slvIds) acks }


sendResponse _ = undefined

sendResponses :: ReaderT (MVar MasterState) IO ()
sendResponses = do
  mvar <- ask
  liftIO $ do
    state <- readMVar mvar
    message <- readChan (channel state)

    forkIO $ runReaderT (sendResponse message) mvar

  sendResponses

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
  liftIO $ traceIO $ "hello"
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

processMessages :: ReaderT (MVar MasterState) IO ()
processMessages = do
  mvar <- ask
  state <- liftIO $ readMVar mvar
  let s = socket state
      c = channel state

  Catch.bracket (liftIO $ accept s)
                (\(h,_,_) -> do
                     liftIO $ hClose h
                     processMessages)
                   (\(h, _, _) -> liftIO $ do
                     msg <- KVProtocol.getMessage h
                     either (\err -> IO.putStr $ show err ++ "\n")
                            (\suc -> writeChan c suc)
                            msg
                   )    


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
        let updatedTimeoutMap = Map.insert (txn_id msg) 0 (timeoutMap state)
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


sendMsgToClient :: KVMessage
                -> Lib.Config
                -> IO()
sendMsgToClient msg cfg = do
  let clientId = fst (txn_id msg)
      clientCfgList = Lib.clientConfig cfg

  if (clientId > Prelude.length clientCfgList - 1) then return ()
  else do
    let clientCfg = Lib.clientConfig cfg !! clientId
    clientH <- uncurry connectTo clientCfg
    KVProtocol.sendMessage clientH msg
    hClose clientH
