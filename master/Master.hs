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

import Data.Maybe

import Control.Exception
import Control.Monad.Reader
import Control.Monad.Catch as Catch
import Control.Concurrent
import Control.Concurrent.Chan

import qualified KVProtocol (getMessage, sendMessage)
import KVProtocol hiding (getMessage, sendMessage, connectToMaster)

import Debug.Trace


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
                }

instance Show (MasterState) where
  show (MasterState skt _ cfg voteMap ackMap) = show skt ++ show cfg ++ show voteMap ++ show ackMap



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
  mvar <- newMVar (MasterState s c cfg Map.empty Map.empty)
  forkIO $ runReaderT processMessages mvar
  runReaderT sendResponses mvar

sendResponse :: KVMessage -> ReaderT (MVar MasterState) IO ()
sendResponse (KVVote _ _ VoteAbort request) = undefined
sendResponse (KVVote txn_id slave_id VoteReady request) = do
  mvar <- ask
  liftIO $ do
    state <- takeMVar mvar
    let oldMap = voteMap state
        oldVotes = Map.lookup txn_id oldMap
        oldMap' = case oldVotes of
                    Nothing -> Map.insert txn_id Set.empty oldMap
                    (Just _) -> oldMap
        oldVotes' = Map.lookup txn_id oldMap'
        newSet = Set.insert slave_id $ fromJust oldVotes'
        newVoteMap = Map.insert txn_id newSet oldMap'

    if Set.size newSet == Prelude.length (Lib.slaveConfig $ cfg state)
    then liftIO $ sequence $ sendMsgToRing (KVDecision txn_id DecisionCommit request) (cfg state)
    else liftIO $ sequence [return ()]

    let state' = state {voteMap = newVoteMap } 
    putMVar mvar state'

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

sendResponse kvMsg@(KVRequest _ _) = do
  mvar <- ask
  liftIO $ do 
    state <- readMVar mvar
    liftIO $ sequence $ sendMsgToRing kvMsg (cfg state)
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

    let acks = if Map.member txn_id (ackMap state) then ackMap state else Map.insert txn_id Set.empty (ackMap state)
        slvIds = fromJust $ Map.lookup txn_id acks

    if not (Set.member slave_id slvIds) && Set.size slvIds == Prelude.length (Lib.slaveConfig $ cfg state) - 1
    then do --the ACK we are processing is the last ack from the ring
      putMVar mvar $ state { ackMap = Map.delete txn_id acks, voteMap = Map.delete txn_id $ voteMap state}
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


sendMsgToRing :: KVMessage                         --request to be forwarded
              -> Lib.Config                        --ring configuration
              -> [IO()]
sendMsgToRing msg cfg = Prelude.map forwardToNode (Lib.slaveConfig cfg)
  where forwardToNode (name, portId) = do
          slaveH <- mandateConnection name portId
          KVProtocol.sendMessage slaveH msg
          hClose slaveH
        mandateConnection name portId = do
          result <- Catch.try $ connectTo name portId
          case result of
            Left (e :: SomeException) -> do 
                    threadDelay 500000
                    mandateConnection name portId
            Right h -> return h


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
