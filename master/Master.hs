{-# LANGUAGE OverloadedStrings #-}

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
import Control.Monad.State
import KVProtocol

import Debug.Trace

data MasterState = MasterState {
                  --add an mvar in here
                  socket :: Socket
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
  deriving (Show)

main :: IO ()
main = do
  success <- Lib.parseArguments
  case success of
    Nothing     -> Lib.printUsage --an error occured
    Just config -> runKVMaster config

runKVMaster :: Lib.Config -> IO ()
runKVMaster cfg = do 
  s <- listenOn (Lib.masterPortId cfg)
  --todo, need to fork client thread (this one, and another one for
  --handling responses from slaves)
  _ <- runStateT processMessages (MasterState s cfg Map.empty Map.empty)
  return ()                     -- todo: change this to be cleaner - should just not return anything?

processMessage :: KVMessage -> StateT MasterState IO ()
processMessage (KVVote _ _ VoteAbort request) = undefined
processMessage (KVVote txn_id slave_id VoteReady request) = do
  state <- get
  let oldMap = voteMap state
      oldVotes = Map.lookup txn_id oldMap
      oldMap' = case oldVotes of
                  Nothing -> Map.insert txn_id Set.empty oldMap
                  (Just _) -> oldMap
      oldVotes' = Map.lookup txn_id oldMap'
      newSet = Set.insert slave_id $ fromJust oldVotes'
      newVoteMap = Map.insert txn_id newSet oldMap'

  if Set.size newSet == (Prelude.length $ Lib.slaveConfig $ cfg state)
  then liftIO $ sequence $ sendMsgToRing (KVDecision txn_id DecisionCommit request) (cfg state) 
  else liftIO $ sequence $ [return ()]

  let state' = MasterState (socket state) (cfg state) newVoteMap (ackMap state)

  put state'

processMessage kvMsg@(KVResponse txn_id slave_id _) = do
  state <- get
  -- use ackMap to keep track of which transactions on the GET pathway have been
  -- forwarded to the client
  let oldAckMap = ackMap state

  case (Map.lookup txn_id oldAckMap) of
    Nothing -> do
      let newAckMap = Map.insert txn_id (Set.singleton slave_id) oldAckMap

      put $ MasterState (socket state) (cfg state) (voteMap state) newAckMap

      liftIO $ sendMsgToClient kvMsg (cfg state)

    Just hasRespondedSet  -> do
      let hasRespondedSet' = Set.insert slave_id hasRespondedSet
          newAckMap' = case (Set.size hasRespondedSet' == (Prelude.length $ Lib.slaveConfig $ cfg state)) of
                        True -> --erase this txn from the ack map, all the slaves have sent a response to master
                          Map.delete txn_id oldAckMap
                        _    -> Map.insert txn_id hasRespondedSet' oldAckMap

      put $ MasterState (socket state) (cfg state) (voteMap state) newAckMap'

processMessage kvMsg@(KVRequest _ _) = do
  state <- get
  liftIO $ sequence $ sendMsgToRing kvMsg (cfg state)
  return ()

processMessage kvMsg@(KVRegistration txn_id hostName portId) = do
  state <- get
  let oldConfig = cfg state
      oldClientCfg = Lib.clientConfig oldConfig
      newClientCfg = oldClientCfg ++ [(hostName, PortNumber $ toEnum portId)]
      newConfig = oldConfig { clientConfig = newClientCfg }
      clientId = (Prelude.length newClientCfg) - 1

  put $ MasterState (socket state) newConfig (voteMap state) (ackMap state)

--  liftIO $ sendMessage h $ KVAck (clientId, snd txn_id) $ Just clientId

processMessage (KVAck txn_id slave_id) = do
  state <- get

  let oldAckMap = ackMap state
      oldAcks = Map.lookup txn_id oldAckMap
      oldAckMap' = case oldAcks of
                  Nothing -> Map.insert txn_id Set.empty oldAckMap
                  (Just _) -> oldAckMap
      oldAcks' = Map.lookup txn_id oldAckMap'
      newSet = Set.insert (fromJust slave_id) $ fromJust oldAcks'
      newAckMap = Map.insert txn_id newSet oldAckMap'

  traceShowM $ show state

  if Set.size newSet == (Prelude.length $ Lib.slaveConfig $ cfg state)
  then do --the ACK we are processing is the last ack from the ring
    let oldVoteMap = voteMap state
        newVoteMap = Map.delete txn_id oldVoteMap
        newAckMap  = Map.delete txn_id oldAckMap'
        state' = MasterState (socket state) (cfg state) newVoteMap newAckMap

    put state'
    liftIO $ sendMsgToClient (KVAck txn_id Nothing) (cfg state) -- todo: these should just not return anything


  else do
    put $ MasterState (socket state) (cfg state) (voteMap state) newAckMap
   -- return () --we need to wait for more acks


processMessage _ = undefined

processMessages :: StateT MasterState IO ()
processMessages = do
  state <- get
  (h, _, _) <- liftIO $ accept $ socket state
  --todo forkIO
  msg <- liftIO $ getMessage h
  either (\errmsg -> liftIO $ IO.putStr errmsg) processMessage msg
  liftIO $ hClose h
  processMessages

sendMsgToRing :: KVMessage                         --request to be forwarded
              -> Lib.Config                        --ring configuration
              -> [IO()]                          
sendMsgToRing msg cfg = Prelude.map forwardToNode (Lib.slaveConfig cfg)
  where forwardToNode (name, portId) = do
          slaveH <- connectTo name portId
          sendMessage slaveH msg
          hClose slaveH

sendMsgToClient :: KVMessage
                -> Lib.Config
                -> IO()
sendMsgToClient msg cfg = do
  let clientId = fst (txn_id msg)
      clientCfg = Lib.clientConfig cfg !! clientId
  clientH <- connectTo (fst clientCfg) (snd clientCfg)
  sendMessage clientH msg
  hClose clientH
