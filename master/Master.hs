{-# LANGUAGE OverloadedStrings #-}

module Main where

import Lib
import System.IO as IO
import Network

import Data.ByteString.Lazy as B
import Data.ByteString.Char8 as C8
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set

import Control.Exception
import Control.Monad.State
import KVProtocol

import Debug.Trace

data MasterState = MasterState {
              --add an mvar in here
              socket :: Socket,
              cfg :: Lib.Config,
              voteMap :: Map.Map Int (Set.Set Int)
            }

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
  _ <- runStateT processMessages (MasterState s cfg Map.empty)
  return ()                     -- todo: change this to be cleaner - should just not return anything?

processMessage :: KVMessage -> StateT MasterState IO ()
processMessage (KVVote _ _ VoteAbort) = undefined
processMessage (KVVote txn_id slave_id VoteReady) = do
  state <- get
  let oldMap = voteMap state
      oldVotes = Map.lookup txn_id oldMap
      config = cfg state
      skt = socket state
  case oldVotes of
    Just set -> do
      let newSet = (Set.insert slave_id set)
      if Set.size newSet == (Prelude.length $ Lib.slaveConfig config)
        then liftIO (sequence $ forwardToRing (KVDecision txn_id DecisionCommit) config)
        else liftIO (sequence $ [return ()])
      put $ MasterState skt config $ Map.insert txn_id newSet oldMap

    Nothing -> put (MasterState skt config (Map.insert txn_id (Set.insert slave_id Set.empty) oldMap))

processMessage kvMsg@(KVResponse _ _ _) = do
  state <- get
  liftIO $ forwardToClient kvMsg (cfg state)
processMessage kvMsg@(KVRequest _ _) = do
  state <- get
  _ <- liftIO $ sequence $ forwardToRing kvMsg (cfg state) -- todo: these should just not return anything
  return ()
processMessage (KVAck _ _) = undefined
processMessage _ = undefined

processMessages :: StateT MasterState IO ()
processMessages = do
  state <- get
  (h, _, _) <- liftIO $ accept $ socket state
  msg <- liftIO $ getMessage h
  either (\errmsg -> liftIO $ IO.putStr errmsg) processMessage msg
  liftIO $ hClose h
  processMessages

forwardToRing :: KVMessage                         --request to be forwarded
              -> Lib.Config                        --ring configuration
              -> [IO()]                          
forwardToRing msg cfg = Prelude.map forwardToNode (Lib.slaveConfig cfg)
  where forwardToNode (name, portId) = do
          slaveH <- connectTo name portId
          sendMessage slaveH msg
          hClose slaveH

forwardToClient :: KVMessage
                -> Lib.Config
                -> IO()
forwardToClient msg cfg = do
  let clientCfg = Prelude.head $ Lib.clientConfig cfg
  clientH <- connectTo (fst clientCfg) (snd clientCfg)
  sendMessage clientH msg
  hClose clientH
