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

  if Set.size newSet == (Prelude.length $ Lib.slaveConfig $ cfg state)
  then liftIO $ sequence $ forwardToRing (KVDecision txn_id DecisionCommit request) (cfg state) 
       --TODO, NEEd to remove the txn_id from the map? after everyone has acked 
  else liftIO $ sequence $ [return ()]

  let state' = MasterState (socket state) (cfg state) (Map.insert txn_id newSet oldMap)

  put state'

processMessage kvMsg@(KVResponse _ _ _) = do
  state <- get
  liftIO $ forwardToClient kvMsg (cfg state)

processMessage kvMsg@(KVRequest _ _) = do
  state <- get
  _ <- liftIO $ sequence $ forwardToRing kvMsg (cfg state) -- todo: these should just not return anything
  return ()

processMessage (KVAck txn_id _) = do
  state <- get
  _ <- liftIO $ forwardToClient (KVAck txn_id Nothing) (cfg state) -- todo: these should just not return anything
  return ()

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
