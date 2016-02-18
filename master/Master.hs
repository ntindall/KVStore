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

import KVProtocol

import Debug.Trace

data MasterState = MasterState {
              --add an mvar in here
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
  let state = MasterState Map.empty

  processMessages s cfg state



processMessages :: Socket -> Lib.Config -> MasterState -> IO()
processMessages s cfg state = do
  (h, hostName, portNumber) <- accept s
  msg <- getMessage h

  case msg of 
    Left errmsg -> do
      IO.putStr errmsg
    Right kvMsg -> 
      case kvMsg of 
        (KVRequest _ _) -> do
          sequence $ forwardToRing kvMsg cfg
          return ()
        (KVResponse _ _ _) -> forwardToClient kvMsg cfg
        (KVAck _ _)      -> undefined
        (KVVote txn_id slave_id vote) ->
          case vote of
            VoteAbort -> undefined --todo, send an abort
            VoteReady -> do
              let oldMap = voteMap state
                  oldVotes = Map.lookup txn_id oldMap

              case oldVotes of
                Just set -> do
                  let newSet = (Set.insert slave_id set)

                  if Set.size newSet == (Prelude.length $ Lib.slaveConfig cfg)
                  then sequence $ forwardToRing (KVDecision txn_id DecisionCommit) cfg
                  else sequence $ [return ()]

                  let state' = MasterState $ Map.insert txn_id newSet oldMap

                  hClose h
                  processMessages s cfg state'
                Nothing -> do
                  let state' = MasterState $ Map.insert txn_id (Set.insert slave_id Set.empty) oldMap
                  hClose h
                  processMessages s cfg state'

        _ -> undefined

  hClose h
  processMessages s cfg state


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