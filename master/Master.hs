{-# LANGUAGE OverloadedStrings #-}

module Main where

import Lib
import System.IO as IO
import Network

import Data.ByteString.Lazy as B
import Data.ByteString.Char8 as C8

import Control.Exception

import KVProtocol

import Debug.Trace

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
  processRequests s cfg

processRequests :: Socket -> Lib.Config ->  IO()
processRequests s cfg = do
  (h, hostName, portNumber) <- accept s
  req <- getMessage h

  case req of 
    Left errmsg -> do
      IO.putStr errmsg
    Right req   -> do 
      sequence $ forwardRequest req cfg
      hClose h

      processRequests s cfg

forwardRequest :: KVMessage                         --request to be forwarded
               -> Lib.Config                        --ring configuration
               -> [IO()]                          
forwardRequest req cfg = Prelude.map forwardToNode (Lib.slaveConfig cfg)
  where forwardToNode (name, portId) = do
          slaveH <- connectTo name portId
          sendMessage slaveH req
          hClose slaveH
