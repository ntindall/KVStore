{-# LANGUAGE OverloadedStrings #-}

module Main where

import Lib
import System.IO as IO
import Network

import Data.Binary as Binary
import Data.ByteString.Lazy as B

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
  (h, hostName, portNumber) <- accept s

  processRequests h cfg
  hClose h

processRequests :: Handle -> Lib.Config ->  IO()
processRequests h cfg =
  msg <- B.hGetContents h

  traceIO $show msg

  let request = (Binary.decode msg) :: KVRequest --a KVRequest

  B.putStr (key request) --(Binary.decode msg) --print the message
  -- todo... threading???

  sequence $ forwardRequest request cfg

  processRequests h cfg

forwardRequest :: KVRequest                         --request to be forwarded
               -> Lib.Config                        --ring configuration
               -> [IO()]                          
forwardRequest req cfg = Prelude.map forwardToNode (Lib.slaveConfig cfg)
  where forwardToNode (name, portId) = do
          slaveH <- connectTo name portId
          B.hPut slaveH (Binary.encode req)
          hClose slaveH