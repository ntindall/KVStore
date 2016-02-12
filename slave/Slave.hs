{-# LANGUAGE OverloadedStrings #-}

module Main where

import Lib
import System.IO as IO
import Network
import Data.Maybe
import Data.List as List

import Data.ByteString.Lazy as B

import Debug.Trace

type SlaveId = Int

main :: IO ()
main = do
  success <- Lib.parseArguments
  case success of
    Nothing     -> Lib.printUsage --an error occured
    Just config -> 
      let slaveId = fromMaybe (-1) (Lib.slaveNumber config)
      in if (slaveId <= (-1) || slaveId >= List.length (Lib.slaveConfig config))
         then Lib.printUsage --error
         else runKVSlave config slaveId

runKVSlave :: Lib.Config -> SlaveId -> IO ()
runKVSlave cfg slaveId = do
    let (slaveName, slavePortId) = (Lib.slaveConfig cfg) !! slaveId
    s <- listenOn slavePortId

    processMessages s cfg

processMessages :: Socket
                -> Lib.Config
                -> IO()
processMessages s cfg = do
  (h, hostName, portNumber) <- accept s

  msg <- B.hGetContents h
  B.putStr msg --print the message 
  processMessages s cfg
