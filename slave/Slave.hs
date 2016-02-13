{-# LANGUAGE OverloadedStrings #-}

module Main where

import Lib
import System.IO as IO
import Network
import Data.Maybe
import Data.List as List

import Data.ByteString.Lazy as B
import Data.ByteString.Char8 as C8

import Control.Exception

import Debug.Trace

import KVProtocol

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
processMessages s cfg =
  bracket (accept s)
          (\(h,_,_) -> do
            hClose h
            processMessages s cfg)
          (\(h, hostName, portNumber) -> do
              req <- getRequest h
              IO.putStr $ (show req) --print the message 


          )
