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
  processMessages s cfg

processMessages :: Socket -> Lib.Config ->  IO()
processMessages s cfg = do
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
        (KVResponse _ _) -> forwardToClient kvMsg cfg
        _ -> undefined

  hClose h
  processMessages s cfg

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