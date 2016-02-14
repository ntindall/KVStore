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
import Control.Concurrent
import Control.Concurrent.Chan

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

    channel <- newChan 
    forkIO $ sendResponses channel cfg -- fork a child
    processMessages s channel cfg
    return ()

processMessages :: Socket
                -> Chan KVMessage
                -> Lib.Config
                -> IO()
processMessages s channel cfg =
  bracket (accept s)
          (\(h,_,_) -> do
            hClose h
            processMessages s channel cfg)
          (\(h, hostName, portNumber) -> do
            msg <- getMessage h
            either (\err -> IO.putStr $ (show err) ++ ['\n'])
                   (\suc -> do
                      IO.putStr $ (show suc) ++ ['\n'] --print the message 
                      writeChan channel suc
                    )
                   msg
          )

sendResponses :: Chan KVMessage
              -> Lib.Config
              -> IO()
sendResponses channel cfg = do
  --get a message from the channel
  message <- readChan channel
  --todo, logic about handling this message 

  case message of
    (KVResponse _ _) -> handleResponse 
    (KVRequest _ _)  -> handleRequest cfg message
    (KVVote _ _)     -> handleVote --protocol error?
    (KVAck _)        -> handleAck -- protocol error?
    (KVDecision _ _) -> handleDecision 

  sendResponses channel cfg

handleRequest :: Lib.Config -> KVMessage -> IO()
handleRequest cfg msg = do
  h <- connectTo (Lib.masterHostName cfg) (Lib.masterPortId cfg)
  let field_txn  = txn_id msg
      field_request = request msg

  case field_request of
      GetReq key     ->
        sendMessage h (KVResponse field_txn (KVSuccess key "Echo"))
      PutReq key _ -> 
        sendMessage h (KVResponse field_txn (KVSuccess key "Echo"))


  IO.hClose h
  
handleResponse = undefined
handleVote = undefined
handleAck = undefined
handleDecision = undefined