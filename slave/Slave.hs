{-# LANGUAGE OverloadedStrings #-}

module Main where

import Lib
import System.IO as IO
import System.Directory as DIR
import Network
import Data.Maybe
import Data.List as List
import qualified Data.Map.Strict as Map

import Data.ByteString.Lazy as B
import Data.ByteString.Lazy.Char8 as C8

import Control.Exception
import Control.Concurrent
import Control.Concurrent.Chan
import Control.Monad.Reader
import Control.Monad.Catch as Catch
import Control.Arrow as CA

import Control.Monad

import Debug.Trace

import qualified KVProtocol (getMessage, sendMessage, connectToMaster)
import KVProtocol hiding (getMessage, sendMessage, connectToMaster)

import Log as LOG
import qualified Utils

--- todo, touch file when slave registers
-- todo, slave registration

type SlaveId = Int

data SlaveState = SlaveState {
                  socket :: Socket
                , channel :: Chan KVMessage
                , cfg :: Lib.Config
                }
 --  deriving (Show)

main :: IO ()
main = do
  success <- Lib.parseArguments
  case success of
    Nothing     -> Lib.printUsage --an error occured
    Just config -> 
      let slaveId = fromJust (Lib.slaveNumber config)
      in if (slaveId <= (-1) || slaveId >= List.length (Lib.slaveConfig config))
         then Lib.printUsage --error
         else do
           mvar <- newMVar SlaveState {cfg = config}
           runReaderT runKVSlave mvar

runKVSlave :: ReaderT (MVar SlaveState) IO ()
runKVSlave = do
  mvar <- ask
  c <- liftIO $ newChan

  liftIO $ do
    state <- readMVar mvar
    let config = cfg state
        slaveId = fromJust $ Lib.slaveNumber config
        (slaveName, slavePortId) = (Lib.slaveConfig config) !! slaveId
    skt <- listenOn slavePortId

    fileExists <- DIR.doesFileExist (LOG.persistentLogName slaveId)
    if fileExists
    then do
      unsentAcks <- LOG.handleUnfinishedTxn (LOG.persistentLogName slaveId) (persistentFileName slaveId)

      sequence_ $ List.map (\txn_id -> do
                              h <- KVProtocol.connectToMaster (cfg state)
                              KVProtocol.sendMessage h (KVAck txn_id (Just slaveId))
                              IO.hClose h
                            )
                            unsentAcks
    else do
      _ <- IO.openFile (LOG.persistentLogName slaveId) IO.AppendMode
      return ()

    state' <- takeMVar mvar
    putMVar mvar $ state' { socket = skt, channel = c }
    forkIO $ runReaderT sendResponses mvar 
  processMessages

processMessages :: ReaderT (MVar SlaveState) IO ()
processMessages = do
  mvar <- ask
  state <- liftIO $ readMVar mvar
  let s = socket state
      c = channel state

  Catch.bracket (liftIO $ accept s)
                (\(h,_,_) -> do
                     liftIO $ hClose h
                     processMessages)
                   (\(h, hostName, portNumber) -> liftIO $ do
                     msg <- KVProtocol.getMessage h
                     either (\err -> IO.putStr $ (show err) ++ ['\n'])
                            (\suc -> do
                              IO.putStr $ (show suc) ++ ['\n'] --print the message 
                              writeChan c suc
                            )
                            msg
                   )


persistentFileName :: Int -> String                                                   --todo, hacky
persistentFileName slaveId = "database/kvstore_" ++ (show slaveId) ++ ".txt"

sendResponses :: ReaderT (MVar SlaveState) IO ()
sendResponses = do
  mvar <- ask
  liftIO $ do
    state <- readMVar mvar
    message <- readChan (channel state)

    let handler = case message of
                    (KVResponse _ _ _) -> handleResponse 
                    (KVRequest _ _)    -> handleRequest message
                    (KVVote _ _ _ _)   -> handleVote --protocol error?
                    (KVAck _ _)        -> handleAck -- protocol error?
                    (KVDecision _ _ _)   -> handleDecision message

    forkIO $ runReaderT handler mvar

  sendResponses

getMyId :: Lib.Config -> Int
getMyId cfg = fromJust $ Lib.slaveNumber cfg

handleRequest :: KVMessage -> ReaderT (MVar SlaveState) IO ()
handleRequest msg = do
  mvar <- ask
  liftIO $ do
    state <- readMVar mvar
    let config = cfg state

    h <- KVProtocol.connectToMaster config
    let field_txn  = txn_id msg
        field_request = request msg
        mySlaveId = getMyId config

    case field_request of
        PutReq key val -> do
          -- LOG READY, <timestamp, txn_id, key, newval>

          -- LOCK!!!@!@! !@ ! ! 
          LOG.writeReady (LOG.persistentLogName mySlaveId) msg
          -- UNLOCK

          KVProtocol.sendMessage h (KVVote field_txn mySlaveId VoteReady field_request)
          --vote abort if invalid key value
        GetReq key     -> do
          kvMap <- liftM Utils.readKVList $ B.readFile $ persistentFileName mySlaveId
          case (Map.lookup key $ Map.fromList kvMap) of
            Nothing -> KVProtocol.sendMessage h (KVResponse field_txn mySlaveId (KVSuccess key Nothing))
            Just val -> KVProtocol.sendMessage h (KVResponse field_txn mySlaveId (KVSuccess key (Just val)))

    IO.hClose h
  
handleDecision :: KVMessage -> ReaderT (MVar SlaveState) IO ()
handleDecision msg = do
  mvar <- ask
  liftIO $ do
    state <- readMVar mvar
    let config = cfg state
        field_txn  = txn_id msg
        field_request = request msg
        mySlaveId = getMyId config
        (key,val) = case field_request of
                      (PutReq k v) -> (k,v)
                      (GetReq _) -> undefined -- protocol error

    --DEAL WITH ABORT
    --USE BRACKET TO MAKE THIS ATOMIC
    --WRITE TO FILE
    kvMap <- liftM Utils.readKVList $ B.readFile $ persistentFileName mySlaveId
    let updatedKvMap = Map.insert key val (Map.fromList kvMap)

    traceIO $ show updatedKvMap
    B.writeFile (persistentFileName mySlaveId) (Utils.writeKVList $ Map.toList updatedKvMap)
    --WRITE TO LOG

    LOG.writeCommit (LOG.persistentLogName mySlaveId) msg
    --TODO!!! ! ! ! 

    h <- KVProtocol.connectToMaster config
    KVProtocol.sendMessage h (KVAck field_txn (Just mySlaveId))
    IO.hClose h

handleResponse = undefined
handleVote = undefined
handleAck = undefined
