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

import System.Exit

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
                , store :: Map.Map KVKey KVVal
                , unresolvedTxns :: Map.Map KVTxnId KVMessage
                }
 --  deriving (Show)

main :: IO ()
main = do
  success <- Lib.parseArguments
  case success of
    Nothing     -> Lib.printUsage --an error occured
    Just config ->
      let slaveId = fromJust (Lib.slaveNumber config)
      in if slaveId <= (-1) || slaveId >= List.length (Lib.slaveConfig config)
         then Lib.printUsage --error
         else do
           mvar <- newMVar SlaveState {cfg = config, store = Map.empty, unresolvedTxns = Map.empty }
           runReaderT runKVSlave mvar

runKVSlave :: ReaderT (MVar SlaveState) IO ()
runKVSlave = do
  mvar <- ask
  c <- liftIO newChan

  liftIO $ do
    state <- readMVar mvar
    let config = cfg state
        slaveId = fromJust $ Lib.slaveNumber config
        (slaveName, slavePortId) = Lib.slaveConfig config !! slaveId
    skt <- listenOn slavePortId

    fileExists <- DIR.doesFileExist (LOG.persistentLogName slaveId)
    if fileExists
    then do
      store <- liftM (Map.fromList . Utils.readKVList) $ B.readFile $ persistentFileName slaveId
      (recoveredTxns, store') <- LOG.rebuild (LOG.persistentLogName slaveId) store

      B.writeFile ((persistentFileName slaveId) ++ ".bak") (Utils.writeKVList $ Map.toList store')
      --clear the log file
      --B.writeFile (LOG.persistentLogName slaveId) B.empty
      --TODO: don't clear log until all of these recoveredTxns ahve been acked (need either separate map or tuple).
      --overwrite the old store
      DIR.renameFile ((persistentFileName slaveId) ++ ".bak") (persistentFileName slaveId)

      state' <- takeMVar mvar
      putMVar mvar $ state' { socket = skt, channel = c, store = store', unresolvedTxns = recoveredTxns }
    else do
     -- IO.openFile (LOG.persistentLogName slaveId) IO.AppendMode
      state' <- takeMVar mvar
      putMVar mvar $ state' { socket = skt, channel = c, store = Map.empty }
      return ()

    forkIO $ runReaderT sendResponses mvar
    forkIO $ runReaderT checkpoint mvar

  processMessages

checkpoint :: ReaderT (MVar SlaveState) IO ()
checkpoint = do 
  mvar <- ask
  liftIO $ do
    state <- readMVar mvar
    let config = cfg state
        mySlaveId = fromJust (Lib.slaveNumber config)

    stateAtomic <- takeMVar mvar
    B.writeFile (persistentFileName mySlaveId) (Utils.writeKVList $ Map.toList (store stateAtomic))
    putMVar mvar stateAtomic

  -- sleep for 2 seconds
  liftIO $ threadDelay 2000000
  checkpoint

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
                     either (\err -> IO.putStr $ show err ++ "\n")
                            (\suc -> writeChan c suc)
                            msg
                   )


persistentFileName :: Int -> String  --todo, hacky
persistentFileName slaveId = "database/kvstore_" ++ show slaveId ++ ".txt"

sendResponses :: ReaderT (MVar SlaveState) IO ()
sendResponses = do
  mvar <- ask
  liftIO $ do
    state <- readMVar mvar
    message <- readChan (channel state)

    let handler = case message of
                    KVResponse{} -> handleResponse
                    KVRequest{}  -> handleRequest message
                    KVVote{}     -> handleVote --protocol error?
                    KVAck{}      -> handleAck -- protocol error?
                    KVDecision{} -> handleDecision message

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
        PutReq ts key val -> do
          -- LOG READY, <timestamp, txn_id, key, newval>

          state' <- takeMVar mvar
          let updatedTxnMap = Map.insert field_txn msg (unresolvedTxns state')
          putMVar mvar $ state' {unresolvedTxns = updatedTxnMap}

          -- LOCK!!!@!@! !@ ! !
          LOG.writeReady (LOG.persistentLogName mySlaveId) msg

          -- UNLOCK

          KVProtocol.sendMessage h (KVVote field_txn mySlaveId VoteReady field_request)
          --vote abort if invalid key value
        GetReq _ key     -> do
          case Map.lookup key (store state)  of
            Nothing -> KVProtocol.sendMessage h (KVResponse field_txn mySlaveId (KVSuccess key Nothing))
            Just val -> KVProtocol.sendMessage h (KVResponse field_txn mySlaveId (KVSuccess key (Just val)))

    IO.hClose h

handleDecision :: KVMessage -> ReaderT (MVar SlaveState) IO ()
handleDecision msg = do
  --liftIO exitSuccess --DIE!
  mvar <- ask
  liftIO $ do
    state <- readMVar mvar
    let config = cfg state
        field_txn  = txn_id msg
        field_request = request msg
        mySlaveId = getMyId config
        (key,val) = case field_request of -- TODO, do we even need the decision to ahve the request anymore?
                      (PutReq ts k v) -> (k,v)
                      (GetReq ts _) -> undefined -- protocol error

    --DEAL WITH ABORT
    --USE BRACKET TO MAKE THIS ATOMIC
    --WRITE TO FILE
    state' <- takeMVar mvar
    let updatedStore = Map.insert key val (store state')
        updatedTxnMap = Map.delete field_txn (unresolvedTxns state')
    putMVar mvar $ state' {store = updatedStore, unresolvedTxns = updatedTxnMap}

    --liftM Utils.readKVList $ B.readFile $ persistentFileName mySlaveId
    --let updatedKvMap = Map.insert key val (Map.fromList kvMap)

    --traceIO $ show updatedKvMap
    --B.writeFile (persistentFileName mySlaveId) (Utils.writeKVList $ Map.toList updatedKvMap)
    --WRITE TO LOG

    LOG.writeCommit (LOG.persistentLogName mySlaveId) msg
    --TODO!!! ! ! ! 

    h <- KVProtocol.connectToMaster config
    KVProtocol.sendMessage h (KVAck field_txn (Just mySlaveId) Nothing)
    IO.hClose h

handleResponse = undefined
handleVote = undefined
handleAck = undefined
