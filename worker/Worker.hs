{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Lib
import System.IO as IO
import System.Directory as DIR
import System.FileLock
import Network as NETWORK
import Network.Socket as SOCKET hiding (listen)
import Network.BSD as BSD
import Data.Maybe
import Data.List as List
import qualified Data.Map.Strict as Map

import Data.ByteString.Lazy as B
import Data.ByteString.Lazy.Char8 as C8
import Data.Tuple.Utils

import Control.Exception
import Control.Concurrent
import Control.Concurrent.Chan
import Control.Monad.Reader
import Control.Monad.Catch as Catch
import Control.Arrow as CA

import Control.Monad
import Control.Concurrent.MState

import Debug.Trace

import qualified KVProtocol (getMessage, sendMessage, connectToHost)
import KVProtocol hiding (getMessage, sendMessage, connectToHost)

import Log as LOG
import qualified Utils

import Math.Probable
import System.Posix.Signals
import System.FileLock

type WorkerId = Int

data WorkerState = WorkerState {
                  --Socket on which this node listens for incoming connections
                  receiver :: SOCKET.Socket
                  -- Incoming messages are received in separate threads and written
                  -- to this channel. Another thread reads from this channel and 
                  -- forks handlers that then manipulate the WorkerState and 
                  -- send appropriate responses to other nodes.
                , channel :: Chan KVMessage
                  -- Static config data instantiated at runtime
                , cfg :: Lib.Config
                  -- The in memory store. While the node is operational, the store
                  -- represents the total state of the values that should be stored
                  -- on it.
                , store :: Map.Map KVKey (KVVal, KVTime)
                  -- The transactions that have not yet been completed (either
                  -- COMMITed or ABORTed). Internal state required for appropriate
                  -- behavior of this node.
                , unresolvedTxns :: Map.Map KVTxnId KVMessage
                  -- The set of transcations recovered from the log. The node will
                  -- not flush its log until all of these transactions have been
                  -- ACKED
                , recoveredTxns :: Map.Map KVTxnId KVMessage
                  -- Write handle for communication with the master
                , sender :: MVar Handle
                }
 --  deriving (Show)

main :: IO ()
main = do
  success <- Lib.parseArguments

  case success of
    Nothing     -> Lib.printUsage --an error occured
    Just config ->
      let workerId = fromJust (Lib.workerNumber config)
      in if workerId <= (-1) || workerId >= List.length (Lib.workerConfig config)
         then Lib.printUsage --error
         else do
          let main_ = evalMState True runKVWorker $ WorkerState { cfg = config 
                                                         ,  store = Map.empty
                                                         ,  unresolvedTxns = Map.empty
                                                         ,  recoveredTxns = Map.empty 
                                                         } 
              --install trap
              handler = Catch (do
                threadDelay 1000000
                traceShowM $ "... [!][!][!] DEATH OCCURED... REBOOTING [!][!][!] ..."
                main_ >>= \_ -> return ())
          installHandler sigABRT handler Nothing
          main_


runKVWorker :: MState WorkerState IO ()
runKVWorker = get >>= \s -> do 
  c <- liftIO newChan

  liftIO $ IO.putStrLn "[!] REBUILDING... "

  let config = cfg s
      workerId = fromJust $ Lib.workerNumber config
      (workerName, workerPortId@(PortNumber portNum)) = Lib.workerConfig config !! workerId
  skt <- liftIO $ KVProtocol.listenOnPort portNum -- $ listenOn workerPortId

  fileExists <- liftIO $ DIR.doesFileExist (LOG.persistentLogName workerId)
  if fileExists
  then do
    store <- liftIO $ liftM (Map.fromList . Utils.readKVList) $ B.readFile $ persistentFileName workerId
    (unackedTxns, store') <- liftIO $ LOG.rebuild (LOG.persistentLogName workerId) store

    let persistentFile = (persistentFileName workerId)
        bakFile = persistentFile ++ ".bak"

    liftIO $ withFileLock persistentFile Exclusive
                          (\_ -> withFileLock bakFile Exclusive
                            (\_ -> do
                              B.writeFile bakFile (Utils.writeKVList $ Map.toList store')
                              DIR.renameFile bakFile persistentFile
                            )
                          )

    modifyM_ $ \s -> s { channel = c, store = store', recoveredTxns = unackedTxns }
  else do
   -- IO.openFile (LOG.persistentLogName workerId) IO.AppendMode
    modifyM_ $ \s -> s { channel = c, store = Map.empty }
    return ()
  liftIO $ IO.putStrLn "[!] DONE REBUILDING... "

  -- TODO: dynamic worker registration, for now, instantiate based upon command
  -- line arguments
  sndr <- liftIO $ do
    s <- KVProtocol.connectToHost (Lib.masterHostName config)
                                  (Lib.masterPortId config)
    newMVar s

  modifyM_ $ \s -> s { receiver = skt, sender = sndr }

  forkM_ sendResponses
  forkM_ checkpoint

  listen

checkpoint :: MState WorkerState IO ()
checkpoint = get >>= \s -> do 
  liftIO $ do
    let config = cfg s
        myWorkerId = fromJust (Lib.workerNumber config)

    IO.putStrLn "[!][!][!] CHECKPOINT"
    --If we have fully recovered
    if (Map.null $ recoveredTxns s)
    then do
      --lock down the log file, cannot have race between checkpoint being written
       --and log being flushed.
      withFileLock (LOG.persistentLogName myWorkerId) Exclusive
                   (\_ -> withFileLock (persistentFileName myWorkerId) Exclusive
                      (\_ -> do
                        B.writeFile (persistentFileName myWorkerId) (Utils.writeKVList $ Map.toList (store s))
                        LOG.flush (LOG.persistentLogName myWorkerId) 
                      )
                   )
    else do
      withFileLock (persistentFileName myWorkerId) Exclusive 
                   (\_ -> B.writeFile (persistentFileName myWorkerId) 
                                      (Utils.writeKVList $ Map.toList (store s))
                   )

  liftIO $ threadDelay 10000000
  checkpoint

-- Listen and write to thread-safe channel
listen :: MState WorkerState IO ()
listen = get >>= \s -> do
  h <- liftIO $ do
    (conn,_) <- SOCKET.accept $ receiver s
    IO.putStrLn "[!] New connection established!"
    socketToHandle conn ReadMode 

  liftIO $ hSetBuffering h LineBuffering
  forkM_ (channelWriter h)
  
  listen

channelWriter :: Handle                       --Read handle, the master is a writer
                                              --to this handle
              -> MState WorkerState IO ()
channelWriter h = get >>= \s -> do
  isEOF <- liftIO $ hIsClosed h >>= (\b -> if b then return b else hIsEOF h)
  if isEOF then do
    liftIO $ do
      IO.putStr "[!] Closing writer handle"
      hClose h >>= (\_ -> return ())
  else do
    liftIO $ do
      message <- KVProtocol.getMessage h
      either (IO.putStr . show . (++ "\n")) 
             (writeChan $ channel s)
             message
  
    channelWriter h 


persistentFileName :: Int    --the workerId
                   -> String --the persistentFile name associated with this worker
persistentFileName workerId = "database/kvstore_" ++ show workerId ++ ".txt"

-- | Channel reading thread. Reads most recent message from the channel (blocks until
-- a message exists. Then forks a handler that manipulates the WorkerState appropriately.
-- The WorkerState is threaded through each thread through the use of forkM_
sendResponses :: MState WorkerState IO ()
sendResponses = get >>= \s -> do
  message <- liftIO $ readChan (channel s)

  thunk <- liftIO $ do
            return $ case message of
              KVResponse{} -> handleResponse
              KVRequest{}  -> handleRequest message
              KVVote{}     -> handleVote --protocol error?
              KVAck{}      -> handleAck -- protocol error?
              KVDecision{} -> handleDecision message

  forkM_ thunk

  sendResponses

getMyId :: Lib.Config -> Int
getMyId cfg = fromJust $ Lib.workerNumber cfg

handleRequest :: KVMessage -> MState WorkerState IO ()
handleRequest msg = get >>= \s -> do
  let config = cfg s

  let field_txn  = txn_id msg
      field_request = request msg
      myWorkerId = getMyId config

  case field_request of
      PutReq ts key val -> do
        -- LOG READY, <timestamp, txn_id, key, newval>
        
        modifyM_ $ \s' ->
          let updatedTxnMap = Map.insert field_txn msg (unresolvedTxns s')
          in s' {unresolvedTxns = updatedTxnMap}

        liftIO $ do
          LOG.writeReady (LOG.persistentLogName myWorkerId) msg
          KVProtocol.sendMessage (sender s) (KVVote field_txn myWorkerId VoteReady field_request)

      --todo, comment
      DelReq ts key -> do
        modifyM_ $ \s' ->
          let updatedTxnMap = Map.insert field_txn msg (unresolvedTxns s')
          in s' {unresolvedTxns = updatedTxnMap}

        liftIO $ do
          LOG.writeReady (LOG.persistentLogName myWorkerId) msg
          KVProtocol.sendMessage (sender s) (KVVote field_txn myWorkerId VoteReady field_request)

      GetReq _ key     -> liftIO $ do 
        case Map.lookup key (store s)  of
          Nothing -> KVProtocol.sendMessage (sender s) (KVResponse field_txn myWorkerId (KVSuccess key Nothing))
          Just val -> KVProtocol.sendMessage (sender s) (KVResponse field_txn myWorkerId (KVSuccess key (Just (fst val))))

safeUpdateStore :: KVKey 
                -> Maybe KVVal 
                -> KVTime 
                -> MState WorkerState IO ()
safeUpdateStore k v ts = modifyM_ $ \s -> do
  let oldVal = Map.lookup k (store s)

  if isNothing oldVal || snd (fromJust oldVal) <= ts 
  then 
    if isJust v then s { store = Map.insert k (fromJust v,ts) (store s)}
                else s { store = Map.delete k (store s)}
  else s

clearTxn :: KVTxnId -> MState WorkerState IO ()
clearTxn tid = modifyM_ $ \s -> s { unresolvedTxns = Map.delete tid (unresolvedTxns s)
                                  , recoveredTxns = Map.delete tid (recoveredTxns s)
                                  }

--Lookup the value in both transaction maps
lookupTX :: KVTxnId -> WorkerState -> Maybe KVMessage
lookupTX tid s = let inR = Map.lookup tid (unresolvedTxns s)
                 in if (isJust inR) then inR else Map.lookup tid (recoveredTxns s)

handleDecision :: KVMessage -> MState WorkerState IO ()
handleDecision msg@(KVDecision tid decision _) = get >>= \s -> do
  --REMOVE THIS IF YOU WANT WORKER TO NOT DIE PROBABALISTICALLY
  death <- liftIO $ mwc $ intIn (1,100)
  --don't make death too probable, leads to unrealistic problems (OS type issues)
  if (death > 99) then liftIO $ raiseSignal sigABRT --die "--DIE!"
  else do 
    let config = cfg s
        maybeRequest = lookupTX tid s
        myWorkerId = getMyId config

    if isJust maybeRequest 
    then do
      let field_request = fromJust $ maybeRequest

      if decision == DecisionCommit 
      then do
        case request field_request of -- TODO, do we even need the decision to ahve the request anymore?
          (PutReq ts k v) -> safeUpdateStore k (Just v) ts
          (DelReq ts k)   -> safeUpdateStore k Nothing ts
          (GetReq ts _) -> undefined -- protocol error

        liftIO $ LOG.writeCommit (LOG.persistentLogName myWorkerId) msg
      else do
        liftIO $ LOG.writeAbort (LOG.persistentLogName myWorkerId) msg

      clearTxn tid
      let errormsg = if decision == DecisionCommit then Nothing
                     else Just $ C8.pack "Transaction aborted"
      liftIO $ do
        KVProtocol.sendMessage (sender s) (KVAck tid (Just myWorkerId) errormsg)
    else --nothing to do, but need to ACK to let master know we are back alive
      liftIO $ do
        KVProtocol.sendMessage (sender s) (KVAck tid (Just myWorkerId) (Just $ C8.pack (show decision)))

handleResponse = undefined
handleVote = undefined
handleAck = undefined
