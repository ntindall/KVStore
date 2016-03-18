 
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Main where

import Lib
import System.IO as IO
import Network
import FarmHash as HASH

import Data.ByteString.Lazy as B
import Data.ByteString.Lazy.Char8 as C8
import Data.Tuple.Utils

import qualified Data.List as L
import qualified Data.Map.Strict as Map
import qualified Data.Set as S
import qualified Data.Sequence as Seq
import qualified Data.Foldable as Foldable

import Data.Maybe

import Network.Socket as SOCKET hiding (listen)

import Control.Concurrent.MState
import Control.Monad.State

import Control.Exception
import Control.Monad.Reader
import Control.Monad.Catch as Catch
import qualified Control.Monad.Error as MonadError
import Control.Concurrent
import Control.Concurrent.Chan
import Control.Concurrent.Thread.Delay

import System.Directory as DIR

import qualified KVProtocol (getMessage, sendMessage, kV_TIMEOUT_MICRO)
import KVProtocol hiding (getMessage, sendMessage, connectToMaster)

import Debug.Trace

import qualified Utils as U

type WkrId = Int

--TODO
--https://hackage.haskell.org/package/lrucache-1.2.0.0/docs/Data-Cache-LRU.html
data MasterState = MasterState {
                  -- Socket on which this node listens for incoming connections
                  receiver :: Socket
                  -- Incoming messages are received in separate threads and written
                  -- to this channel. Another thread reads from this channel and 
                  -- forks handlers that then manipulate the MasterState and 
                  -- send appropriate responses to other nodes.
                , channel :: Chan KVMessage
                  -- Static config data instantiated at runtime
                , cfg :: Lib.Config
                  -- Map representing the current state of the transcations that
                  -- the master is handling.
                , txs :: Map.Map KVTxnId TX
                  -- Map representing the write handles from the master to each client
                , clntHMap :: Map.Map Int (MVar Handle)
                  -- Map representing the write handles from the master to each worker
                , wkrHMap :: Map.Map Int (MVar Handle)
                }
                   
data TXState = VOTE | ACK | RESPONSE
  deriving (Show, Eq, Ord)

data TX = TX { 
               -- VOTE = PHASE1, ACK = PHASE 2, RESPONSE = GET
               txState :: TXState
               -- The set of worker nodes who have either responded with a VOTE
               -- or with an ACK for a decision. This set is cleared when the 
               -- transaction is moved into phase 2.
             , responded :: S.Set WkrId
               -- The decision that was made for this transaction (if any).
             , kvDecision :: Maybe KVDecision
               -- The time at which the message was issued by the client
             , timeout :: KVTime
               -- The message itself
             , message :: KVMessage
             }
 deriving (Show)

data KVWorker = KVWorker { wkrID :: Int , host :: HostName,  port :: PortID }
  deriving (Show)

instance Show (MasterState) where
  show (MasterState skt _ cfg txs _ _) = 
    show skt ++ show cfg ++ show txs

-- | Entry Point
main :: IO ()
main = Lib.parseArguments >>= \args -> case args of
  Nothing -> Lib.printUsage -- an error occured 
  Just c -> do
    --INITIALIZE THE DATABASE STATE--
    --DANGEROUS CODE BELOW--
    DIR.removeDirectoryRecursive "database"
    DIR.createDirectory "database"
    DIR.createDirectory "database/logs"
    ---------------------------------
    let workers = L.mapAccumL (\i (h,p) -> (i+1, KVWorker i h p)) 0 (workerConfig $ c)

    workers' <- mapM (\wkr -> do
                  s <- liftIO $ KVProtocol.connectToHost (host wkr) (port wkr)
                  m <- liftIO $ newMVar s

                  liftIO $ return (wkrID wkr, m)
               ) (snd workers)

    let pid@(PortNumber pno) = Lib.masterPortId c
    recvSock <- KVProtocol.listenOnPort pno
    chan <- newChan

    evalMState True initMaster (MasterState recvSock chan c Map.empty Map.empty (Map.fromList workers'))

-- | Initialization for Master Node
initMaster :: MState MasterState IO ()
initMaster = get >>= \s -> do
  liftIO $ IO.putStrLn "[!] Initializing Master"

  forkM_ $ listen
  forkM_ $ timeoutThread
  processMessages

-- | Listen then fork a channel writer
listen :: MState MasterState IO ()
listen = get >>= \s -> do
  h <- liftIO $ do
    (conn,_) <- SOCKET.accept $ receiver s
    IO.putStrLn "[!] New connection established!"
    socketToHandle conn ReadMode 

  liftIO $ hSetBuffering h LineBuffering
  forkM_ (channelWriter h)
  
  listen

-- | Listen and write to thread-safe channel
channelWriter :: Handle                   --Read handle. Either a client or a 
                                          --worker may be a writer to the handle
              -> MState MasterState IO ()
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

-- | Channel reading thread. Reads most recent message from the channel (blocks until
-- a message exists. Then forks a handler that manipulates the MasterState appropriately.
-- The MasterState is threaded through each thread through the use of forkM_
processMessages :: MState MasterState IO ()
processMessages = get >>= \s -> do
  message <- liftIO $ readChan $ channel s
  forkM_ $ processMessage message
  processMessages

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- | Message handling function. Pattern matches on the type of message received
-- and manipulates the MasterState appropriately.
processMessage :: KVMessage                      --the message 
               -> MState MasterState IO ()
processMessage (KVVote _ _ VoteAbort _) = undefined
--TODO: When receive an abort send an abort to everyone.
--      this is a protocol error

processMessage (KVVote tid sid VoteReady request) = do
  workerResponded tid sid
  commit <- isComplete tid
  timeout <- timedOut tid

  when (commit || timeout) $ modifyM_ $ \s -> do
    let tx = lookupTX tid s
        tx' = fromJust tx
    if (isJust tx) then updateTX tid (tx' { responded = S.empty
                                          , txState = ACK
                                          , kvDecision = if commit then Just DecisionCommit else Just DecisionAbort
                                          }) s else s

  if commit then sendDecisionToRing (KVDecision tid DecisionCommit request) 
  else when timeout $ sendDecisionToRing (KVDecision tid DecisionAbort request)

processMessage kvMsg@(KVResponse tid sid _) = do
  workerResponded tid sid
  complete <- isComplete tid
  if complete then sendMsgToClient kvMsg >>= (\_ -> clearTX tid) else (liftIO $ return ())


processMessage kvMsg@(KVRequest tid req) = do
  now <- liftIO U.currentTimeMicro
  let txstate = case req of
        GetReq{} -> RESPONSE
        PutReq{} -> VOTE
        DelReq{} -> VOTE
  --Communicating with shard for the first time, keep track of this in the timeout map.
  modifyM_ $ \s -> addTX tid (TX txstate S.empty Nothing now kvMsg) s
  sendMsgToRing kvMsg

processMessage (KVAck tid (Just sid) maybeSuccess) = do  
  workerResponded tid sid
  complete <- isComplete tid
  when complete $ do
    clearTX tid
    sendMsgToClient $ KVAck tid Nothing maybeSuccess

processMessage kvMsg@(KVRegistration txn_id hostName portId) = do
  let cfgTuple = (hostName, PortNumber $ toEnum portId)
  s <- get
  let oldConfig = cfg s
      oldClientCfg = Lib.clientConfig oldConfig       
      newClientCfg = oldClientCfg ++ [cfgTuple]
      newConfig = oldConfig { clientConfig = newClientCfg }
      clientId = Prelude.length newClientCfg - 1

  modifyM_ $ \s -> do s { cfg = newConfig }
  s <- liftIO $ uncurry KVProtocol.connectToHost cfgTuple
  sMVar <- liftIO $ newMVar s

  modifyM_ $ \s' -> s' { clntHMap = Map.insert clientId sMVar (clntHMap s')}

  liftIO $ KVProtocol.sendMessage sMVar $ KVAck (clientId, snd txn_id) (Just clientId) Nothing

-- | TODO... better error handling if protocol is broken
processMessage _ = undefined


-- | Reads the current state of transactions that are currently being handled by
-- the master and aborts any of those that have exceeded the timeout threshold. 
-- Since elements are not removed from the map until the final ACK from a worker
-- node is received, this will continually send Decision messages to all nodes
-- that have not responded to the decision.
timeoutThread :: MState MasterState IO ()
timeoutThread = get >>= \s -> do

  now <- liftIO $ U.currentTimeMicro
  mapM_ (\(tid, tx) ->
          if (now - KVProtocol.kV_TIMEOUT_MICRO >= timeout tx) 
          then do
            -- Lookup the decision, if we decided to commit, it is possible that other
            -- nodes have also commited. In that case, we need to tell nodes that may
            -- have died to commit as well, in order to keep the store consistent. 
            let final_decision = fromMaybe DecisionAbort (kvDecision tx)
            if (txState tx == ACK)
              then sendDecisionToRing (KVDecision tid final_decision (request $ message tx))
            else do --is VOTE or RESPONSE
              sendDecisionToRing (KVDecision tid DecisionAbort (request $ message tx))
              -- TODO: could eagerly inform client that the request has timed out
              -- i.e sendMsgToClient (KVResponse tid (-1) (KVFailure (C8.pack "Timeout")))
              -- For now, wait for worker nodes to ACK aborts prior to responding.
              -- TODO: exponential backoff, store in transaction how long to wait on the next
              -- iteration. 
          else return ()
        ) $ Map.toList $ txs s

  liftIO $ delay (KVProtocol.kV_TIMEOUT_MICRO)
  timeoutThread

-- | Blindly forwards a message to the entire shard that should service the KVMessage
sendMsgToRing :: KVMessage -> MState MasterState IO ()
sendMsgToRing msg = consistentHashing msg >>= mapM_ (\n -> forkM_ $ forwardToWorker n msg)

-- | Forwards a KVDecision message to all nodes that have not yet ACKed the decision
sendDecisionToRing :: KVMessage -> MState MasterState IO ()
sendDecisionToRing msg@(KVDecision tid decision req) = get >>= \s -> do
  shard <- consistentHashing msg
  let tx = lookupTX (txn_id msg) s
      tx' = fromJust tx

  if isJust tx then mapM_ (\n -> if (not . S.member (wkrID n) $ responded $ tx') then forkM_ $ forwardToWorker n msg else return ()) shard
  else return ()

-- | Forwards a message to a worker represented by the KVWorker struct. If the
-- message fails to send, it is assumed that the connection died and a new one
-- is instantiated (and the state updated appropriately). 
forwardToWorker :: KVWorker 
                -> KVMessage 
                -> MState MasterState IO ()
forwardToWorker wkr msg = get >>= \s -> do
  let h = fromJust $ Map.lookup (wkrID wkr) (wkrHMap s)
  MonadError.catchError (liftIO $ KVProtocol.sendMessage h msg)
                        (\(e :: IOException) -> do 
                          --means that connection died, we need to reconnect
                          socket <- liftIO $ do
                            IO.putStr (show e)
                            delay 1000000
                            KVProtocol.connectToHost (host wkr) (port wkr)
                          sMVar <- liftIO $ newMVar socket
                          modifyM_ $ \s -> s { wkrHMap = Map.insert (wkrID wkr) sMVar (wkrHMap s)}

                          modifyM_ $ \s -> do
                            let tx = lookupTX (txn_id msg) s
                                tx' = fromJust tx
                            if isJust tx then updateTX (txn_id msg) (tx' { timeout = 0 } ) s else s
                        )

-- | Forwards a message to the client node. Extracts the client ID by examining
-- the first value of the KVTxnId type, which is the clientId, then looks up
-- which handle to send to in the client handle map.
sendMsgToClient :: KVMessage 
                -> MState MasterState IO ()
sendMsgToClient msg = get >>= \s -> liftIO $ do
  let clientId = fst (txn_id msg)
      clientCfgList = Lib.clientConfig $ cfg s

  if (clientId > Prelude.length clientCfgList - 1) then return ()
  else do
    -- TODO, what if client disconnected? need to add timeout logic here (or just stop if
    -- exception is thrown while trying to connectTo the client.
    KVProtocol.sendMessage (fromJust $ Map.lookup clientId (clntHMap s)) msg

--------------------------------------------------------------------------------
-------------------------------HELPER FUNCTIONS---------------------------------

workerResponded :: KVTxnId -> WkrId -> MState MasterState IO ()
workerResponded tid wkrId = modifyM_ $ \s -> do
  let tx = lookupTX tid s
      tx' = fromJust tx
  if isJust tx then updateTX tid (tx' { responded = S.insert wkrId (responded tx') }) s else s 

isComplete :: KVTxnId -> MState MasterState IO Bool
isComplete tid = get >>= \s -> do
  let tx = lookupTX tid s
      tx' = fromJust tx
  if isJust tx then return $ S.size (responded tx') == 2 else return False

timedOut :: KVTxnId -> MState MasterState IO Bool
timedOut tid = get >>= \s -> liftIO $ do
  let tx = lookupTX tid s
      tx' = fromJust tx
  now <- U.currentTimeMicro
  if isJust tx
  then return $ now - KVProtocol.kV_TIMEOUT_MICRO >= timeout tx'
  else return False

clearTX :: KVTxnId -> MState MasterState IO ()
clearTX tid = modifyM_ $ \s -> s { txs = Map.delete tid $ txs s }

addTX :: KVTxnId -> TX -> MasterState -> MasterState
addTX tid tx s = s { txs = Map.insert tid tx (txs s) }

-- containsTX :: KVTxnId -> MasterState -> Bool
-- containsTX tid s = if isNothing (Map.lookup tid $ txs s) then False else True

lookupTX :: KVTxnId -> MasterState -> Maybe TX
lookupTX tid s = Map.lookup tid $ txs s

-- | updateTX, mutates the state, must be called within a mutateM_ block
updateTX :: KVTxnId -> TX -> MasterState -> MasterState
updateTX tid tx s = s { txs = Map.insert tid tx (txs s) }

clearResponded :: KVTxnId -> MasterState -> MasterState
clearResponded tid s = do
  let tx = lookupTX tid s
      tx' = fromJust tx
  if isJust tx then updateTX tid (tx' { responded = S.empty }) s else s

-- | Polymorphic wrapper for the _consistentHashing function
consistentHashing :: KVMessage 
                  -> MState MasterState IO [KVWorker]
consistentHashing (KVRequest _ (GetReq _ k))      = consistentHashing_ k
consistentHashing (KVRequest _ (PutReq _ k _))    = consistentHashing_ k
consistentHashing (KVDecision _ _ (GetReq _ k))   = consistentHashing_ k
consistentHashing (KVDecision _ _ (PutReq _ k _)) = consistentHashing_ k


-- | Simple implementation of consistent hashing. Uses Farmhash to hash the
-- KVRequest associated with the KVMessage. Takes the mod of the hash by the
-- size of the ring to determine the first node that the data should be 
-- wrtten to (or searched at). The second node is just the current node + 1 (mod
-- the size of the ring
consistentHashing_ :: KVKey 
                   -> MState MasterState IO [KVWorker]
consistentHashing_ request = get >>= \s -> do
    let ring = (workerConfig $ cfg s)
        current = getHash request `mod` (L.length ring)
        successor = (current + 1) `mod` (L.length ring)
        shard = [uncurry (\h p -> KVWorker current h p) (ring !! current),
                 uncurry (\h p -> KVWorker successor h p) (ring !! successor)]
    return shard
    where getHash k = fromIntegral $ HASH.hash32 (B.toStrict k)
