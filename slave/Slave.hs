{-# LANGUAGE OverloadedStrings #-}

module Main where

import Lib
import System.IO as IO
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
import Control.Arrow as CA

import Control.Monad

import Debug.Trace

import KVProtocol
import Log as LOG

--- todo, touch file when slave registers
-- todo, slave registration

type SlaveId = Int

data SlaveState = SlaveState {
                  socket :: Socket
                 , channel :: Chan KVMessage
                 , cfg :: Lib.Config
                }
  -- deriving (Show)

main :: IO ()
main = do
  success <- Lib.parseArguments
  case success of
    Nothing     -> Lib.printUsage --an error occured
    Just config -> 
      let slaveId = fromMaybe (-1) (Lib.slaveNumber config)
      in if (slaveId <= (-1) || slaveId >= List.length (Lib.slaveConfig config))
         then Lib.printUsage --error
         else do
           mvar <- newMVar SlaveState {cfg = config}
           runReaderT runKVSlave mvar

runKVSlave :: ReaderT (MVar SlaveState) IO ()
runKVSlave = do
   mvar <- ask
   liftIO $ do
     state <- readMVar mvar
     let (slaveName, slavePortId) = (Lib.slaveConfig (cfg state)) !! (fromJust $ slaveNumber $ cfg state)
     skt <- listenOn slavePortId
     _ <- takeMVar mvar
     c <- newChan
     putMVar mvar state { socket = skt, channel = c }
     forkIO $ runReaderT sendResponses mvar
   return ()
    -- channel <- newChan
    -- forkIO $ sendResponses channel cfg -- fork a child
    -- processMessages s channel cfg
    -- return ()

-- processMessages :: ReaderT (MVar SlaveState) IO ()
-- processMessages s channel cfg =
--   bracket (accept s)
--           (\(h,_,_) -> do
--             hClose h
--             processMessages s channel cfg)
--           (\(h, hostName, portNumber) -> do
--             msg <- getMessage h
--             either (\err -> IO.putStr $ (show err) ++ ['\n'])
--                    (\suc -> do
--                       IO.putStr $ (show suc) ++ ['\n'] --print the message 
--                       writeChan channel suc
--                     )
--                    msg
--           )

writeKVList :: [(B.ByteString, B.ByteString)] -> B.ByteString
writeKVList kvstore = C8.intercalate (C8.pack "\n") $ Prelude.map helper kvstore
  where helper (k,v) = C8.concat [k, (C8.pack "="), v]

readKVList :: B.ByteString -> [(B.ByteString, B.ByteString)]
readKVList = Prelude.map parseField . C8.lines
  where parseField = second (C8.drop 1) . C8.break (== '=')

persistentFileName :: Int -> String                                                   --todo, hacky
persistentFileName slaveId = "database/kvstore_" ++ (show slaveId) ++ ".txt"

sendResponses :: ReaderT (MVar SlaveState) IO ()
sendResponses = do
  mvar <- ask
  liftIO $ do
    state <- readMVar mvar
    message <- readChan (channel state)

    forkIO $ do
      case message of
        (KVResponse _ _ _) -> handleResponse 
        (KVRequest _ _)    -> handleRequest (cfg state) message
        (KVVote _ _ _ _)   -> handleVote --protocol error?
        (KVAck _ _)        -> handleAck -- protocol error?
        (KVDecision _ _ _)   -> handleDecision (cfg state) message

  sendResponses

getMyId :: Lib.Config -> Int
getMyId cfg = fromJust $ Lib.slaveNumber cfg

handleRequest :: Lib.Config -> KVMessage -> IO()
handleRequest cfg msg = do
  h <- connectTo (Lib.masterHostName cfg) (Lib.masterPortId cfg)
  let field_txn  = txn_id msg
      field_request = request msg
      mySlaveId = getMyId cfg

  case field_request of
      PutReq key val -> do
        -- LOG READY, <timestamp, txn_id, key, newval>

        -- LOCK!!!@!@! !@ ! ! 
        LOG.writeReady (LOG.persistentLogName mySlaveId) msg
        -- UNLOCK

        sendMessage h (KVVote field_txn mySlaveId VoteReady field_request)
        --vote abort if invalid key value
      GetReq key     -> do
        kvMap <- liftM readKVList $ B.readFile $ persistentFileName mySlaveId
        case (Map.lookup key $ Map.fromList kvMap) of
          Nothing -> sendMessage h (KVResponse field_txn mySlaveId (KVSuccess key Nothing))
          Just val -> sendMessage h (KVResponse field_txn mySlaveId (KVSuccess key (Just val)))

  IO.hClose h
  
handleDecision :: Lib.Config -> KVMessage -> IO()
handleDecision cfg msg = do
  let field_txn  = txn_id msg
      field_request = request msg
      mySlaveId = getMyId cfg
      (key,val) = case field_request of
                    (PutReq k v) -> (k,v)
                    (GetReq _) -> undefined -- protocol error

  --DEAL WITH ABORT
  --USE BRACKET TO MAKE THIS ATOMIC
  --WRITE TO FILE
  kvMap <- liftM readKVList $ B.readFile $ persistentFileName mySlaveId
  let updatedKvMap = Map.insert key val (Map.fromList kvMap)
  traceIO $ show updatedKvMap
  B.writeFile (persistentFileName mySlaveId) (writeKVList $ Map.toList updatedKvMap)
  --WRITE TO LOG

  LOG.writeCommit (LOG.persistentLogName mySlaveId) msg
  --TODO!!! ! ! ! 

  h <- connectTo (Lib.masterHostName cfg) (Lib.masterPortId cfg)
  sendMessage h (KVAck field_txn (Just mySlaveId))
  IO.hClose h

handleResponse = undefined
handleVote = undefined
handleAck = undefined
