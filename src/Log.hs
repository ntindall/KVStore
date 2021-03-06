{-# LANGUAGE OverloadedStrings #-}

module Log where

import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as C8
import qualified Data.Map as Map
import qualified Utils

import KVProtocol
import System.IO as IO

import Control.Applicative
import Data.List as List
import Data.Set as Set
import Data.Tuple.Utils
import Data.Maybe

import Debug.Trace

import Control.Monad

import System.FileLock

import qualified Utils

type KeyValueMapWithState = Map.Map B.ByteString ((KVKey, KVVal, B.ByteString), Bool)


getSeparator :: B.ByteString
getSeparator = " " --todo, escaping?

writeReady :: FilePath 
           -> KVMessage
           -> IO()
writeReady filename msg = do
  -- LOG READY, <timestamp, txn_id, key, newval>
  -- time <- Utils.currentTimeMicro
  let field_txn = txn_id msg
      field_request = request msg --use the timestamp the request was made
      sep = getSeparator
      logEntry = case field_request of
                  PutReq ts key val -> 
                    (C8.intercalate sep [C8.pack "READY", C8.pack $ show field_txn, key, val, C8.pack $ show ts]) `C8.append` (C8.pack "\n")
                  DelReq ts key -> 
                    (C8.intercalate sep [C8.pack "DELETE", C8.pack $ show field_txn, key, C8.pack $ show ts]) `C8.append` (C8.pack "\n")

  withFileLock filename Exclusive (\_ -> B.appendFile filename logEntry)

flush :: FilePath -> IO ()
flush filename = B.writeFile filename B.empty

writeCommit :: FilePath -> KVMessage -> IO()
writeCommit filename msg = do
  -- LOG COMMIT, <timestamp, txn_id>
  time <- Utils.currentTimeMicro
  let field_txn = txn_id msg
      sep = getSeparator
      logEntry = (C8.intercalate sep [C8.pack "COMMIT", C8.pack $ show field_txn, C8.pack $ show time]) `C8.append` (C8.pack "\n")

  withFileLock filename Exclusive (\_ -> B.appendFile filename logEntry)

writeAbort :: FilePath -> KVMessage -> IO()
writeAbort filename msg = do
  -- LOG ABORT, <timestamp, txn_id>
  time <- Utils.currentTimeMicro
  let field_txn = txn_id msg
      sep = getSeparator
      logEntry = (C8.intercalate sep [C8.pack "ABORT", C8.pack $ show field_txn, C8.pack $ show time]) `C8.append` (C8.pack "\n")

  withFileLock filename Exclusive (\_ -> B.appendFile filename logEntry)


-- | Rebuild the in memory store using the log, redoing all committed actions that
-- occured after a checkpoint
rebuild :: FilePath -> Map.Map KVKey (KVVal, KVTime) -> IO (Map.Map KVTxnId KVMessage, Map.Map KVKey (KVVal, KVTime))
rebuild logPath oldStore = do

  file <- B.readFile logPath
  let lines = C8.split '\n' file

  (unfinishedReqs, updatedStore) <- handleLines lines Map.empty oldStore

  let unfinishedReqList = Map.toList unfinishedReqs

  return (Map.fromList $ List.map (\(txn_id,(k,v,ts)) -> 
                                    let txn_id' = read (C8.unpack txn_id) :: (Int, Int)
                                        ts_int  = read (C8.unpack ts)     :: KVTime
                                    in case v of 
                                      (Just v') -> (txn_id', KVRequest txn_id' (PutReq ts_int k v'))
                                      Nothing   -> (txn_id', KVRequest txn_id' (DelReq ts_int k))
                                  ) unfinishedReqList
         , updatedStore)

handleLines :: [B.ByteString]                      --each line in the file
            -> Map.Map B.ByteString (KVKey, Maybe KVVal, B.ByteString) --unmatched ready map accumulator
            -> Map.Map KVKey (KVVal, KVTime)               --store accumulator
            -> IO(Map.Map B.ByteString (KVKey, Maybe KVVal, B.ByteString), Map.Map KVKey (KVVal, KVTime))
handleLines [] unmatchedReadyMap storeMap = return (unmatchedReadyMap, storeMap)
handleLines (x:xs) unmatchedReadyMap storeMap 
  | C8.null x = handleLines xs unmatchedReadyMap storeMap
  | C8.length x < 3 = do
      IO.putStr $ "Error, corrupted logfile..."
      return (Map.empty, Map.empty) --todo.... exception?
  | otherwise = do
    let pieces = C8.split ' ' x
        action = pieces !! 0
        txn_id = pieces !! 1
    case action of 
      "READY" -> do
        let key = pieces !! 2
            val = pieces !! 3
            ts  = pieces !! 4
            unmatchedReadyMap' = Map.insert txn_id (key, (Just val), ts) unmatchedReadyMap
        handleLines xs unmatchedReadyMap' storeMap
      "DELETE" -> do
        let key = pieces !! 2
            ts = pieces !! 3
            unmatchedReadyMap' = Map.insert txn_id (key, Nothing, ts) unmatchedReadyMap
        handleLines xs unmatchedReadyMap' storeMap
      "COMMIT" -> 
        case Map.lookup txn_id unmatchedReadyMap of
          (Just (k,v,ts)) -> do
            let oldVal = Map.lookup k storeMap
                ts_int = read (C8.unpack ts) :: KVTime
            
            --SAFE UPDATE (timestamp check)
            if isNothing oldVal || snd (fromJust oldVal) <= ts_int
            then 
                handleLines xs (Map.delete txn_id unmatchedReadyMap) 
                               (if isNothing v then Map.delete k storeMap
                                               else (Map.insert k (fromJust v, ts_int) storeMap))
            else handleLines xs (Map.delete txn_id unmatchedReadyMap) 
                                storeMap
          Nothing -> do
            handleLines xs unmatchedReadyMap storeMap

      "ABORT" -> do
        -- delete the transaction id from the unmatched readyMap
        handleLines xs (Map.delete txn_id unmatchedReadyMap) storeMap

persistentLogName :: Int -> String
persistentLogName workerId = "database/logs/log_kvstore_" ++ show workerId ++ ".txt"