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

writeReady :: FilePath -> KVMessage -> IO()
writeReady filename msg = do
  -- LOG READY, <timestamp, txn_id, key, newval>
  -- time <- Utils.currentTimeInt
  let field_txn = txn_id msg
      field_request = request msg
      field_issuedUTC = issuedUTC field_request --use the timestamp the request was made
      key = putkey field_request
      val = putval field_request
      sep = getSeparator
      logEntry = (C8.intercalate sep [C8.pack "READY", C8.pack $ show field_txn, key, val, C8.pack $ show field_issuedUTC]) `C8.append` (C8.pack "\n")

  withFileLock filename Exclusive (\_ -> B.appendFile filename logEntry)

flush :: FilePath -> IO ()
flush filename = B.writeFile filename B.empty

writeCommit :: FilePath -> KVMessage -> IO()
writeCommit filename msg = do
  -- LOG COMMIT, <timestamp, txn_id>
  time <- Utils.currentTimeInt
  let field_txn = txn_id msg
      sep = getSeparator
      logEntry = (C8.intercalate sep [C8.pack "COMMIT", C8.pack $ show field_txn, C8.pack $ show time]) `C8.append` (C8.pack "\n")

  withFileLock filename Exclusive (\_ -> B.appendFile filename logEntry)

writeAbort :: FilePath -> KVMessage -> IO()
writeAbort filename msg = do
  -- LOG ABORT, <timestamp, txn_id>
  time <- Utils.currentTimeInt
  let field_txn = txn_id msg
      sep = getSeparator
      logEntry = (C8.intercalate sep [C8.pack "ABORT", C8.pack $ show field_txn, C8.pack $ show time]) `C8.append` (C8.pack "\n")

  withFileLock filename Exclusive (\_ -> B.appendFile filename logEntry)


--rebuild the in memory store using the log, redoing all committed actions that
--occured after a checkpoint
rebuild :: FilePath -> Map.Map KVKey (KVVal, Int) -> IO (Map.Map KVTxnId KVMessage, Map.Map KVKey (KVVal, Int))
rebuild logPath oldStore = do

  file <- B.readFile logPath
  let lines = C8.split '\n' file

  (unfinishedReqs, updatedStore) <- handleLines lines Map.empty oldStore

  let unfinishedReqList = Map.toList unfinishedReqs

  return (Map.fromList $ List.map (\(txn_id,(k,v,ts)) -> 
                                    let txn_id' = read (C8.unpack txn_id) :: (Int, Int)
                                        ts_int  = read (C8.unpack ts)     :: Int
                                    in (txn_id', KVRequest txn_id' (PutReq ts_int k v))
                                  ) unfinishedReqList
         , updatedStore)

handleLines :: [B.ByteString]                      --each line in the file
            -> Map.Map B.ByteString (KVKey, KVVal, B.ByteString) --unmatched ready map accumulator
            -> Map.Map KVKey (KVVal, Int)               --store accumulator
            -> IO(Map.Map B.ByteString (KVKey, KVVal, B.ByteString), Map.Map KVKey (KVVal, Int))
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
            unmatchedReadyMap' = Map.insert txn_id (key, val, ts) unmatchedReadyMap
        handleLines xs unmatchedReadyMap' storeMap
      "COMMIT" -> 
        case Map.lookup txn_id unmatchedReadyMap of
          (Just (k,v,ts)) -> do
            let oldVal = Map.lookup k storeMap
                ts_int = read (C8.unpack ts) :: Int
            
            if isNothing oldVal || snd (fromJust oldVal) <= ts_int
            then handleLines xs (Map.delete txn_id unmatchedReadyMap) 
                                (Map.insert k (v, ts_int) storeMap)
            else handleLines xs (Map.delete txn_id unmatchedReadyMap) 
                                storeMap
          Nothing -> do
            handleLines xs unmatchedReadyMap storeMap

      "ABORT" -> do
        -- delete the transaction id from the unmatched readyMap
        handleLines xs (Map.delete txn_id unmatchedReadyMap) storeMap

--todo, auto touch files if they are not there
persistentLogName :: Int -> String                                                   --todo, hacky
persistentLogName slaveId = "database/logs/log_kvstore_" ++ show slaveId ++ ".txt"

--updateKVStore :: FilePath -> B.ByteString -> B.ByteString -> IO ()
--updateKVStore filePath key val = do
--  kvMap <- liftM Utils.readKVList $ B.readFile filePath
--  let updatedKvMap = Map.insert key val (Map.fromList kvMap)
--  traceIO $ show updatedKvMap
--  B.writeFile filePath (Utils.writeKVList $ Map.toList updatedKvMap)
