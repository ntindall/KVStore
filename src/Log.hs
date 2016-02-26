{-# LANGUAGE OverloadedStrings #-}

module Log where

import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as C8
import qualified Data.Map as Map
import qualified Utils

import KVProtocol
import System.IO as IO

import Data.Time
import Control.Applicative
import Data.Time.Clock.POSIX
import Data.List as List
import Data.Set as Set

import Debug.Trace

import Control.Monad

type KeyValueMapWithState = Map.Map B.ByteString ((KVKey, KVVal, B.ByteString), Bool)

--https://stackoverflow.com/questions/17909770/get-time-as-int
currentTimeInt :: IO Int
currentTimeInt = round `fmap` getPOSIXTime :: IO Int


getSeparator :: B.ByteString
getSeparator = " " --todo, escaping?

writeReady :: FilePath -> KVMessage -> IO()
writeReady filename msg = do
  -- LOG READY, <timestamp, txn_id, key, newval>
  time <- currentTimeInt
  let field_txn = txn_id msg
      field_request = request msg
      key = putkey field_request
      val = putval field_request
      sep = getSeparator
      logEntry = (C8.intercalate sep [C8.pack "READY", C8.pack $ show field_txn, key, val, C8.pack $ show time]) `C8.append` (C8.pack "\n")

  B.appendFile filename logEntry


writeCommit :: FilePath -> KVMessage -> IO()
writeCommit filename msg = do
  -- LOG COMMIT, <timestamp, txn_id>
  time <- currentTimeInt
  let field_txn = txn_id msg
      sep = getSeparator
      logEntry = (C8.intercalate sep [C8.pack "COMMIT", C8.pack $ show field_txn, C8.pack $ show time]) `C8.append` (C8.pack "\n")

  B.appendFile filename logEntry

--rebuild the in memory store using the log, redoing all committed actions that
--occured after a checkpoint
rebuild :: FilePath -> Map.Map B.ByteString B.ByteString -> IO (Map.Map B.ByteString B.ByteString, [(Int, Int)])
rebuild logPath oldStore = do

  file <- B.readFile logPath
  let lines = C8.split '\n' file

  (unfinishedReqs, updatedStore) <- handleLines lines Map.empty oldStore

  let unfinishedReqList = Map.toList unfinishedReqs
      unfinishedReqSorted = List.sortBy (\(_,((_, _, ts), _)) (_,((_, _, ts'), _)) -> ts `compare` ts') unfinishedReqList
      ackAndUpdateList = List.filter (\(_,((_, _, _), b)) -> b == False) unfinishedReqSorted
      updatedStore' = addToStore updatedStore ackAndUpdateList

  return (updatedStore', List.map (\(txn_id,_) -> 
                                    read (C8.unpack txn_id) :: (Int, Int)
                                  ) unfinishedReqSorted)

  where addToStore map [] = map
        addToStore map ((_,((k, v, _), _)):xs) = addToStore (Map.insert k v map) xs


handleLines :: [B.ByteString]                                    
            -> KeyValueMapWithState                 -- map from txn_id to ((KVKey, KVVal, Timestamp), bool) tuples, where bool is true
                                                    -- if the value should actually be writen to the store, and false if only
                                                    -- an ACK need to be sent back.
            -> Map.Map B.ByteString B.ByteString 
            -> IO (KeyValueMapWithState, Map.Map KVKey KVVal)
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
      "READY"  -> do
        let key = pieces !! 2
            val = pieces !! 3
            ts  = pieces !! 4
            unmatchedReadyMap' = Map.insert txn_id ((key, val, ts), True) unmatchedReadyMap
        handleLines xs unmatchedReadyMap' storeMap
      "COMMIT" -> do
        let ts = pieces !! 2
        case Map.lookup txn_id unmatchedReadyMap of
          (Just ((k,v,_), _)) -> do
            let storeMap' = Map.insert k v storeMap
                unmatchedReadyMap' = 
                  -- If there exists a Ready with a timestamp less than the current transaction's timestamp, which has
                  -- a ready any a commit, we set the bool value associated with that txn id in the readyMap to be false,
                  -- indicating that the txn_id should not be inserted into the store, but must be ACKed.
                  Map.mapWithKey (\txn_id ((k', v', ts'), b') ->
                    if k == k' && ts' < ts 
                    then ((k',v',ts'), False)
                    else ((k',v',ts'), b')
                  ) unmatchedReadyMap
            handleLines xs (Map.delete txn_id unmatchedReadyMap') storeMap'
          Nothing -> do
            IO.putStr $ "Error, corrupted logfile... unexpected COMMIT with no preceeding READY"
            return (Map.empty, Map.empty) -- todo, exception?

handleUnfinishedTxn :: FilePath -> FilePath -> IO [(Int,Int)]
handleUnfinishedTxn logPath storePath = undefined

--do
--  traceIO "attempting to rebuild"

--  file <- B.readFile logPath
--  let lines = reverse $ C8.split '\n' file
--      txnSet = Set.empty
--      unfinishedReqs = reverse $ handleLines lines txnSet []
--      reqTxnIdKVTuples = List.map (\line ->
--                          let pieces = C8.split ' ' line
--                          in (pieces !! 1, pieces !! 2, pieces !! 3))
--                          unfinishedReqs

--  traceIO $ show unfinishedReqs
--  mapM (\(id, k, v) -> do
--          updateKVStore storePath k v
--          let txn_id = read (C8.unpack id) :: (Int, Int)
--          writeCommit logPath $ KVRequest txn_id (PutReq k v)
--          return txn_id
--       ) reqTxnIdKVTuples


----TODO quickcheck
--handleLines :: [B.ByteString] -> Set.Set B.ByteString -> [B.ByteString] -> [B.ByteString]
--handleLines [] commitAcc readyAcc = readyAcc
--handleLines (x:xs) commitAcc readyAcc 
--  | C8.null x = handleLines xs commitAcc readyAcc
--  | otherwise = let pieces = C8.split ' ' x
--                    action = head pieces
--                    txn_id = (pieces !! 1)
--                in
--                  if action == "COMMIT"
--                  then
--                    let commitAcc' = Set.insert txn_id commitAcc
--                    in handleLines xs commitAcc' readyAcc
--                  else
--                    if Set.member txn_id commitAcc
--                    then handleLines xs commitAcc readyAcc
--                    else handleLines xs commitAcc (readyAcc ++ [x])


--todo, auto touch files if they are not there
persistentLogName :: Int -> String                                                   --todo, hacky
persistentLogName slaveId = "database/logs/log_kvstore_" ++ show slaveId ++ ".txt"

updateKVStore :: FilePath -> B.ByteString -> B.ByteString -> IO ()
updateKVStore filePath key val = do
  kvMap <- liftM Utils.readKVList $ B.readFile filePath
  let updatedKvMap = Map.insert key val (Map.fromList kvMap)
  traceIO $ show updatedKvMap
  B.writeFile filePath (Utils.writeKVList $ Map.toList updatedKvMap)
