{-# LANGUAGE OverloadedStrings #-}

module Log where

import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as C8
import qualified Data.Map as Map
import qualified Utils

import KVProtocol
import System.IO

import Data.Time
import Control.Applicative
import Data.Time.Clock.POSIX
import Data.List as List
import Data.Set as Set

import Debug.Trace

import Control.Monad
import KVProtocol

--https://stackoverflow.com/questions/17909770/get-time-as-int
currentTimeInt :: IO (Int)
currentTimeInt = round `fmap` getPOSIXTime :: IO(Int)


getSeparator :: Char
getSeparator = ' ' --todo, escaping?

writeReady :: FilePath -> KVMessage -> IO()
writeReady filename msg = do 
  -- LOG READY, <timestamp, txn_id, key, newval>
  time <- currentTimeInt
  let field_txn = txn_id msg
      field_request = request msg
      key = putkey field_request
      val = putval field_request
      sep = getSeparator
      logEntry = (List.intercalate [sep] $ ["READY", (show field_txn), (show key), (show val), (show time) ]) ++ ['\n']

  traceIO $ logEntry
  appendFile filename logEntry


writeCommit :: FilePath -> KVMessage -> IO()
writeCommit filename msg = do
  -- LOG COMMIT, <timestamp, txn_id>
  time <- currentTimeInt
  let field_txn = txn_id msg
      sep = getSeparator
      logEntry = (List.intercalate [sep] $ ["COMMIT", (show field_txn), (show time)]) ++ ['\n']

  appendFile filename logEntry

handleUnfinishedTxn :: FilePath -> FilePath -> IO([(Int,Int)])
handleUnfinishedTxn logPath storePath = do
  file <- B.readFile logPath
  let lines = reverse $ C8.split '\n' file
      txnSet = Set.empty
      unfinishedReqs = handleLines lines txnSet []
      reqTxnIdKVTuples = List.map (\line -> 
                        let pieces = C8.split ' ' line
                        in (pieces !! 1, pieces !! 2, pieces !! 3))
                        unfinishedReqs

  txnIds <- sequence $ Prelude.map (\(id, k, v) -> do
                        updateKVStore storePath k v
                        let txn_id = read (C8.unpack id) :: (Int, Int)
                        writeCommit logPath $ KVRequest txn_id (PutReq k v)

                        return txn_id



                      ) reqTxnIdKVTuples

  return txnIds


--TODO quickcheck
handleLines :: [B.ByteString] -> Set.Set B.ByteString -> [B.ByteString] -> [B.ByteString]
handleLines [] commitAcc readyAcc = readyAcc
handleLines (x:xs) commitAcc readyAcc =
  let pieces = C8.split ' ' x
      action = (pieces !! 0)
      txn_id = (pieces !! 1)
  in 
    if action == "COMMIT"
    then
      let commitAcc' = Set.insert txn_id commitAcc
      in handleLines xs commitAcc' readyAcc
    else 
      if Set.member txn_id commitAcc
      then handleLines xs commitAcc readyAcc
      else handleLines xs commitAcc (readyAcc ++ [x])


--todo, auto touch files if they are not there
persistentLogName :: Int -> String                                                   --todo, hacky
persistentLogName slaveId = "database/logs/log_kvstore_" ++ (show slaveId) ++ ".txt"

updateKVStore :: FilePath -> B.ByteString -> B.ByteString -> IO ()
updateKVStore filePath key val = do
  kvMap <- liftM Utils.readKVList $ B.readFile $ filePath
  let updatedKvMap = Map.insert key val (Map.fromList kvMap)
  traceIO $ show updatedKvMap
  B.writeFile filePath (Utils.writeKVList $ Map.toList updatedKvMap)