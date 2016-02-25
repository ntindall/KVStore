module Log where

import qualified Data.ByteString.Lazy as B

import KVProtocol
import System.IO

import Data.Time
import Control.Applicative
import Data.Time.Clock.POSIX
import Data.List as List

import Debug.Trace

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
      logEntry = (List.intercalate [sep] $ [(show time), "READY", (show field_txn), (show key), (show val)]) ++ ['\n']

  traceIO $ logEntry
  appendFile filename logEntry


writeCommit :: FilePath -> KVMessage -> IO()
writeCommit filename msg = do
  -- LOG COMMIT, <timestamp, txn_id>
  time <- currentTimeInt
  let field_txn = txn_id msg
      sep = getSeparator
      logEntry = (List.intercalate [sep] $ [(show time), "COMMIT", (show field_txn)]) ++ ['\n']

  appendFile filename logEntry


--todo, auto touch files if they are not there
persistentLogName :: Int -> String                                                   --todo, hacky
persistentLogName slaveId = "database/logs/log_kvstore_" ++ (show slaveId) ++ ".txt"