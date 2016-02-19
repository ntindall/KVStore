{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}

module KVProtocol
  (
    KVRequest(..)
  , KVResponse(..)
  , KVMessage(..)
  , KVVote(..)
  , KVDecision(..)
  , KVKey
  , KVVal
  , KVTxnId
  , getMessage
  , sendMessage
  ) where

import Data.Serialize as CEREAL
import Data.ByteString.Lazy  as B
import Data.ByteString.Char8 as C8
import Debug.Trace

import GHC.Generics (Generic)

import Network
import System.IO as IO

type KVKey = B.ByteString
type KVVal = B.ByteString
type KVTxnId = (Int, Int) -- (client_id, txn_id)

-- TODO, with more clients, need txn_id to be (txn_id, client_id) tuples

data KVRequest = GetReq {
                  reqkey :: KVKey
                }
               | PutReq {
                  putkey :: KVKey
                , putval :: KVVal
                }
  deriving (Generic, Show)

data KVResponse = KVSuccess {
                    key :: KVKey
                  , val :: Maybe KVVal --Nothing if not found (missing) 
                  }
                | KVFailure {
                    errorMsg :: B.ByteString
                  }
  deriving (Generic, Show)

data KVDecision = DecisionCommit | DecisionAbort
  deriving (Generic, Show)

data KVVote = VoteReady | VoteAbort
  deriving (Generic, Show)

data KVMessage = KVRegistration {
                  txn_id :: KVTxnId
                , hostname :: HostName
                , portId   :: Int
                }
                | KVResponse {
                  txn_id   :: KVTxnId
                , slave_id :: Int
                , response :: KVResponse
                }
               | KVRequest {  -- PREPARE
                  txn_id   :: KVTxnId
                , request :: KVRequest
                }
               | KVDecision { -- COMMIT or ABORT, sent by master
                  txn_id   :: KVTxnId               
                , decision :: KVDecision
                , request  :: KVRequest
                }
               | KVAck {
                  txn_id   :: KVTxnId --final message, sent by slave
                , ack_id :: Maybe Int --either the slaveId (if sent FROM slave), or Nothing
               }
               | KVVote {
                  txn_id   :: KVTxnId -- READY or ABORT, sent by slave
                , slave_id :: Int
                , vote     :: KVVote
                , request  :: KVRequest
               }        
  deriving (Generic, Show)

data KVObject = KBObject B.ByteString B.ByteString
  deriving (Generic, Show)

instance Serialize KVRequest
instance Serialize KVObject
instance Serialize KVMessage
instance Serialize KVResponse
instance Serialize KVDecision
instance Serialize KVVote


decodeMsg :: B.ByteString -> Either String KVMessage
decodeMsg b = CEREAL.decodeLazy b

getMessage :: Handle -> IO(Either String KVMessage)
getMessage h = do
  bytes <- C8.hGetContents h
  case (C8.null bytes) of
    True -> return $ Left "Handle is empty"
    False -> return $ decodeMsg (fromStrict bytes) 

sendMessage :: Handle -> KVMessage -> IO ()
sendMessage h msg = do
  traceIO $ "sending " ++ show msg
  C8.hPutStrLn h $ toStrict (CEREAL.encodeLazy msg)
