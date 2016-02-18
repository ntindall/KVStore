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

data KVMessage = KVResponse {
                  txn_id   :: Int
                , slave_id :: Int
                , response :: KVResponse
                }
               | KVRequest {  -- PREPARE
                  txn_id   :: Int
                , request :: KVRequest
                }
               | KVDecision { -- COMMIT or ABORT, sent by master
                  txn_id   :: Int                 
                , decision :: KVDecision
                , request  :: KVRequest
                }
               | KVAck {
                  txn_id   :: Int --final message, sent by slave
                , ack_slave_id :: Maybe Int
               }
               | KVVote {
                  txn_id   :: Int -- READY or ABORT, sent by slave
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
