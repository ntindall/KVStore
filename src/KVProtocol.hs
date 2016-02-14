{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}

module KVProtocol
  (
    KVRequest(..)
  , KVResponse(..)
  , KVMessage(..)
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
                  key :: KVKey
                }
               | PutReq {
                  key :: KVKey
                , val :: KVVal
                }
  deriving (Generic, Show)

data KVResponse = KVSuccess {
                    obj :: KVObject
                  }
                | KVFailure {
                    errorMsg :: B.ByteString
                  }
  deriving (Generic, Show)

data KVMessage = KVResponse {
                  response :: KVResponse
                }
               | KVRequest {
                  request :: KVRequest
                }
  deriving (Generic, Show)

data KVObject = KBObject B.ByteString B.ByteString
  deriving (Generic, Show)

instance Serialize KVRequest
instance Serialize KVObject
instance Serialize KVMessage
instance Serialize KVResponse


decodeMsg :: B.ByteString -> Either String KVMessage
decodeMsg b = traceShow b $ CEREAL.decodeLazy b

getMessage :: Handle -> IO(Either String KVMessage)
getMessage h = do
  bytes <- C8.hGetContents h
  case (C8.null bytes) of
    True -> return $ Left "Handle is empty"
    False -> return $ decodeMsg (fromStrict bytes) 

sendMessage :: Handle -> KVMessage -> IO ()
sendMessage h req = do
  IO.putStr $ (show req) ++ ['\n']
  C8.hPutStrLn h $ toStrict (CEREAL.encodeLazy req)
