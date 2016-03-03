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
  , kV_TIMEOUT
  , getMessage
  , sendMessage
  , connectToMaster
  ) where

import Data.Serialize as CEREAL
import Data.ByteString.Lazy  as B
import Data.ByteString.Char8 as C8
import Debug.Trace

import GHC.Generics (Generic)

import Network
import System.IO as IO
import Rainbow as Rainbow

import qualified Lib

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

kV_TIMEOUT :: Int
kV_TIMEOUT = 1

decodeMsg :: B.ByteString -> Either String KVMessage
decodeMsg = CEREAL.decodeLazy

getMessage :: Handle -> IO(Either String KVMessage)
getMessage h = do
  bytes <- C8.hGetContents h
  if C8.null bytes
  then return $ Left "Handle is empty"
  else do
    let msg = decodeMsg (fromStrict bytes)

        color = either (\e -> brightRed)
                       (\m -> prettyPrint m)
                       msg


    Rainbow.putChunkLn $ chunk ("[!] Received: " ++ show msg) & fore color
    return $ decodeMsg (fromStrict bytes)

sendMessage :: Handle -> KVMessage -> IO ()
sendMessage h msg = do
 -- let color = prettyPrint msg
  C8.hPutStrLn h $ toStrict (CEREAL.encodeLazy msg)
  Rainbow.putChunkLn $ chunk ("[!] Sending: " ++ show msg) & fore brightYellow

connectToMaster :: Lib.Config -> IO Handle
connectToMaster cfg = connectTo (Lib.masterHostName cfg) (Lib.masterPortId cfg)

prettyPrint :: KVMessage -> Radiant
prettyPrint m = case m of
                  KVAck{}  -> brightGreen
                  KVVote{} -> brightMagenta
                  _        -> brightCyan

