{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}

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
  , KVTime
  , kV_TIMEOUT_MICRO
  , getMessage
  , sendMessage
  , connectToHost
  , listenOnPort
  ) where

import Data.Serialize as CEREAL
import Data.ByteString.Lazy  as B
import Data.ByteString.Char8 as C8
import Debug.Trace
import Control.Exception as E
import Control.Concurrent
import Control.Monad
import Network as NETWORK
import Network.Socket as SOCKET
import Network.Socket.ByteString as SOCKETBSTRING
import Network.BSD as BSD

import GHC.Generics (Generic)

import Data.Time.Clock

import Network
import System.IO as IO
import Rainbow as Rainbow

import qualified Lib

type KVKey = B.ByteString
type KVVal = B.ByteString
type KVTxnId = (Int, Int) -- (client_id, txn_id)
type KVTime = Integer

-- TODO, with more clients, need txn_id to be (txn_id, client_id) tuples

data KVRequest = GetReq {
                  issuedUTC :: KVTime
                , reqkey :: KVKey
                }
               | PutReq {
                  issuedUTC :: KVTime
                , putkey :: KVKey
                , putval :: KVVal
                }
               | DelReq {
                  issuedUTC :: KVTime
                , delkey :: KVKey
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
  deriving (Generic, Show, Eq)

data KVVote = VoteReady | VoteAbort
  deriving (Generic, Show, Eq)

data KVMessage = KVRegistration {
                  txn_id :: KVTxnId
                , hostname :: HostName
                , portId   :: Int
                }
                | KVResponse {
                  txn_id   :: KVTxnId
                , worker_id :: Int
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
                  txn_id   :: KVTxnId --final message, sent by worker
                , ack_id   :: Maybe Int --either the workerId (if sent FROM worker), or Nothing
                , success  :: Maybe B.ByteString
               }
               | KVVote {
                  txn_id   :: KVTxnId -- READY or ABORT, sent by worker
                , worker_id :: Int
                , vote     :: KVVote
                , request  :: KVRequest
               }        
  deriving (Generic, Show)

instance Serialize KVRequest
instance Serialize KVMessage
instance Serialize KVResponse
instance Serialize KVDecision
instance Serialize KVVote

--MICROSECONDS
kV_TIMEOUT_MICRO :: KVTime
kV_TIMEOUT_MICRO = 1000000

decodeMsg :: B.ByteString -> Either String KVMessage
decodeMsg = CEREAL.decodeLazy

getMessage :: Handle -> IO(Either String KVMessage)
getMessage h = do
  bytes <- C8.hGetLine h
  if C8.null bytes
  then return $ Left "Handle is empty"
  else do
    let msg = decodeMsg (fromStrict bytes)

        color = either (\e -> brightRed)
                       (\m -> prettyPrint m)
                       msg

    --Note: serialization issue caused by CEREAL was fixed on the `bench'
    --branch but not merged to master.
    Rainbow.putChunkLn $ chunk ("[!] Received: " ++ show msg) & fore color
    return $ decodeMsg (fromStrict bytes)

--socket must already be connected
sendMessage :: MVar Handle -> KVMessage -> IO ()
sendMessage hMvar msg = do
  withMVar hMvar (\h -> do
    C8.hPutStrLn h $ CEREAL.encode msg
    Rainbow.putChunkLn $ chunk ("[!] Sending: " ++ show msg) & fore brightYellow)

--creates a WRITE socket for               
connectToHost :: HostName -> PortID -> IO Handle
connectToHost hostname pid@(PortNumber pno) = 
  E.catch (do
    hostEntry <- BSD.getHostByName hostname

    sock <- SOCKET.socket AF_INET Stream defaultProtocol

    SOCKET.setSocketOption sock KeepAlive 1
    SOCKET.connect sock (SockAddrInet pno (hostAddress hostEntry))

    --convert to handle
    socketToHandle sock ReadWriteMode
  )
  (\(e :: SomeException) -> do
    traceIO $ show e
    threadDelay 1000000

    connectToHost hostname pid
  )

listenOnPort :: PortNumber -> IO NETWORK.Socket
listenOnPort port = do
  catch (do
    proto <- BSD.getProtocolNumber "tcp"
    E.bracketOnError
      (SOCKET.socket AF_INET Stream defaultProtocol)
      SOCKET.sClose
      (\sock -> do
          SOCKET.setSocketOption sock ReuseAddr 1
          SOCKET.setSocketOption sock ReusePort 1
          SOCKET.setSocketOption sock KeepAlive 1
          SOCKET.bindSocket sock (SockAddrInet port iNADDR_ANY)
          SOCKET.listen sock maxListenQueue
          return sock
      )
    )
    (\(e :: SomeException) -> do
      traceIO $ show e
      threadDelay 1000000

      listenOnPort port
    )

prettyPrint :: KVMessage -> Radiant
prettyPrint m = case m of
                  KVAck{}  -> brightGreen
                  KVVote{} -> brightMagenta
                  _        -> brightCyan

