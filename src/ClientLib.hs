{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module ClientLib
    ( registerWithMaster
    , putVal
    , getVal
    , delVal
    , MasterHandle
    ) where

import qualified Lib


import Control.Monad
import Control.Exception

import Data.ByteString.Lazy as B
import Data.ByteString.Lazy.Char8 as C8
import Data.Maybe
import Data.Map.Strict as Map

import System.IO as IO

import Network as NETWORK
import Network.Socket as SOCKET hiding (listen)
import Network.BSD as BSD

import Control.Concurrent
import Control.Concurrent.MVar

-------------------------------------

--TODO, separate PROTOCOL module from TYPES module so things are more
--human readable
import qualified KVProtocol (getMessage, sendMessage, connectToHost)
import KVProtocol hiding (getMessage, sendMessage, connectToHost)
import qualified Utils as Utils

import Debug.Trace

type ClientId = Int

type MasterHandle = MVar ClientState

data ClientState = ClientState {
                    receiver :: Socket
                  , sender :: MVar Socket
                  , cfg :: Lib.Config
                  , me :: (ClientId, HostName, PortID) 
                  , outstandingTxns :: Map.Map KVTxnId (MVar KVMessage)
                  , nextTid :: Int
                 }

instance Show (ClientState) where
  show (ClientState skt _ cfg me _ nextTid) = show skt ++ show cfg ++ show me ++ show nextTid 


establishListen :: Socket -> IO (Socket, SockAddr)
establishListen skt = catch (SOCKET.accept skt) 
                            (\(e:: SomeException) -> do
                              threadDelay 1000000
                              establishListen skt
                            )

listen :: MasterHandle -> IO()
listen mvar = do
  state <- readMVar mvar

  (conn, _) <- establishListen (receiver state)

  response <- KVProtocol.getMessage conn

  either (\errmsg -> do
          IO.putStr $ errmsg ++ "\n"
          listen mvar
         )
         (\kvMsg -> do
            let tid = txn_id kvMsg
            state' <- readMVar mvar
            let txnMVar = Map.lookup tid (outstandingTxns state')

            --means that the message sent from the master was a duplicate (should not happen)
            --could happen if code in master is irregular (should fix)
            if isNothing txnMVar then listen mvar
            --write into the mvar, waking the thread waiting to take the response!
            else putMVar (fromJust txnMVar) kvMsg >>= \_ -> listen mvar
          )
         response
  listen mvar

registerWithMaster :: Lib.Config -> IO (MasterHandle)
registerWithMaster cfg = do
  --Allocate a socket on the client for communication with the master
  receiver <- KVProtocol.listenOnPort aNY_PORT

  sender <- KVProtocol.connectToHost (Lib.masterHostName cfg) (Lib.masterPortId cfg)
  senderMVar <- newMVar sender

  meData <- registerWithMaster_ cfg receiver senderMVar
  handleMVar <- newMVar $ ClientState receiver senderMVar cfg meData Map.empty 1
  forkIO $ listen handleMVar
  return handleMVar

--Send a registration message to the master with txn_id 0 and wait to receive
--clientId back
registerWithMaster_ :: Lib.Config -> Socket -> MVar Socket -> IO (ClientId, HostName, PortID)
registerWithMaster_ cfg receiver senderMVar = do
  portId@(PortNumber pid) <- NETWORK.socketPort receiver
  hostName <- BSD.getHostName
  let txn_id = 0

  KVProtocol.sendMessage senderMVar (KVRegistration (0, txn_id) hostName (fromEnum pid))

  clientId <- waitForFirstAck -- wait for the Master to respond with the initial ack, and
                              -- update the config to self identify with the clientId
  return (clientId, hostName, portId)

  where waitForFirstAck = do
          (conn, _) <- establishListen receiver
          response <- KVProtocol.getMessage conn

          either (\errmsg -> do
                  IO.putStr $ errmsg ++ "\n"
                  waitForFirstAck
                 )
                 (\kvMsg -> do
                    case kvMsg of
                      (KVAck (clientId, txn_id) _ _ ) -> return clientId
                      _ -> do
                        waitForFirstAck --todo, error handling
                 )
                 response

sendRequestAndWaitForResponse :: MVar ClientState -> KVRequest -> IO(Maybe KVVal)
sendRequestAndWaitForResponse mvar req = do
  myMvar <- newEmptyMVar
  state <- takeMVar mvar
  let tNo = nextTid state
      (clientId,_, _) = me state
      config = cfg state
      tid = (clientId, tNo) 

      request = KVRequest tid req

  let outstandingTxns' = Map.insert tid myMvar (outstandingTxns state)

  putMVar mvar $ state { nextTid = tNo + 1
                       , outstandingTxns = outstandingTxns'}

  state' <- readMVar mvar
  KVProtocol.sendMessage (sender state') request

  response <- takeMVar myMvar

  state'' <- takeMVar mvar
  let outstandingTxns'' = Map.delete tid (outstandingTxns state'')
  putMVar mvar $ state'' { outstandingTxns = outstandingTxns''}

  return $ Just B.empty

--todo, modify these functions to re-sendtherequestandwaitforresponse if an
-- error occured.

putVal :: MVar ClientState
       -> KVKey
       -> KVVal
       -> IO()
putVal mvar k v = do
  now <- Utils.currentTimeMicro
  sendRequestAndWaitForResponse mvar (PutReq now k v) >>= (\_ -> return ())


getVal :: MVar ClientState
       -> KVKey
       -> IO(KVVal)
getVal mvar k = do
  now <- Utils.currentTimeMicro
  sendRequestAndWaitForResponse mvar (GetReq now k) >>= (\(Just x) -> return x)

delVal :: MVar ClientState
       -> KVKey
       -> IO ()
delVal mvar k = do
  now <- Utils.currentTimeMicro
  sendRequestAndWaitForResponse mvar (DelReq now k) >>= (\_ -> return ())
