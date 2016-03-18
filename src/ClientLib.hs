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
import Data.List as List

import System.IO as IO

import Network as NETWORK
import Network.Socket as SOCKET hiding (listen)
import Network.BSD as BSD

import Control.Concurrent
import Control.Concurrent.Thread.Delay (delay)
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
                    receiver :: Handle
                  , sender :: MVar Handle
                  , cfg :: Lib.Config
                  , me :: (ClientId, HostName, PortID) 
                  , outstandingTxns :: Map.Map KVTxnId (MVar KVMessage)
                  , issuedTimes :: Map.Map KVTxnId KVTime
                  , nextTid :: Int
                 }

instance Show (ClientState) where
  show (ClientState skt _ cfg me _ issued nextTid) = show skt ++ show cfg ++ show me ++ show issued ++ show nextTid 


establishAccept :: Socket -> IO (Socket, SockAddr)
establishAccept skt = catch (SOCKET.accept skt) 
                            (\(e:: SomeException) -> do
                              threadDelay 1000000
                              establishAccept skt
                            )

listen :: MasterHandle -> IO()
listen mvar = do
  state <- readMVar mvar

  response <- KVProtocol.getMessage (receiver state)

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


timeoutThread :: MasterHandle -> IO ()
timeoutThread mvar = do
  state <- takeMVar mvar
  now <- Utils.currentTimeMicro

  timedOut <- mapM  (\(tid, time) ->
                      if (now - (KVProtocol.kV_TIMEOUT_MICRO) >= time) --give master the benefit of the doubt...
                      then return $ Just tid -- has timed out
                      else return $ Nothing
                    ) $ Map.toList $ (issuedTimes state)

  mapM (\tid -> IO.putStr $ "[!][!] Timing out" ++ show tid) (catMaybes timedOut)

  --put a dummy message into the tids mvars, causing their waiting threads to wake up
  dummyMessage (outstandingTxns state) (catMaybes timedOut)
  putMVar mvar $ state { issuedTimes     = deleteList (issuedTimes state) (catMaybes timedOut) }


  delay (KVProtocol.kV_TIMEOUT_MICRO)

  timeoutThread mvar

  where deleteList map (x:xs) = deleteList (Map.delete x map) xs
        deleteList map [] = map

        dummyMessage map (x:xs) = do
          let mvar = fromJust $ Map.lookup x map
          putMVar mvar $ KVAck x Nothing (Just $ C8.pack "Transaction timed out from client")
          dummyMessage map xs
        dummyMessage map [] = return ()



registerWithMaster :: Lib.Config -> IO (MasterHandle)
registerWithMaster cfg = do
  --Allocate a socket on the client for communication with the master
  listener <- KVProtocol.listenOnPort aNY_PORT

  sender <- KVProtocol.connectToHost (Lib.masterHostName cfg) (Lib.masterPortId cfg)
  senderMVar <- newMVar sender

  (meData, receiver) <- registerWithMaster_ cfg listener senderMVar
  handleMVar <- newMVar $ ClientState receiver senderMVar cfg meData Map.empty Map.empty 1 --transaction numbers start at 11 to avoid CEREAL bug
                                                                                  --difficulty deserializing data when txn_id = (x,10)
  forkIO $ listen handleMVar
  forkIO $ timeoutThread handleMVar
  return handleMVar

--Send a registration message to the master with txn_id 0 and wait to receive
--clientId back
registerWithMaster_ :: Lib.Config -> Socket -> MVar Handle -> IO ((ClientId, HostName, PortID), Handle)
registerWithMaster_ cfg listener senderMVar = do
  portId@(PortNumber pid) <- NETWORK.socketPort listener
  hostName <- BSD.getHostName
  let txn_id = 0

  KVProtocol.sendMessage senderMVar (KVRegistration (0, txn_id) hostName (fromEnum pid))

  (clientId, h) <- waitForFirstAck -- wait for the Master to respond with the initial ack, and
                              -- update the config to self identify with the clientId
  return ((clientId, hostName, portId), h)

  where waitForFirstAck = do
          (conn, _) <- establishAccept listener

          h <- SOCKET.socketToHandle conn ReadMode
          response <- KVProtocol.getMessage h

          either (\errmsg -> do
                  IO.putStr $ errmsg ++ "\n"
                  waitForFirstAck
                 )
                 (\kvMsg -> do
                    case kvMsg of
                      (KVAck (clientId, txn_id) _ _ ) -> return (clientId, h)
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
      issuedTimes' = Map.insert tid (issuedUTC req) (issuedTimes state) 

  putMVar mvar $ state { nextTid = tNo + 1
                       , outstandingTxns = outstandingTxns'
                       , issuedTimes = issuedTimes'
                       }

  state' <- readMVar mvar

  catch (KVProtocol.sendMessage (sender state') request) 
        ((\(e :: SomeException) -> IO.putStr (show e)))

  response <- takeMVar myMvar

  state'' <- takeMVar mvar
  let outstandingTxns'' = Map.delete tid (outstandingTxns state'')
  putMVar mvar $ state'' { outstandingTxns = Map.delete tid (outstandingTxns state'')
                         , issuedTimes = Map.delete tid (issuedTimes state'')
                         }

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
