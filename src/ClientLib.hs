{-# LANGUAGE OverloadedStrings #-}

module ClientLib where

import qualified Lib


import Control.Monad
import Control.Exception

import Data.ByteString.Lazy as B
import Data.ByteString.Lazy.Char8 as C8
import Data.Char as Char
import Data.Maybe
import Data.Word as W
import Data.Map.Strict as Map

import System.Environment
import System.IO as IO
import System.IO.Unsafe as UNSAFEIO
import Network as NETWORK
import Network.Socket as SOCKET
import Network.BSD as BSD

import Control.Concurrent.MVar

-------------------------------------

--TODO, separate PROTOCOL module from TYPES module so things are more
--human readable
import qualified KVProtocol (getMessage, sendMessage, connectToMaster)
import KVProtocol hiding (getMessage, sendMessage, connectToMaster)
import qualified Utils as Utils

import Debug.Trace

type ClientId = Int

data ClientState = ClientState {
                    skt :: Socket
                  , me :: (ClientId, HostName, PortID) 
                  , outstandingTxns :: Map.Map KVTxnId (MVar KVMessage)
                  , nextTid :: Int
                 }

instance Show (ClientState) where
  show (ClientState skt me _ nextTid) = show skt ++ show me ++ show nextTid 

--main :: IO ()
--main = do
--  master <- registerWithMaster
--  getVal <- getKey master "Hello"
--  putVal <- putKey master "Hello" 3
--  return ()
--  --todo, unregisted

registerWithMaster :: Lib.Config -> IO (MVar ClientState)
registerWithMaster cfg = do
  --Allocate a socket on the client for communication with the master
  s <- Utils.getFreeSocket

  meData <- registerWithMaster_ cfg s

  mvar <- newMVar $ ClientState s meData Map.empty 1

  return mvar


--Send a registration message to the master with txn_id 0 and wait to receive
--clientId back
registerWithMaster_ :: Lib.Config -> Socket -> IO (ClientId, HostName, PortID)
registerWithMaster_ cfg s = do
  portId@(PortNumber pid) <- NETWORK.socketPort s
  hostName <- BSD.getHostName

  let txn_id = 0
  h <- KVProtocol.connectToMaster cfg
  KVProtocol.sendMessage h (KVRegistration (0, txn_id) hostName (fromEnum pid))
  IO.hClose h

  clientId <- waitForFirstAck -- wait for the Master to respond with the initial ack, and
                              -- update the config to self identify with the clientId
  return (clientId, hostName, portId)

  where waitForFirstAck = do
          (h', _, _) <- NETWORK.accept s
          response <- KVProtocol.getMessage h'

          either (\errmsg -> do
                  IO.putStr $ errmsg ++ "\n"
                  IO.hClose h'
                  waitForFirstAck
                 )
                 (\kvMsg -> do
                    case kvMsg of
                      (KVAck (clientId, txn_id) _) -> return clientId
                      _ -> do
                        IO.hClose h'
                        waitForFirstAck --todo, error handling
                 )
                 response

putKey :: MVar ClientState
       -> KVKey
       -> KVVal
       -> IO(KVVal)
putKey mvar _ _ = undefined

getKey :: MVar ClientState
       -> KVKey
       -> IO(KVVal)
getKey _ _ = undefined




--runKVClient :: Lib.Config -> IO ()
--runKVClient cfg = do
--  -- cfg' <- registerWithMaster cfg
--  -- let cfg' = cfg
--  s <- getFreeSocket --listenOn (snd clientCfg) --todo, make this dynamic
--  hostName <- BSD.getHostName
--  portId <- NETWORK.socketPort s

--  let cfg' = cfg { Lib.clientConfig = [(hostName, portId)]}

--  (a, s) <- runStateT registerWithMaster (ClientState s cfg' 0)
--  traceIO $ show (a,s)
--  evalStateT issueRequests s

--registerWithMaster :: StateT ClientState IO ()
--registerWithMaster = do
--  state <- get
--  let config = cfg state
--  let clientCfg = Prelude.head $ Lib.clientConfig config
--      hostname = fst clientCfg
--      portId = case snd clientCfg of
--                  (PortNumber n) -> fromEnum n
--                  _ -> undefined -- TODO extremely hacky
--      txn_id = next_txn_id state

--  h <- liftIO $ KVProtocol.connectToMaster config
--  liftIO $ KVProtocol.sendMessage h (KVRegistration (0, txn_id) hostname portId)

--  waitForFirstAck -- wait for the Master to respond with the initial ack, and
--                  -- update the config to self identify with the clientId

--  where waitForFirstAck = do
--          state <- get
--          let txn_id = next_txn_id state
--          (h', _, _) <- liftIO $ NETWORK.accept (skt state)
--          response <- liftIO $ KVProtocol.getMessage h'

--          either (\errmsg -> do
--                  liftIO $ IO.putStr $ errmsg ++ "\n"
--                  liftIO $ IO.hClose h'
--                  waitForFirstAck
--                 )
--                 (\kvMsg -> do
--                    case kvMsg of
--                      (KVAck (clientId, txn_id) _) -> do
--                        let config' = (cfg state) { Lib.clientNumber = Just clientId}
--                        let state' = state { cfg = config', next_txn_id = txn_id + 1 }

--                        put state'
--                      _ -> do
--                        liftIO $ IO.hClose h'
--                        waitForFirstAck --todo, error handling
--                 )
--                 response

--parseInput :: B.ByteString -> StateT ClientState IO(Maybe KVMessage)
--parseInput text = do
--  state <- get
--  let clientId = fromJust $ Lib.clientNumber (cfg state)
--      txn_id = next_txn_id state
--      pieces = C8.split ' ' text
--  if Prelude.length pieces > 1
--  then let reqType = Prelude.head pieces
--           key = pieces !! 1
--           val | Prelude.length pieces >= 3 = pieces !! 2
--               | otherwise = B.empty

--           in if reqType == "PUT" || reqType == "put"
--              then return $ Just (KVRequest (clientId, txn_id) (PutReq key val))
--              else return $ Just (KVRequest (clientId, txn_id) (GetReq key))
--  else return Nothing


--issueRequests :: StateT ClientState IO ()
--issueRequests = do
--  state <- get
--  let config = cfg state
--      txn_id = next_txn_id state
--      s = skt state

--  text <- liftIO IO.getLine
--  request <- parseInput (C8.pack text)
--  if isNothing request
--  then issueRequests
--  else do
--    liftIO (do
--      let request' = fromJust request
--      h <- KVProtocol.connectToMaster config

--      KVProtocol.sendMessage h request'

--      IO.hClose h

--      (h', hostName, portNumber) <- NETWORK.accept s
--      msg <- KVProtocol.getMessage h'

--      IO.hClose h'
--      )
--    put $ state { next_txn_id = txn_id + 1 }
--    issueRequests


---- makeRequest :: IO (KVMessage)
---- makeRequest = do
----   rstr1 <- randomString
----   rstr2 <- randomString
----   return $ KVRequest (0,1) (PutReq rstr1 rstr2)

--------------------------------------
----for now

----https://stackoverflow.com/questions/17500194/generate-a-random-string-at-compile-time-or-run-time-and-use-it-in-the-rest-of-t
---- randomString :: IO B.ByteString
---- randomString = do
----   let string = Prelude.take 10 $ randomRs ('a','z') $ UNSAFEIO.unsafePerformIO newStdGen
----   return $ C8.pack string
