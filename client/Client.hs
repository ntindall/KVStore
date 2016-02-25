{-# LANGUAGE OverloadedStrings #-}

module Main where

import qualified Lib as Lib


import Control.Monad
import Control.Exception

import Data.ByteString.Lazy as B
import Data.ByteString.Lazy.Char8 as C8
import Data.Char as Char
import Data.Maybe
import Data.Word as W

import System.Random
import System.Environment
import System.IO as IO
import System.IO.Unsafe as UNSAFEIO
import Network as NETWORK
import Network.Socket as SOCKET
import Network.BSD as BSD

import Control.Monad.State

-------------------------------------

import KVProtocol

import Debug.Trace

data ClientState = ClientState {
                    skt :: Socket
                  , cfg :: Lib.Config
                  , next_txn_id :: Int
                 }
  deriving (Show)

main :: IO ()
main = do
  success <- Lib.parseArguments
  case success of
    Nothing     -> Lib.printUsage --an error occured
    Just config -> runKVClient config

runKVClient :: Lib.Config -> IO ()
runKVClient cfg = do
  -- cfg' <- registerWithMaster cfg
  -- let cfg' = cfg
  s <- getFreeSocket --listenOn (snd clientCfg) --todo, make this dynamic
  hostName <- BSD.getHostName
  portId <- NETWORK.socketPort s

  let cfg' = cfg { Lib.clientConfig = [(hostName, portId)]}

  (a, s) <- runStateT registerWithMaster (ClientState s cfg' 0)
  traceIO $ (show (a,s))
  evalStateT issueRequests s

getFreeSocket :: IO(Socket)
getFreeSocket = do
  proto <- BSD.getProtocolNumber "tcp"
  bracketOnError
    (SOCKET.socket AF_INET Stream proto)
    (SOCKET.sClose)
    (\sock -> do
        let port = aNY_PORT
        SOCKET.setSocketOption sock ReuseAddr 1
        SOCKET.bindSocket sock (SockAddrInet port iNADDR_ANY)
        SOCKET.listen sock maxListenQueue
        return sock
    )

registerWithMaster :: StateT ClientState IO ()
registerWithMaster = do
  state <- get
  let config = cfg state
  let clientCfg = Prelude.head $ Lib.clientConfig config
      hostname = fst clientCfg
      portId = case (snd clientCfg) of
                  (PortNumber n) -> fromEnum n
                  _ -> undefined -- TODO extremely hacky
      txn_id = next_txn_id state

  h <- liftIO $ connectTo (Lib.masterHostName config) (Lib.masterPortId config)
  liftIO $ sendMessage h (KVRegistration (0, txn_id) hostname portId)

  waitForFirstAck -- wait for the Master to respond with the initial ack, and
                  -- update the config to self identify with the clientId

  where waitForFirstAck = do
          state <- get
          let txn_id = next_txn_id state
          (h', _, _) <- liftIO $ NETWORK.accept (skt state)
          response <- liftIO $ getMessage h'

          either (\errmsg -> do
                  liftIO $ IO.putStr $ errmsg ++ ['\n']
                  liftIO $ IO.hClose h'
                  waitForFirstAck
                 )
                 (\kvMsg -> do
                    liftIO $ IO.putStr $ (show kvMsg) ++ ['\n']
                    case kvMsg of
                      (KVAck (clientId, txn_id) _) -> do
                        let config' = (cfg state) { Lib.clientNumber = Just clientId}
                        let state' = state { cfg = config', next_txn_id = txn_id + 1 }

                        put state'
                      _ -> do
                        liftIO $ IO.hClose h'
                        waitForFirstAck --todo, error handling
                 )
                 response

parseInput :: B.ByteString -> StateT ClientState IO(Maybe KVMessage)
parseInput text = do
  state <- get
  let clientId = (fromJust $ Lib.clientNumber (cfg state))
      txn_id = next_txn_id state
      pieces = C8.split ' ' text
  if (Prelude.length pieces > 1)
  then let reqType = pieces !! 0
           key = pieces !! 1
           val | Prelude.length pieces >= 3 = pieces !! 2
               | otherwise = B.empty
               
           in if (reqType == "PUT" || reqType == "put")
              then return $ Just (KVRequest (clientId, txn_id) (PutReq key val))
              else return $ Just (KVRequest (clientId, txn_id) (GetReq key))
  else return Nothing


issueRequests :: StateT ClientState IO ()
issueRequests = do
  state <- get
  let config = cfg state
      txn_id = next_txn_id state
      s = skt state

  text <- liftIO $ IO.getLine
  request <- parseInput (C8.pack text)
  if isNothing request
  then do
    issueRequests
  else do
    liftIO $ (do
      let request' = fromJust request
      h <- NETWORK.connectTo (Lib.masterHostName config) (Lib.masterPortId config)
      
     -- kvReq <- makeRequest
      traceIO $ (show request')
      sendMessage h request'

      IO.hClose h

      (h', hostName, portNumber) <- NETWORK.accept s
      msg <- getMessage h'

      either (\errmsg -> IO.putStr $ errmsg ++ ['\n'])
             (\kvMsg -> IO.putStr $ (show kvMsg) ++ ['\n'])
             msg
      
      IO.hClose h'
      )
    put $ state { next_txn_id = txn_id + 1 }
    issueRequests


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
