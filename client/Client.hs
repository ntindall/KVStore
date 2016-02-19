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

-------------------------------------

import KVProtocol

import Debug.Trace

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

  cfg'' <- registerWithMaster cfg'

  (h', hostName, portNumber) <- NETWORK.accept s
  response <- getMessage h'

  either (\errmsg -> do
            IO.putStr $ errmsg ++ ['\n']
         )
         (\kvMsg -> do
            IO.putStr $ (show kvMsg) ++ ['\n']
            case kvMsg of
              (KVAck (clientId, txn_id) _) -> do
                let cfg' = cfg { Lib.clientNumber = Just clientId,
                                 Lib.clientConfig = undefined
                               }
                issueRequests cfg' s 0
              _ -> undefined --todo, error handling
         )
         response


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

registerWithMaster :: Lib.Config -> IO()
registerWithMaster cfg = do
  let clientCfg = Prelude.head $ Lib.clientConfig cfg
      hostname = fst clientCfg
      portId = case (snd clientCfg) of
                  (PortNumber n) -> fromEnum n
                  _ -> undefined

  h <- connectTo (Lib.masterHostName cfg) (Lib.masterPortId cfg)
  sendMessage h (KVRegistration (0,0) hostname portId) --todo check flags

parseInput :: B.ByteString -> Int -> Int -> IO(Maybe KVMessage)
parseInput text clientId txn_id = do
  let pieces = C8.split ' ' text
  traceIO $ show pieces 
  if (Prelude.length pieces > 1)
  then let reqType = pieces !! 0
           key = pieces !! 1
           val | Prelude.length pieces >= 3 = pieces !! 2
               | otherwise = B.empty


           in if (reqType == "PUT" || reqType == "put")
              then return $ Just (KVRequest (clientId, txn_id) (PutReq key val))
              else return $ Just (KVRequest (clientId, txn_id) (GetReq key))
  else return Nothing


issueRequests :: Lib.Config
              -> Socket
              -> Int
              -> IO()
issueRequests cfg s txn_id = do

  text <- IO.getLine
  request <- parseInput (C8.pack text) (fromJust $ Lib.clientNumber cfg) txn_id
  if isNothing request
  then do
    issueRequests cfg s txn_id
  else do
    let request' = fromJust request
    h <- NETWORK.connectTo (Lib.masterHostName cfg) (Lib.masterPortId cfg)
    
   -- kvReq <- makeRequest
    traceIO (show request')
    sendMessage h request'

    IO.hClose h

    (h', hostName, portNumber) <- NETWORK.accept s
    msg <- getMessage h'

    case msg of 
      Left errmsg -> do
        IO.putStr $ errmsg ++ ['\n']
      Right kvMsg ->
        IO.putStr $ (show kvMsg) ++ ['\n']
    
    IO.hClose h'

    issueRequests cfg s (txn_id + 1)


-- makeRequest :: IO (KVMessage)
-- makeRequest = do
--   rstr1 <- randomString
--   rstr2 <- randomString
--   return $ KVRequest (0,1) (PutReq rstr1 rstr2)

------------------------------------
--for now

--https://stackoverflow.com/questions/17500194/generate-a-random-string-at-compile-time-or-run-time-and-use-it-in-the-rest-of-t
-- randomString :: IO B.ByteString
-- randomString = do
--   let string = Prelude.take 10 $ randomRs ('a','z') $ UNSAFEIO.unsafePerformIO newStdGen
--   return $ C8.pack string
