{-# LANGUAGE OverloadedStrings #-}

module Main where

import qualified Lib as Lib


import Control.Monad

import Data.ByteString.Lazy as B
import Data.ByteString.Lazy.Char8 as C8
import Data.Word as W

import System.Random
import System.Environment
import System.IO as IO
import System.IO.Unsafe as UNSAFEIO
import Network

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
  let clientCfg = Prelude.head $ Lib.clientConfig cfg

  s <- listenOn (snd clientCfg) --todo, make this dynamic

  issueRequests cfg s

issueRequests :: Lib.Config
              -> Socket
              -> IO()
issueRequests cfg s = do
  h <- connectTo (Lib.masterHostName cfg) (Lib.masterPortId cfg)
  
  kvReq <- makeRequest
  traceIO (show kvReq)
  sendMessage h kvReq

  (h', hostName, portNumber) <- accept s
  msg <- getMessage h'

  case msg of 
    Left errmsg -> do
      IO.putStr $ errmsg ++ ['\n']
    Right kvMsg ->
      IO.putStr $ (show kvMsg) ++ ['\n']
  
  IO.hClose h'

  issueRequests cfg s


makeRequest :: IO (KVMessage)
makeRequest = do
  rstr1 <- randomString
  rstr2 <- randomString
  return $ KVRequest 0 (PutReq rstr1 rstr2)

------------------------------------
--for now

--https://stackoverflow.com/questions/17500194/generate-a-random-string-at-compile-time-or-run-time-and-use-it-in-the-rest-of-t
randomString :: IO B.ByteString
randomString = do
  let string = Prelude.take 10 $ randomRs ('a','z') $ UNSAFEIO.unsafePerformIO newStdGen
  return $ C8.pack string
