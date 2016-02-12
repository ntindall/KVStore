{-# LANGUAGE OverloadedStrings #-}

module Main where

import qualified Lib as Lib

import qualified Data.Binary as Binary
import Data.ByteString.Lazy as B

import System.Environment
import System.IO as IO
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
  h <- connectTo (Lib.masterHostName cfg) (Lib.masterPortId cfg)
  issueRequests h
  IO.hClose h

issueRequests :: Handle
              -> IO()
issueRequests h = do
  kvReq <- makeRequest
  let encoding = Binary.encode kvReq

  IO.putStr $ (show kvReq)
  
  B.hPut h encoding
  issueRequests h


makeRequest :: IO (KVRequest)
makeRequest = return (Get "ExampleRequest")