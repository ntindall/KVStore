{-# LANGUAGE OverloadedStrings #-}

module Main where

import qualified Lib as Lib

import qualified Data.Binary as Binary
import Data.ByteString.Lazy as B
import Data.ByteString.Char8 as C8

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

  issueRequests cfg

issueRequests :: Lib.Config
              -> IO()
issueRequests cfg = do
  h <- connectTo (Lib.masterHostName cfg) (Lib.masterPortId cfg)
  
  kvReq <- makeRequest
  sendMessage h kvReq
  
  IO.hClose h

  issueRequests cfg


makeRequest :: IO (KVMessage)
makeRequest = return $ KVRequest (Get "ExampleRequest")