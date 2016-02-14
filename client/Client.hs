{-# LANGUAGE OverloadedStrings #-}

module Main where

import qualified Lib as Lib


import Control.Monad

import Data.ByteString.Lazy as B
import Data.ByteString.Char8 as C8
import Data.Word as W

import System.Random
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
makeRequest = do
  rstr1 <- randomString
  rstr2 <- randomString
  return $ KVRequest (PutReq rstr1 rstr2)

------------------------------------
--for now
--https://stackoverflow.com/questions/20889729/how-to-properly-generate-a-random-bytestring-in-haskell
randomBytes :: Int -> StdGen -> [W.Word8]
randomBytes 0 _ = []
randomBytes count g = fromIntegral value:randomBytes (count - 1) nextG
                      where (value, nextG) = next g

randomByteString :: Int -> StdGen -> B.ByteString
randomByteString count g = B.pack $ randomBytes count g


randomString :: IO B.ByteString
randomString = do
  g <- getStdGen
  let bytestring = randomByteString 12 g
  return bytestring
