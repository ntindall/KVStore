{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.Maybe
import Data.ByteString.Lazy  as B
import Data.ByteString.Lazy.Char8 as C8

import qualified System.IO as IO

import qualified ClientLib as CL
import qualified Utils as Utils
import qualified Lib as Lib

import qualified KVProtocol (getMessage, sendMessage, connectToMaster)
import KVProtocol hiding (getMessage, sendMessage, connectToMaster)

main :: IO ()
main = Lib.parseArguments >>= \(Just config) -> do
  masterH <- CL.registerWithMaster config

  issueRequests masterH
  --todo, unregisted


issueRequests :: CL.MasterHandle -> IO ()
issueRequests mH = do

  text <- IO.getLine
  request <- parseInput (C8.pack text)
  if isNothing request
  then issueRequests mH
  else do
    let request' = fromJust request
    case request' of
      (Left k) -> do
        CL.getVal mH k
      (Right (k,v)) -> do 
        CL.putVal mH k v
    return ()

  issueRequests mH

parseInput :: B.ByteString -> IO(Maybe (Either KVKey (KVKey, KVVal)))
parseInput text = do
  let pieces = C8.split ' ' text
  if Prelude.length pieces > 1
  then let reqType = Prelude.head pieces
           key = pieces !! 1
           val | Prelude.length pieces >= 3 = pieces !! 2
               | otherwise = B.empty

           in if reqType == "PUT" || reqType == "put"
              then return $ Just (Right (key, val))
              else return $ Just (Left key)
  else return Nothing
