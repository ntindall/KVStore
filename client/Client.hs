{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Concurrent

import Data.Maybe
import Data.ByteString.Lazy  as B
import Data.ByteString.Lazy.Char8 as C8
import Data.Serialize

import qualified System.IO as IO

import qualified ClientLib as CL
import qualified Utils as Utils
import qualified Lib as Lib

import qualified KVProtocol (getMessage, sendMessage, connectToMaster)
import KVProtocol hiding (getMessage, sendMessage, connectToMaster)

import Math.Probable

import Debug.Trace

main :: IO ()
main = Lib.parseArguments >>= \(Just config) -> do
  masterH <- CL.registerWithMaster config

  cLIissueRequests masterH

  --children <- issueNRequests masterH 1000 []

  --mapM_ takeMVar children
  --todo, unregisted

--issueNRequests :: CL.MasterHandle -> Int -> [MVar ()] -> IO ([MVar ()])
--issueNRequests mH n mvars
--  | n == 0 = return mvars
--  | otherwise = do
--    let request = createRequest n
--    m <- newEmptyMVar 
--    tid <- forkFinally (case request of
--                      (Left k) -> do
--                        CL.getVal mH k
--                        return ()
--                      (Right (k,v)) -> do 
--                        CL.putVal mH k v
--                        return ()
--                  ) (\_ -> putMVar m ())

--    issueNRequests mH (n - 1) (mvars ++ [m])

--createRequest n = let nBstring = C8.pack $ show n 
--                  in Right (nBstring, nBstring)

cLIissueRequests :: CL.MasterHandle -> IO ()
cLIissueRequests mH = do

  text <- IO.getLine
  request <- parseInput (C8.pack text)
  if isNothing request
  then cLIissueRequests mH
  else do
    let request' = fromJust request
    case request' of
      (PutReq _ k v) -> do
        CL.putVal mH k v >>= (\_ -> return ())
      (DelReq _ k) -> do 
        CL.delVal mH k
      (GetReq _ k) -> do
        CL.getVal mH k >>= (\_ -> return ())
    return ()

  cLIissueRequests mH

parseInput :: B.ByteString -> IO(Maybe KVRequest)
parseInput text = do

  let pieces = C8.split ' ' text
  if Prelude.length pieces > 1
  then let reqType = Prelude.head pieces
           key = pieces !! 1
           val | Prelude.length pieces >= 3 = pieces !! 2
               | otherwise = B.empty

          in return $ case reqType of
                        "PUT" -> Just $ PutReq 0 key val
                        "GET" -> Just $ GetReq 0 key
                        "DEL" -> Just $ DelReq 0 key   
                        _     -> Nothing        
  else return Nothing
