 {-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Data.ByteString.Lazy as B
import Data.ByteString.Lazy.Char8 as C8

-- import Test.Hspec
-- import Test.QuickCheck hiding (Result)
-- import Test.QuickCheck.Property (Result, rejected, liftBool)

import System.IO as IO

import Network as NETWORK
import Network.Socket as SOCKET hiding (listen)
import Network.BSD as BSD

import Control.Concurrent
import Control.Concurrent.MVar

import System.Process

import ClientLib
import Lib
import KVProtocol

import Control.Monad.State
import Control.Monad.Trans (liftIO)

import Data.Maybe

import Debug.Trace

import Control.Concurrent.Thread.Delay

data TestState = TestState {
  masterKVHandle :: MasterHandle,
  masterHandle :: ProcessHandle,
  slaveHandles :: [ProcessHandle],
  numCompleted :: MVar Int
}

main :: IO ()
main = do
  state <- setUpTopology
  runStateT runTests state
  return ()

runTests :: StateT TestState IO ()
runTests = do
  testN 200
  killNodes

setUpTopology :: IO TestState
setUpTopology = do
  m <- spawnCommand "stack exec kvstore-master -- -l -n 1"
  delay 100000
  sl <- spawnCommand "stack exec kvstore-worker -- -l -n 1 -i 0"
  delay 100000
  sl2 <- spawnCommand "stack exec kvstore-worker -- -l -n 1 -i 0"
  delay 100000
  mh <- registerWithMaster testConfig
  mvar <- newMVar 0
  return $ TestState mh m [sl, sl2] mvar

testConfig :: Lib.Config
testConfig = do
  let hn = "127.0.0.1"
  let pno = PortNumber 1063
  let clientCfg = []
  let slaveCfg = [("127.0.0.1", PortNumber 1064), ("127.0.0.1", PortNumber 1065)]
  Lib.Config hn pno clientCfg slaveCfg Nothing Nothing

killNodes :: StateT TestState IO ()
killNodes = get >>= \s -> liftIO $ do
  terminateProcess $ masterHandle s
  sequence $ Prelude.map terminateProcess $ slaveHandles s
  return ()

testN :: Int -> StateT TestState IO ()
testN n = do
  testNSpawn n
  testNWait n

testNWait :: Int -> StateT TestState IO ()
testNWait n = get >>= \s -> do
  completed <- liftIO $ readMVar $ numCompleted s
  if completed == n then return ()
  else do
    liftIO $ delay 10000
    testNWait n

testNSpawn :: Int -> StateT TestState IO ()
testNSpawn 0 = return ()
testNSpawn n = get >>= \s -> do
  let k = C8.pack $ "key" ++ show n
      v = C8.pack $ "val" ++ show n
  _ <- liftIO $ forkIO $ do
    putVal (masterKVHandle s) k v
    i <- takeMVar (numCompleted s)
    putMVar (numCompleted s) (i + 1)
  testNSpawn (n-1)
