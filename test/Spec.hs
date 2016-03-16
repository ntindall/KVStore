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

import Control.Concurrent.Thread.Delay

data TestState = TestState {
  masterKVHandle :: MasterHandle,
  masterHandle :: ProcessHandle,
  slaveHandles :: [ProcessHandle]
}

main :: IO ()
main = do
  state <- setUpTopology
  runStateT runTests state
  return ()

runTests :: StateT TestState IO ()
runTests = do
  testN 5
  killNodes

setUpTopology :: IO TestState
setUpTopology = do
  m <- spawnCommand "stack exec kvstore-master -- -l -n 1"
  delay 100000
  sl <- spawnCommand "stack exec kvstore-slave -- -l -n 1 -i 0"
  delay 100000
  mh <- registerWithMaster testConfig
  return $ TestState mh m [sl]

testConfig :: Lib.Config
testConfig = do
  let hn = "127.0.0.1"
  let pno = PortNumber 1063
  let clientCfg = []
  let slaveCfg = [("127.0.0.1", PortNumber 1064)]
  Lib.Config hn pno clientCfg slaveCfg Nothing Nothing

killNodes :: StateT TestState IO ()
killNodes = get >>= \s -> liftIO $ do
  terminateProcess $ masterHandle s
  sequence $ Prelude.map terminateProcess $ slaveHandles s
  return ()

testN :: Int -> StateT TestState IO ()
testN 0 = return ()
testN n = get >>= \s -> do
  let key = C8.pack $ "key" ++ show n
  let val = C8.pack $ "val" ++ show n
  liftIO $ putVal (masterKVHandle s) key val
  testN (n-1)
