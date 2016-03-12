module Main (main) where

import Data.ByteString.Lazy as B
import Data.ByteString.Lazy.Char8 as C8

import Test.Hspec
import Test.QuickCheck hiding (Result)
import Test.QuickCheck.Property (Result, rejected, liftBool)

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

data TestState = TestState {
   masterHandles :: [ProcessHandle], 
   clientHandles :: [ProcessHandle],
   slaveHandles :: [ProcessHandle]
}

main :: IO ()
main = do
  (s, a) <- runStateT runTests $ TestState [] [] []
  return ()

runTests :: StateT TestState IO ()
runTests = do
  setUpTopology
  liftIO $ hspec $ do
    getTests

setUpTopology :: StateT TestState IO ()
setUpTopology = get >>= \s -> do
  m <- liftIO $ spawnCommand "stack exec -- kvstore-master --local --size=1"
  sl <- liftIO $ spawnCommand "stack exec -- kvstore-slave --local --size=1 --id=0"
  c <- liftIO $ spawnCommand "stack exec -- kvstore-client --local --size=1"

  put s { masterHandles = [m], clientHandles = [c], slaveHandles = [sl] }
  return ()


getTests :: Spec
getTests = describe "test get requests" $ do
  describe "single get request issued" $ do
    it "(key,val) -> val" $ do
      --portId@(PortNumber pid) <- NETWORK.socketPort s
      --hostName <- BSD.getHostName
      let pno = PortNumber 1
      let masterPortId = PortNumber 1           
      let clientCfg = [("127.0.0.1", PortNumber 1064)]
      let slaveCfg = [("blee", PortNumber 2)]
      let overallCfg = Lib.Config "" pno clientCfg slaveCfg Nothing Nothing

      finCfg <- registerWithMaster overallCfg
      let expOutput = C8.pack "val"
      actualOutput <- getVal finCfg $ C8.pack "key"

      actualOutput `shouldBe` expOutput
