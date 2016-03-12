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

import Control.Monad.Trans (liftIO)

import Data.Maybe

main :: IO ()
main = do
  setUpTopology
  hspec $ do
    getTests
	--getTests
	--putTests

setUpTopology :: IO ()
setUpTopology = do
  spawnCommand "stack exec -- kvstore-master --local --size=1" --"stack" ["exec", "--", "kvstore-master", "--local", "--size=1"] []
  spawnCommand "stack exec -- kvstore-slave --local --size=1 --id=0"
  spawnCommand "stack exec -- kvstore-client --local --size=1"
  return ()


getTests :: Spec
getTests = describe "test get requests" $ do
  describe "single get request issued" $ do
    it "(key,val) -> val" $ do
      --portId@(PortNumber pid) <- NETWORK.socketPort s
  	  --hostName <- BSD.getHostName
      let hostName = "blah"
      let pno = PortNumber 1
      let masterPortId = PortNumber 1           
      let clientCfg = [("127.0.0.1", PortNumber 1064)]
      let slaveCfg = [("blee", PortNumber 2)]
      let overallCfg = Lib.Config hostName pno clientCfg slaveCfg Nothing Nothing

      finCfg <- registerWithMaster overallCfg
      let expOutput = C8.pack "val"
      actualOutput <- getVal finCfg $ C8.pack "key"

      actualOutput `shouldBe` expOutput
