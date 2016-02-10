module Main where

import qualified Lib as Lib
import System.Console.GetOpt
import System.Environment
import System.IO
import Network

import Debug.Trace

main :: IO ()
main = do
  success <- Lib.parseArguments
  case success of
    Nothing     -> Lib.printUsage --an error occured
    Just config -> runKVClient config

runKVClient :: Lib.Config -> IO ()
runKVClient cfg = do
  msg <- getLine
  h <- connectTo (Lib.masterHostName cfg) (Lib.masterPortId cfg)
  hPutStr h msg
  hClose h