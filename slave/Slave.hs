module Main where

import Lib
import System.IO
import Network

main :: IO ()
main = do
  success <- Lib.parseArguments
  case success of
    Nothing     -> Lib.printUsage --an error occured
    Just config -> runKVSlave config

runKVSlave :: Lib.Config -> IO ()
runKVSlave cfg = do
  let (slave1Name, slave1PortId) = head (Lib.slaveConfig cfg)
  s <- listenOn slave1PortId
  (h, hostName, portNumber) <- accept s
  msg <- hGetLine h
  putStrLn $ "[!] Slave received " ++ msg --print the message 
  hClose h