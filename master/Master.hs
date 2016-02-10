module Main where

import Lib
import System.IO
import Network

main :: IO ()
main = do
  success <- Lib.parseArguments
  case success of
    Nothing     -> Lib.printUsage --an error occured
    Just config -> runKVMaster config

runKVMaster :: Lib.Config -> IO ()
runKVMaster cfg = do 
  s <- listenOn (Lib.masterPortId cfg)
  (h, hostName, portNumber) <- accept s
  msg <- hGetLine h
  putStrLn $ "[!] Server received " ++ msg --print the message
  -- todo... threading???

  let (slave1Name, slave1PortId) = head (Lib.slaveConfig cfg)
  slaveH <- connectTo slave1Name slave1PortId
  hPutStr slaveH msg
  hClose h
