module Main where

import Lib
import System.IO
import Network
import Data.Maybe
import Data.List

type SlaveId = Int

main :: IO ()
main = do
  success <- Lib.parseArguments
  case success of
    Nothing     -> Lib.printUsage --an error occured
    Just config -> 
      let slaveId = fromMaybe (-1) (Lib.slaveNumber config)
      in if (slaveId <= (-1) || slaveId >= length (Lib.slaveConfig config))
         then Lib.printUsage --error
         else runKVSlave config slaveId

runKVSlave :: Lib.Config -> SlaveId -> IO ()
runKVSlave cfg slaveId = do
    let (slaveName, slavePortId) = (Lib.slaveConfig cfg) !! slaveId
    s <- listenOn slavePortId
    (h, hostName, portNumber) <- accept s
    msg <- hGetLine h
    putStrLn $ "[!] Slave received " ++ msg --print the message 
    hClose h