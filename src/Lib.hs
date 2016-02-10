module Lib
    ( someFunc
    , printUsage
    , options
    , parseArguments
    , Mode
    , Config(..)
    ) where

import System.Console.GetOpt
import System.Environment
import System.IO
import Network

data Mode = Local
          | Dist 
        deriving (Show) --can add additional modes


data Config = Config { masterHostName :: HostName
                     , masterPortId   :: PortID
                     , slaveConfig    :: [(HostName, PortID)]
                     }
        deriving (Show)

someFunc :: IO ()
someFunc = putStrLn "someFunc"

options :: [ OptDescr Mode ]
options = [ Option ['l'] ["local"] (NoArg Local) "local mode" ] 

printUsage :: IO ()
printUsage = do
  putStr $ usageInfo "KVStore" options


parseArguments :: IO (Maybe Config)
parseArguments = do
  args <- getArgs
  let (isLocal, nonOpts, errors) = getOpt RequireOrder Lib.options args
  if not (null errors) 
  then return Nothing
  else case (null isLocal) of
    False -> return $ Just Config { masterHostName = "localHost",
                                    masterPortId   = (PortNumber 1063),
                                    slaveConfig    = [("localhost", (PortNumber 1064))]
                                  }

    True  -> return Nothing