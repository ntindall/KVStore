module Lib
    ( printUsage
    , options
    , parseArguments
    , Mode
    , Config(..)
    ) where

import System.Console.GetOpt
import System.Environment
import System.IO
import Network
import Data.Tuple.Utils

import Debug.Trace

data Mode = Local
          | RingSize Int
          | SlaveNum Int
        deriving (Show) --can add additional modes


data Config = Config { masterHostName :: HostName
                     , masterPortId   :: PortID
                     , clientConfig   :: [(HostName, PortID)]
                     , slaveConfig    :: [(HostName, PortID)]
                     , slaveNumber    :: Maybe Int
                     , clientNumber   :: Maybe Int
                     }
        deriving (Show)

options :: [ OptDescr Mode ]
options = [ Option ['l'] ["local"] (NoArg Local) "local mode"
          , Option ['n'] ["size"]  (ReqArg (\s -> RingSize (read s :: Int)) "Int") "ring size"
          , Option ['i'] ["id"]    (ReqArg (\s -> SlaveNum (read s :: Int)) "Int") "slaveId"
          ]

printUsage :: IO ()
printUsage = putStr $ usageInfo "KVStore" options

parseArguments :: IO (Maybe Config)
parseArguments = do
  args <- getArgs
  let (options, nonOpts, errors) = getOpt RequireOrder Lib.options args
  if not (null errors) || null options
  then return Nothing
  else
      let (isLocal, n, slaveN) = parseOptions options
          slaveCfg = allocSlaves isLocal n
      in case isLocal of
        True -> return $ Just Config { masterHostName = "localHost"
                                     , masterPortId   = PortNumber 1063
                                     , clientConfig   = [] --TODO ALLOW DYNAMIC REGISTRATION OF CLIENTS
                                     , slaveConfig    = slaveCfg
                                     , slaveNumber = slaveN
                                     , clientNumber = Nothing -- clients must dynamically register
                                     }
        False -> return Nothing


parseOptions :: [Mode]                        --options parsed from command line
             -> (Bool, Int, Maybe Int)        --(isLocal, ringSize, slaveId)
parseOptions l = optionsAcc l (False, 1, Nothing)
  where optionsAcc l' acc
          | null l' = acc
          | otherwise =
            let opt = head l'
            in case opt of
              Local      -> optionsAcc (tail l') (True, snd3 acc, thd3 acc)
              RingSize n -> optionsAcc (tail l') (fst3 acc, n, thd3 acc)
              SlaveNum s -> optionsAcc (tail l') (fst3 acc, snd3 acc, Just s)

allocSlaves :: Bool                                --is the configuration local?
            -> Int                                 --ring size
            -> [(HostName, PortID)]
allocSlaves True n  = zip (replicate n "localhost") (map PortNumber [1064..])
allocSlaves False _ = undefined --not implemented yet

