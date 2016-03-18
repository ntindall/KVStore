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
          | WorkerNum Int
        deriving (Show) --can add additional modes


data Config = Config { masterHostName :: HostName
                     , masterPortId   :: PortID
                     , clientConfig   :: [(HostName, PortID)]
                     , workerConfig    :: [(HostName, PortID)]
                     , workerNumber    :: Maybe Int
                     , clientNumber   :: Maybe Int
                     }
        deriving (Show)

options :: [ OptDescr Mode ]
options = [ Option ['l'] ["local"] (NoArg Local) "local mode"
          , Option ['n'] ["size"]  (ReqArg (\s -> RingSize (read s :: Int)) "Int") "ring size"
          , Option ['i'] ["id"]    (ReqArg (\s -> WorkerNum (read s :: Int)) "Int") "workerId"
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
      let (isLocal, n, workerN) = parseOptions options
          workerCfg = allocworkers isLocal n
      in case isLocal of
        True -> return $ Just Config {  masterHostName = "127.0.0.1"
                                      , masterPortId   = PortNumber 1063
                                      , clientConfig   = [] --TODO ALLOW DYNAMIC REGISTRATION OF CLIENTS
                                      , workerConfig   = workerCfg
                                      , workerNumber   = workerN
                                      , clientNumber   = Nothing -- clients must dynamically register
                                      }
        False -> return $ Just Config { masterHostName = "171.67.216.73" --corn 08
                                      , masterPortId   = PortNumber 1063
                                      , clientConfig   = [] --TODO ALLOW DYNAMIC REGISTRATION OF CLIENTS
                                      , workerConfig   = workerCfg
                                      , workerNumber   = workerN
                                      , clientNumber   = Nothing -- clients must dynamically register
                                      }


parseOptions :: [Mode]                        --options parsed from command line
             -> (Bool, Int, Maybe Int)        --(isLocal, ringSize, workerId)
parseOptions l = optionsAcc l (False, 1, Nothing)
  where optionsAcc l' acc
          | null l' = acc
          | otherwise =
            let opt = head l'
            in case opt of
              Local      -> optionsAcc (tail l') (True, snd3 acc, thd3 acc)
              RingSize n -> optionsAcc (tail l') (fst3 acc, n, thd3 acc)
              WorkerNum s -> optionsAcc (tail l') (fst3 acc, snd3 acc, Just s)

allocworkers :: Bool                                --is the configuration local?
            -> Int                                 --ring size
            -> [(HostName, PortID)]
allocworkers True n  = zip (replicate n "127.0.0.1") (map PortNumber [1064..])
allocworkers False n = zip (["171.67.216.74" --corn09
                             ,"171.67.216.72" --corn07
                             ,"171.67.216.76" --corn11
                             ,"171.67.216.67" --corn02
                             ,"171.67.216.75" --corn10
                             ,"171.67.216.77" --corn12
                             ,"171.67.216.79" --corn14
                             ,"171.67.216.80" --corn15
                             ]) (replicate n (PortNumber 1064))

