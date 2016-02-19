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

someFunc :: IO ()
someFunc = putStrLn "someFunc"

options :: [ OptDescr Mode ]
options = [ Option ['l'] ["local"] (NoArg Local) "local mode"
          , Option ['n'] ["size"]  (ReqArg (\s -> RingSize (read s :: Int)) "Int") "ring size"
          , Option ['i'] ["id"]    (ReqArg (\s -> SlaveNum (read s :: Int)) "Int") "slaveId"
          ] 

printUsage :: IO ()
printUsage = do
  putStr $ usageInfo "KVStore" options


parseArguments :: IO (Maybe Config)
parseArguments = do
  args <- getArgs
  let (options, nonOpts, errors) = getOpt RequireOrder Lib.options args
  if not (null errors) 
  then return Nothing
  else 
    if (null options) then return Nothing -- error
    else 
      let (isLocal, n, slaveN) = parseOptions options
          slaveCfg = allocSlaves isLocal n
      in case (isLocal) of
        True -> return $ Just Config { masterHostName = "localHost"
                                     , masterPortId   = (PortNumber 1063)
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
            let opt = (head l')
            in case opt of 
              Local      -> optionsAcc (tail l') (True, snd' acc, thrd acc)
              RingSize n -> optionsAcc (tail l') (fst' acc, n, thrd acc)
              SlaveNum s -> optionsAcc (tail l') (fst' acc, snd' acc, Just s)

allocSlaves :: Bool                                --is the configuration local?
            -> Int                                 --ring size
            -> [(HostName, PortID)]
allocSlaves True n  = zip (replicate n "localhost") (map PortNumber [1064..])
allocSlaves False _ = undefined --not implemented yet

---------------- HELPER FUNCTIONS -------------------

fst' :: (a,b,c) -> a
fst' (a,_,_) = a

snd' :: (a,b,c) -> b
snd' (_,b,_) = b

thrd :: (a,b,c) -> c
thrd (_,_,c) = c