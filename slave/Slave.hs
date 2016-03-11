{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Lib
import System.IO as IO
import System.Directory as DIR
import System.FileLock
import Network as NETWORK
import Network.Socket as SOCKET
import Network.BSD as BSD
import Data.Maybe
import Data.List as List
import qualified Data.Map.Strict as Map

import Data.ByteString.Lazy as B
import Data.ByteString.Lazy.Char8 as C8
import Data.Tuple.Utils

import Control.Exception
import Control.Concurrent
import Control.Concurrent.Chan
import Control.Monad.Reader
import qualified Control.Monad.Catch as Catch
import Control.Arrow as CA

import Control.Monad
import Control.Concurrent.MState

import Debug.Trace

import qualified KVProtocol (getMessage, sendMessage, connectToMaster)
import KVProtocol hiding (getMessage, sendMessage, connectToMaster)

import Log as LOG
import qualified Utils

import Math.Probable
import System.Posix.Signals
import System.FileLock

--- todo, touch file when slave registers
-- todo, slave registration

type SlaveId = Int

data SlaveState = SlaveState {
                  sock :: NETWORK.Socket
                , channel :: Chan KVMessage
                , cfg :: Lib.Config
                , store :: Map.Map KVKey (KVVal, KVTime)
                , unresolvedTxns :: Map.Map KVTxnId KVMessage
                , recoveredTxns :: Map.Map KVTxnId KVMessage
                }
 --  deriving (Show)

main :: IO ()
main = do
  success <- Lib.parseArguments

  case success of
    Nothing     -> Lib.printUsage --an error occured
    Just config ->
      let slaveId = fromJust (Lib.slaveNumber config)
      in if slaveId <= (-1) || slaveId >= List.length (Lib.slaveConfig config)
         then Lib.printUsage --error
         else do
          let main_ = execMState runKVSlave $ SlaveState { cfg = config 
                                                         ,  store = Map.empty
                                                         ,  unresolvedTxns = Map.empty
                                                         ,  recoveredTxns = Map.empty 
                                                         } 
              --install trap
              handler = Catch (do
                threadDelay 1000000
                traceShowM $ "... [!][!][!] DEATH OCCURED... REBOOTING [!][!][!] ..."
                main_ >>= \_ -> return ())
          installHandler sigABRT handler Nothing
          main_ >>= (\_ -> return ())

listenOnPort :: PortNumber -> IO NETWORK.Socket
listenOnPort port = do
  proto <- BSD.getProtocolNumber "tcp"
  bracketOnError
    (SOCKET.socket AF_INET Stream proto)
    SOCKET.sClose
    (\sock -> do
        SOCKET.setSocketOption sock ReuseAddr 1
        SOCKET.setSocketOption sock ReusePort 1
        SOCKET.bindSocket sock (SockAddrInet port iNADDR_ANY)
        SOCKET.listen sock maxListenQueue
        return sock
    )


runKVSlave :: MState SlaveState IO ()
runKVSlave = get >>= \s -> do 
  c <- liftIO newChan

  liftIO $ IO.putStrLn "[!] REBUILDING... "

  let config = cfg s
      slaveId = fromJust $ Lib.slaveNumber config
      (slaveName, slavePortId@(PortNumber portNum)) = Lib.slaveConfig config !! slaveId
  skt <- liftIO $ listenOnPort portNum -- $ listenOn slavePortId

  fileExists <- liftIO $ DIR.doesFileExist (LOG.persistentLogName slaveId)
  if fileExists
  then do
    store <- liftIO $ liftM (Map.fromList . Utils.readKVList) $ B.readFile $ persistentFileName slaveId
    (unackedTxns, store') <- liftIO $ LOG.rebuild (LOG.persistentLogName slaveId) store

    let persistentFile = (persistentFileName slaveId)
        bakFile = persistentFile ++ ".bak"

    liftIO $ withFileLock persistentFile Exclusive
                          (\_ -> withFileLock bakFile Exclusive
                            (\_ -> do
                              B.writeFile bakFile (Utils.writeKVList $ Map.toList store')
                              DIR.renameFile bakFile persistentFile
                            )
                          )

    modifyM_ $ \s -> s { sock = skt, channel = c, store = store', recoveredTxns = unackedTxns }
  else do
   -- IO.openFile (LOG.persistentLogName slaveId) IO.AppendMode
    modifyM_ $ \s -> s { sock = skt, channel = c, store = Map.empty }
    return ()
  liftIO $ IO.putStrLn "[!] DONE REBUILDING... "

  forkM_ sendResponses
  forkM_ checkpoint

  processMessages

checkpoint :: MState SlaveState IO ()
checkpoint = get >>= \s -> do 
  liftIO $ do
    let config = cfg s
        mySlaveId = fromJust (Lib.slaveNumber config)

    IO.putStrLn "[!][!][!] CHECKPOINT"
    --If we have fully recovered
    if (Map.null $ recoveredTxns s)
    then do
      --lock down the log file, cannot have race between checkpoint being written
       --and log being flushed.
      withFileLock (LOG.persistentLogName mySlaveId) Exclusive
                   (\_ -> withFileLock (persistentFileName mySlaveId) Exclusive
                      (\_ -> do
                        B.writeFile (persistentFileName mySlaveId) (Utils.writeKVList $ Map.toList (store s))
                        LOG.flush (LOG.persistentLogName mySlaveId) 
                      )
                   )
    else do
      withFileLock (persistentFileName mySlaveId) Exclusive 
                   (\_ -> B.writeFile (persistentFileName mySlaveId) 
                                      (Utils.writeKVList $ Map.toList (store s))
                   )

  liftIO $ threadDelay 10000000
  checkpoint

processMessages :: MState SlaveState IO ()
processMessages = get >>= \s -> do
  let sock' = sock s
      c = channel s

  liftIO $ catch (bracket (NETWORK.accept sock')
                          (\(h, _, _) -> hClose h)
                          (\(h, hostName, portNumber) -> liftIO $ do
                             msg <- KVProtocol.getMessage h
                             either (\err -> IO.putStr $ show err ++ "\n")
                                    (\suc -> writeChan c suc)
                                    msg
                          )
                  )
                  --Wait for resources to free a bit (especially a problem when local)
                  (\(e :: SomeException) -> threadDelay 10000)
  processMessages


persistentFileName :: Int -> String  --todo, hacky
persistentFileName slaveId = "database/kvstore_" ++ show slaveId ++ ".txt"

sendResponses :: MState SlaveState IO ()
sendResponses = get >>= \s -> do
  thunk <- liftIO $ do
            message <- readChan (channel s)

            return $ case message of
              KVResponse{} -> handleResponse
              KVRequest{}  -> handleRequest message
              KVVote{}     -> handleVote --protocol error?
              KVAck{}      -> handleAck -- protocol error?
              KVDecision{} -> handleDecision message

  forkM_ thunk

  sendResponses

getMyId :: Lib.Config -> Int
getMyId cfg = fromJust $ Lib.slaveNumber cfg

handleRequest :: KVMessage -> MState SlaveState IO ()
handleRequest msg = get >>= \s -> do
  let config = cfg s

  h <- liftIO $ KVProtocol.connectToMaster config
  let field_txn  = txn_id msg
      field_request = request msg
      mySlaveId = getMyId config

  case field_request of
      PutReq ts key val -> do
        -- LOG READY, <timestamp, txn_id, key, newval>
        
        modifyM_ $ \s' ->
          let updatedTxnMap = Map.insert field_txn msg (unresolvedTxns s')
          in s' {unresolvedTxns = updatedTxnMap}

        -- LOCK!!!@!@! !@ ! !
        liftIO $ do
          LOG.writeReady (LOG.persistentLogName mySlaveId) msg

        -- UNLOCK

          KVProtocol.sendMessage h (KVVote field_txn mySlaveId VoteReady field_request)
        --vote abort if invalid key value
      GetReq _ key     -> liftIO $ do 
        case Map.lookup key (store s)  of
          Nothing -> KVProtocol.sendMessage h (KVResponse field_txn mySlaveId (KVSuccess key Nothing))
          Just val -> KVProtocol.sendMessage h (KVResponse field_txn mySlaveId (KVSuccess key (Just (fst val))))

  liftIO $ IO.hClose h

safeUpdateStore :: KVKey -> KVVal -> KVTime -> MState SlaveState IO ()
safeUpdateStore k v ts = modifyM_ $ \s -> do
  let oldVal = Map.lookup k (store s)

  if isNothing oldVal || snd (fromJust oldVal) <= ts 
  then s { store = Map.insert k (v,ts) (store s)}
  else s

clearTxn :: KVTxnId -> MState SlaveState IO ()
clearTxn tid = modifyM_ $ \s -> s { unresolvedTxns = Map.delete tid (unresolvedTxns s)
                                  , recoveredTxns = Map.delete tid (recoveredTxns s)
                                  }

--Lookup the value in both transaction maps
lookupTX :: KVTxnId -> SlaveState -> Maybe KVMessage
lookupTX tid s = let inR = Map.lookup tid (unresolvedTxns s)
                 in if (isJust inR) then inR else Map.lookup tid (recoveredTxns s)

handleDecision :: KVMessage -> MState SlaveState IO ()
handleDecision msg@(KVDecision tid decision _) = get >>= \s -> do
  --REMOVE THIS IF YOU WANT SLAVE TO NOT DIE PROBABALISTICALLY
  death <- liftIO $ mwc $ intIn (1,100)
  --don't make death too probable, leads to unrealistic problems (OS type issues)
  if (death > 101) then liftIO $ raiseSignal sigABRT --die "--DIE!"
  else do 
    let config = cfg s
        maybeRequest = lookupTX tid s
        mySlaveId = getMyId config

    if isJust maybeRequest 
    then do
      let field_request = fromJust $ maybeRequest
          (ts, key,val) = case request field_request of -- TODO, do we even need the decision to ahve the request anymore?
                        (PutReq ts k v) -> (ts, k, v)
                        (GetReq ts _) -> undefined -- protocol error

      if decision == DecisionCommit 
      then do
        safeUpdateStore key val ts 
        -- NEED TO FILELOCK
        liftIO $ LOG.writeCommit (LOG.persistentLogName mySlaveId) msg
      else do
        liftIO $ LOG.writeAbort (LOG.persistentLogName mySlaveId) msg

      clearTxn tid
      let errormsg = if decision == DecisionCommit then Nothing
                     else Just $ C8.pack "Transaction aborted"
      liftIO $ do
        h <- KVProtocol.connectToMaster config
        KVProtocol.sendMessage h (KVAck tid (Just mySlaveId) errormsg)
        IO.hClose h
    else --nothing to do, but need to ACK to let master know we are back alive
      liftIO $ do
        h <- KVProtocol.connectToMaster config
        KVProtocol.sendMessage h (KVAck tid (Just mySlaveId) (Just "Duplicate DECISION message"))
        IO.hClose h

handleResponse = undefined
handleVote = undefined
handleAck = undefined
