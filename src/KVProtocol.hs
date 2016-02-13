{-# LANGUAGE OverloadedStrings #-}

module KVProtocol
  (
    KVRequest(..)
  , KVResponse(..)
  , KVMessage(..)
  , KVKey
  , KVVal
  , getMessage
  , sendMessage
  ) where

import Data.Binary
import Data.ByteString.Lazy  as B
import Data.ByteString.Char8 as C8
import Debug.Trace

import Network
import System.IO as IO

type KVKey = B.ByteString

type KVVal = B.ByteString

data KVRequest = Get {
                  key :: KVKey
                }
               | Put {
                  key :: KVKey
                , val :: KVVal
                }
  deriving (Eq, Show)

data KVResponse = KVSuccess {
                    obj :: KVObject
                  }
                | KVFailure {
                    errorMsg :: B.ByteString
                  }
  deriving (Eq, Show)

data KVMessage = KVResponse {
                  response :: KVResponse
                }
               | KVRequest {
                  request :: KVRequest
                }
  deriving (Eq, Show)

data KVObject = KBObject B.ByteString B.ByteString
  deriving (Eq, Show)

decodeMsg :: B.ByteString -> KVMessage
decodeMsg b = (decode b) :: KVMessage

getMessage :: Handle -> IO(Either String KVMessage)
getMessage h = do
  msg <- C8.hGetLine h
  case (C8.null msg) of
    True -> return $ Left "Handle is empty"
    False -> let req = decodeMsg $ fromStrict msg 
             in return $ Right req



sendMessage :: Handle -> KVMessage -> IO ()
sendMessage h req = do
  IO.putStr $ (show req) ++ ['\n']
  C8.hPutStrLn h $ toStrict (encode req)

{-!
deriving instance Binary KVRequest
deriving instance Binary KVResponse
deriving instance Binary KVMessage
deriving instance Binary KVObject
!-}

-- stack exec derive -- -a src/KVProtocol.hs 
-- GENERATED START

 
instance Binary KVRequest where
        put x
          = case x of
                Get x1 -> do putWord8 0
                             put x1
                Put x1 x2 -> do putWord8 1
                                put x1
                                put x2
        get
          = do i <- getWord8
               case i of
                   0 -> do x1 <- get
                           return (Get x1)
                   1 -> do x1 <- get
                           x2 <- get
                           return (Put x1 x2)
                   _ -> error "Corrupted binary data for KVRequest"

 
instance Binary KVResponse where
        put x
          = case x of
                KVSuccess x1 -> do putWord8 0
                                   put x1
                KVFailure x1 -> do putWord8 1
                                   put x1
        get
          = do i <- getWord8
               case i of
                   0 -> do x1 <- get
                           return (KVSuccess x1)
                   1 -> do x1 <- get
                           return (KVFailure x1)
                   _ -> error "Corrupted binary data for KVResponse"

 
instance Binary KVMessage where
        put x
          = case x of
                KVResponse x1 -> do putWord8 0
                                    put x1
                KVRequest x1 -> do putWord8 1
                                   put x1
        get
          = do i <- getWord8
               case i of
                   0 -> do x1 <- get
                           return (KVResponse x1)
                   1 -> do x1 <- get
                           return (KVRequest x1)
                   _ -> error "Corrupted binary data for KVMessage"

 
instance Binary KVObject where
        put (KBObject x1 x2)
          = do put x1
               put x2
        get
          = do x1 <- get
               x2 <- get
               return (KBObject x1 x2)
-- GENERATED STOP
