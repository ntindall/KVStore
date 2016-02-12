module KVProtocol
  (
    KVRequest(..)
  , KVResponse(..)
  , KVKey
  , KVVal
  ) where

import Data.Binary
import Data.ByteString.Lazy

type KVKey = ByteString

type KVVal = ByteString

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
                    errorMsg :: ByteString
                  }
  deriving (Eq, Show)

data KVObject = KBObject ByteString ByteString
  deriving (Eq, Show)


{-!
deriving instance Binary KVRequest
deriving instance Binary KVResponse
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

 
instance Binary KVObject where
        put (KBObject x1 x2)
          = do put x1
               put x2
        get
          = do x1 <- get
               x2 <- get
               return (KBObject x1 x2)
-- GENERATED STOP
