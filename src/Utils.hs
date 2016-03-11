module Utils where

import Data.ByteString.Lazy.Char8 as C8
import Data.ByteString.Lazy as B

import qualified Data.Map.Strict as Map
import qualified Data.Set as Set

import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Ratio

import Network as NETWORK
import Network.Socket as SOCKET
import Network.BSD as BSD

import Control.Arrow
import Control.Exception

import KVProtocol

writeKVList :: [(KVKey, (KVVal, KVTime))] -> B.ByteString
writeKVList kvstore = C8.intercalate (C8.pack "\n") $ Prelude.map helper kvstore
  where helper (k,(v,ts)) = C8.concat [C8.pack (show ts), C8.pack ":", k, C8.pack "=", v]

readKVList :: B.ByteString -> [(KVKey, (KVVal, KVTime))]
readKVList = Prelude.map parseField . C8.lines
  where parseField line =
          let (ts, kv) = C8.break (== ':') line
              ts_int = read (C8.unpack ts) :: KVTime
              (k, v) = C8.break (== '=') (C8.drop 1 kv)
          in (k, (C8.drop 1 v, ts_int))

--https://stackoverflow.com/questions/17909770/get-time-as-int
--microseconds
currentTimeMicro :: IO Integer
currentTimeMicro = numerator . toRational . (* 1000000) <$> getPOSIXTime

getFreeSocket :: IO Socket
getFreeSocket = do
  proto <- BSD.getProtocolNumber "tcp"
  bracketOnError
    (SOCKET.socket AF_INET Stream proto)
    SOCKET.sClose
    (\sock -> do
        let port = aNY_PORT
        SOCKET.setSocketOption sock ReuseAddr 1
        SOCKET.bindSocket sock (SockAddrInet port iNADDR_ANY)
        SOCKET.listen sock maxListenQueue
        return sock
    )

insertM :: Ord a => a -> b -> Maybe (Map.Map a b) -> Map.Map a b
insertM k v (Just m) = Map.insert k v m
insertM k v Nothing = Map.singleton k v

insertS :: Ord a => a -> Maybe (Set.Set a) -> Set.Set a
insertS v (Just s) = Set.insert v s
insertS v Nothing = Set.singleton v
