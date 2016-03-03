module Utils where

import Data.ByteString.Lazy.Char8 as C8
import Data.ByteString.Lazy as B

import Data.Time.Clock.POSIX

import Control.Arrow

writeKVList :: [(B.ByteString, B.ByteString)] -> B.ByteString
writeKVList kvstore = C8.intercalate (C8.pack "\n") $ Prelude.map helper kvstore
  where helper (k,v) = C8.concat [k, C8.pack "=", v]

readKVList :: B.ByteString -> [(B.ByteString, B.ByteString)]
readKVList = Prelude.map parseField . C8.lines
  where parseField = second (C8.drop 1) . C8.break (== '=')

--https://stackoverflow.com/questions/17909770/get-time-as-int
currentTimeInt :: IO Int
currentTimeInt = round `fmap` getPOSIXTime :: IO Int