{-# LANGUAGE OverloadedStrings #-}

module Main where

import qualified ClientLib as CL
import qualified Utils as Utils
import qualified Lib as Lib

main :: IO ()
main = Lib.parseArguments >>= \(Just config) -> do
  master <- CL.registerWithMaster config
  getVal <- CL.getKey master "Hello"
  putVal <- CL.putKey master "Hello" "3"
  return ()
  --todo, unregisted