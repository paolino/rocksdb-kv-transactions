module Data.Serialize.Extra
    ( evalPutM
    , unsafeEvalGet
    , evalGetM
    )
where

import Data.ByteString (ByteString)
import Data.Serialize
    ( Get
    , PutM
    , runGet
    , runPutM
    )

evalPutM :: PutM a -> ByteString
evalPutM putM = snd $ runPutM putM

unsafeEvalGet :: Get a -> ByteString -> a
unsafeEvalGet get bs = case runGet get bs of
    Right a -> a
    Left err -> error $ "unsafeEvalGet: parse error: " ++ err

evalGetM :: Get a -> ByteString -> Maybe a
evalGetM get bs = case runGet get bs of
    Right a -> Just a
    Left _err -> Nothing
