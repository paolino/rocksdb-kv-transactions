module Database.KV.RocksDB
    ( mkRocksDBDatabase
    )
where

import Control.Monad.IO.Class (MonadIO (..))
import Database.KV.Database
    ( Database (..)
    , Pos (..)
    , QueryIterator (..)
    )
import Database.KV.Transaction
    ( Column (..)
    , DMap
    )
import Database.RocksDB
    ( BatchOp (DelCF, PutCF)
    , ColumnFamily
    , DB
    , createIterator
    , destroyIterator
    , getCF
    , iterEntry
    , iterFirst
    , iterLast
    , iterNext
    , iterPrev
    , iterSeek
    , iterValid
    , write
    )

mkRocksDBDatabase
    :: MonadIO m
    => DB
    -> DMap t (Column ColumnFamily)
    -> Database m ColumnFamily t BatchOp
mkRocksDBDatabase db columns =
    Database
        { valueAt = \cf k -> do
            getCF db cf k
        , applyOps = \ops -> do
            write db ops
        , mkOperation = \cf k mv ->
            case mv of
                Just v -> PutCF cf k v
                Nothing -> DelCF cf k
        , columns
        , newIterator = \cf -> do
            i <- createIterator db $ Just cf
            return
                $ QueryIterator
                    { isValid = liftIO $ iterValid i
                    , entry = liftIO $ iterEntry i
                    , step = \pos -> liftIO $ case pos of
                        PosFirst -> iterFirst i
                        PosLast -> iterLast i
                        PosNext -> iterNext i
                        PosPrev -> iterPrev i
                        PosAny k -> iterSeek i k
                        PosDestroy -> destroyIterator i
                    }
        }
