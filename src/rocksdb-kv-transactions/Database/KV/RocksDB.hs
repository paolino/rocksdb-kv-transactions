{- |
Module      : Database.KV.RocksDB
Description : RocksDB backend for type-safe key-value transactions
Copyright   : (c) Paolo Veronelli, 2024
License     : Apache-2.0

This module provides a RocksDB implementation of the 'Database' interface,
enabling persistent storage with full transaction support.

= Features

* Column families for logical separation of data
* Atomic batch writes via RocksDB's WriteBatch
* Efficient range iteration via RocksDB iterators
* Consistent reads within cursor operations

= Usage

@
import Database.RocksDB (open, defaultOptions)
import Database.KV.RocksDB (mkRocksDBDatabase)
import Database.KV.Transaction (newRunTransaction, query, insert)

main = do
    db <- open "mydb" defaultOptions
    let database = mkRocksDBDatabase db columns
    runTx <- newRunTransaction database
    runTx $ do
        insert Users "user1" User{...}
        mUser <- query Users "user1"
        ...
@

= Column Families

RocksDB column families provide logical separation of data within
a single database. Each column in your schema maps to a column family.
You must create column families before using them.
-}
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

{- | Create a 'Database' backed by RocksDB.

This connects the abstract 'Database' interface to RocksDB operations:

* 'valueAt' uses 'getCF' for point lookups
* 'applyOps' uses 'write' for atomic batch operations
* 'newIterator' creates RocksDB iterators for range queries

The @columns@ parameter maps your typed column selectors to
RocksDB column families with their serialization codecs.
-}
mkRocksDBDatabase
    :: (MonadIO m)
    => DB
    -- ^ Open RocksDB database handle
    -> DMap t (Column ColumnFamily)
    -- ^ Column definitions mapping selectors to column families
    -> Database m ColumnFamily t BatchOp
mkRocksDBDatabase db columns =
    Database
        { valueAt = \cf k -> do
            -- Point lookup in the specified column family
            getCF db cf k
        , applyOps = \ops -> do
            -- Atomic batch write of all operations
            write db ops
        , mkOperation = \cf k mv ->
            -- Create put or delete operation based on value presence
            case mv of
                Just v -> PutCF cf k v
                Nothing -> DelCF cf k
        , columns
        , newIterator = \cf -> do
            -- Create a new iterator for range queries
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
