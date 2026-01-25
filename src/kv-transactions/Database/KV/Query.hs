{- |
Module      : Database.KV.Query
Description : Read-only queries on type-safe key-value databases
Copyright   : (c) Paolo Veronelli, 2024
License     : Apache-2.0

This module provides a read-only query interface over the 'Database' abstraction.
Unlike 'Transaction', queries do not buffer writes and cannot modify the database.

= Snapshot Semantics

All operations within a single 'interpretQuerying' call operate on the same
database snapshot, providing atomic/consistent reads. This means:

* Multiple 'query' calls see the same data
* 'iterating' cursors see the same data as 'query' calls
* Concurrent writes by other threads are not visible

= When to Use

Use 'Querying' when you only need to read data and want consistent reads
across multiple keys. For read-modify-write operations, use 'Transaction' instead.
-}
module Database.KV.Query
    ( -- * Query Instructions
      QueryInstruction

      -- * Query Monad
    , Querying
    , query
    , iterating

      -- * Running Queries
    , interpretQuerying

      -- * Re-exports
    , module Data.GADT.Compare
    , module Data.Dependent.Map
    , module Data.Dependent.Sum
    )
where

import Control.Lens (review)
import Control.Monad.Operational
    ( ProgramT
    , ProgramViewT (..)
    , singleton
    , viewT
    )
import Control.Monad.Trans.Class (MonadTrans (..))
import Data.Dependent.Map (DMap, fromList)
import Data.Dependent.Map qualified as DMap
import Data.Dependent.Sum (DSum ((:=>)))
import Data.GADT.Compare (GCompare (..), GEq (..), GOrdering (..))
import Database.KV.Cursor (Cursor, interpretCursor)
import Database.KV.Database
    ( Codecs (keyCodec)
    , Column (Column, codecs, family)
    , Database (Database, columns, newIterator, valueAt, withSnapshot)
    , KeyOf
    , ValueOf
    , decodeValueThrow
    , hoistQueryIterator
    )

{- |
Low-level query instructions.
These are interpreted by 'interpretQuerying'.
-}
data QueryInstruction m cf t op a where
    -- | Read a value from a column
    Query
        :: (GCompare t, Ord (KeyOf c))
        => t c
        -> KeyOf c
        -> QueryInstruction m cf t op (Maybe (ValueOf c))
    -- | Run a cursor program over a column
    Iterating
        :: (GCompare t)
        => t c
        -> Cursor (Querying m cf t op) c a
        -> QueryInstruction m cf t op a

{- |
Query monad for composing read-only database operations.
Built using the operational monad pattern for easy interpretation.
-}
type Querying m cf t op =
    ProgramT (QueryInstruction m cf t op) m

{- |
Read a value from a column.
Returns @Nothing@ if the key doesn't exist.
-}
query
    :: (GCompare t, Ord (KeyOf c))
    => t c
    -- ^ Column selector
    -> KeyOf c
    -- ^ Key to look up
    -> Querying m cf t op (Maybe (ValueOf c))
query t k = singleton $ Query t k

{- |
Run a cursor program over a column.
Enables range queries and iteration over entries.

The cursor operates on a snapshot, providing consistent reads
across multiple entries within the same 'iterating' call.
-}
iterating
    :: (GCompare t)
    => t c
    -- ^ Column selector
    -> Cursor (Querying m cf t op) c a
    -- ^ Cursor program to execute
    -> Querying m cf t op a
iterating t cursorProg = singleton $ Iterating t cursorProg

-- | Execute a single query instruction against the database.
interpretQuery
    :: (GCompare t, MonadFail m)
    => Database m cf t op
    -> t c
    -> KeyOf c
    -> m (Maybe (ValueOf c))
interpretQuery Database{valueAt, columns} t k = do
    Column{family = cf, codecs = codecs} <-
        case DMap.lookup t columns of
            Just col -> pure col
            Nothing -> fail "query: column not found"
    rvalue <- valueAt cf $ review (keyCodec codecs) k
    mapM (decodeValueThrow codecs) rvalue

-- | Execute a cursor program against the database.
interpretIterating
    :: (GCompare t, MonadFail m)
    => Database m cf t op
    -> t c
    -> Cursor (Querying m cf t op) c a
    -> m a
interpretIterating db@Database{newIterator, columns} t cursorProg = do
    column <-
        case DMap.lookup t columns of
            Just col -> pure col
            Nothing -> fail "interpretIterating: column not found"
    qi <- newIterator (family column)
    interpretLoop db
        $ interpretCursor
            (hoistQueryIterator lift qi)
            column
            cursorProg

{- |
Interpret a query program against the database.

All operations run on a single snapshot, providing atomic reads.
Point queries and iterators all see the same consistent view of the database.
-}
interpretQuerying
    :: (GCompare t, MonadFail m)
    => Database m cf t op
    -- ^ Database to query
    -> Querying m cf t op a
    -- ^ Query program to execute
    -> m a
interpretQuerying db prog =
    withSnapshot db $ \snapDB -> interpretLoop snapDB prog

-- | Internal: Interpret query instructions on a snapshot-aware database.
interpretLoop
    :: (GCompare t, MonadFail m)
    => Database m cf t op
    -> Querying m cf t op a
    -> m a
interpretLoop db prog = do
    v <- viewT prog
    case v of
        Return a -> pure a
        instr :>>= cont -> case instr of
            Query t key -> do
                r <- interpretQuery db t key
                interpretLoop db $ cont r
            Iterating t cursorProg -> do
                r <- interpretIterating db t cursorProg
                interpretLoop db $ cont r
