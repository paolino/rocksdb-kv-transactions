{- |
Module      : Database.KV.Database
Description : Core database abstractions for type-safe key-value storage
Copyright   : (c) Paolo Veronelli, 2024
License     : Apache-2.0

This module provides the foundational types for building type-safe key-value
databases with multiple collections (columns). It uses dependent types via
'DMap' to ensure that keys and values are correctly typed for each collection.

= Overview

The key abstraction is the 'Database' record, which provides a backend-agnostic
interface for:

* Reading values by key ('valueAt')
* Applying batch operations ('applyOps')
* Creating iterators for range queries ('newIterator')

Each collection is represented by a 'Column' which pairs a backend-specific
column family identifier with 'Codecs' for serializing keys and values.

= Type Safety

The 'KV' phantom type and associated type families ('KeyOf', 'ValueOf') ensure
that operations on a collection use the correct key and value types at compile time.

= Example

@
data MyColumns c where
    Users :: MyColumns (KV UserId User)
    Posts :: MyColumns (KV PostId Post)

-- GCompare instance enables lookup in DMap
instance GCompare MyColumns where ...
@
-}
module Database.KV.Database
    ( -- * Column Type Representation
      -- | Types for representing key-value pairs in a type-safe manner
      KV
    , Selector
    , KeyOf
    , ValueOf

      -- * Codecs for Serialization
    , Codecs (..)
    , decodeValueThrow

      -- * Column Definition
    , Column (..)
    , getColumn
    , mkColumns

      -- * Database Interface
    , Database (..)
    , hoistDatabase
    , mkOp

      -- * Iterator Types
    , Pos (..)
    , QueryIterator (..)
    , hoistQueryIterator

      -- * Utilities
    , mkCols

      -- * Re-exports
    , module Data.GADT.Compare
    , module Data.Dependent.Map
    , module Data.Dependent.Sum
    )
where

import Control.Lens (Prism', preview, review)
import Data.ByteString (ByteString)
import Data.Dependent.Map (DMap, fromList)
import Data.Dependent.Map qualified as DMap
import Data.Dependent.Sum (DSum ((:=>)))
import Data.GADT.Compare (GCompare (..), GEq (..), GOrdering (..))

-- | Phantom type representing a key-value pair in a collection.
-- Used as a type-level tag to associate keys and values.
--
-- @KV UserId User@ represents a collection mapping @UserId@ to @User@.
data KV k v

-- | Type alias for selecting a column from a column family.
-- @Selector t k v@ is a column selector @t@ for key type @k@ and value type @v@.
type Selector t k v = t (KV k v)

-- | Extract the key type from a 'KV' pair.
--
-- @KeyOf (KV UserId User) ~ UserId@
type family KeyOf c where
    KeyOf (KV k v) = k

-- | Extract the value type from a 'KV' pair.
--
-- @ValueOf (KV UserId User) ~ User@
type family ValueOf c where
    ValueOf (KV k v) = v

-- | Codecs for encoding and decoding keys and values to/from 'ByteString'.
-- Uses lens 'Prism'' for bidirectional encoding that may fail on decode.
data Codecs c = Codecs
    { keyCodec :: Prism' ByteString (KeyOf c)
    -- ^ Prism for encoding/decoding keys
    , valueCodec :: Prism' ByteString (ValueOf c)
    -- ^ Prism for encoding/decoding values
    }

-- | A column definition pairing a backend-specific column family
-- with codecs for serialization.
data Column cf c = Column
    { family :: cf
    -- ^ Backend-specific column family identifier (e.g., RocksDB ColumnFamily)
    , codecs :: Codecs c
    -- ^ Codecs for this column's keys and values
    }

-- | Create columns from a list of column families and codecs.
-- The lists must have the same length; families are paired with codecs in order.
mkColumns :: [cf] -> DMap k2 Codecs -> DMap k2 (Column cf)
mkColumns columnFamilies = snd . DMap.mapAccumLWithKey f columnFamilies
  where
    f (c : cfs) _ codec =
        (cfs, Column c codec)
    f [] _ _ =
        error "mkColumns: not enough column families in DB"

-- | Look up a column in the column map, failing if not found.
getColumn
    :: (GCompare k2, MonadFail f1) => k2 v -> DMap k2 f2 -> f1 (f2 v)
getColumn t columns =
    case DMap.lookup t columns of
        Just col -> pure col
        Nothing -> fail "query: column not found"

-- | Decode a value using the column's codec, failing if decode fails.
-- This should never fail if the data was written correctly.
decodeValueThrow
    :: MonadFail m => Codecs c -> ByteString -> m (ValueOf c)
decodeValueThrow codec bs =
    case preview (valueCodec codec) bs of
        Just v -> return v
        Nothing -> fail "Failed to decode value"

-- | Iterator position commands for cursor operations.
data Pos
    = PosLast
    -- ^ Move to last entry
    | PosPrev
    -- ^ Move to previous entry
    | PosAny ByteString
    -- ^ Seek to specific key
    | PosNext
    -- ^ Move to next entry
    | PosFirst
    -- ^ Move to first entry
    | PosDestroy
    -- ^ Release iterator resources

-- | Backend-agnostic iterator interface for range queries.
-- Iterators maintain a position and can move through entries.
data QueryIterator m = QueryIterator
    { step :: Pos -> m ()
    -- ^ Move the iterator to a new position
    , isValid :: m Bool
    -- ^ Check if iterator points to a valid entry
    , entry :: m (Maybe (ByteString, ByteString))
    -- ^ Get the current key-value pair (if valid)
    }

-- | Transform the monad of a 'QueryIterator'.
hoistQueryIterator
    :: (forall x. m x -> n x)
    -> QueryIterator m
    -> QueryIterator n
hoistQueryIterator nat QueryIterator{step, isValid, entry} =
    QueryIterator
        { step = nat . step
        , isValid = nat isValid
        , entry = nat entry
        }

-- | Abstract database interface supporting multiple typed columns.
-- This is the core abstraction that backends (like RocksDB) implement.
data Database m cf t op = Database
    { valueAt :: cf -> ByteString -> m (Maybe ByteString)
    -- ^ Read a value by column family and key
    , applyOps :: [op] -> m ()
    -- ^ Apply a batch of operations atomically
    , mkOperation :: cf -> ByteString -> Maybe ByteString -> op
    -- ^ Create an operation: @Just v@ for put, @Nothing@ for delete
    , newIterator :: cf -> m (QueryIterator m)
    -- ^ Create a new iterator for range queries
    , columns :: DMap t (Column cf)
    -- ^ Map of all columns in this database
    }

-- | Transform the monad of a 'Database'.
hoistDatabase
    :: Functor m
    => (forall x. m x -> n x)
    -> Database m cf t op
    -> Database n cf t op
hoistDatabase nat Database{..} =
    Database
        { valueAt = \cf k -> nat $ valueAt cf k
        , applyOps = nat . applyOps
        , mkOperation = mkOperation
        , newIterator = \cf -> nat $ hoistQueryIterator nat <$> newIterator cf
        , columns = columns
        }

-- | Create a batch operation from a key and optional value.
-- @Just v@ creates a put operation, @Nothing@ creates a delete.
mkOp
    :: Database m cf t op
    -> Column cf c
    -> KeyOf c
    -> Maybe (ValueOf c)
    -> op
mkOp
    Database{mkOperation}
    Column{family, codecs = Codecs{keyCodec, valueCodec}}
    k = mkOperation family (review keyCodec k) . fmap (review valueCodec)

-- | Convenience function to create a 'DMap' from a list of typed pairs.
mkCols :: GCompare t => [DSum t r] -> DMap t r
mkCols = DMap.fromList
