module Database.KV.Database
    ( -- * Columns and Codecs
      Codecs (..)
    , decodeValueThrow
    , Column (..)
    , getColumn
    , mkColumns

      -- * KV
    , Selector
    , KeyOf
    , ValueOf
    , KV
    , mkOp

      -- * Transaction monadic context
    , Database (..)
    , hoistDatabase
    , Pos (..)
    , QueryIterator (..)
    , hoistQueryIterator

      -- * Reexport
    , module Data.GADT.Compare
    , module Data.Dependent.Map
    , module Data.Dependent.Sum
    , mkCols
    )
where

import Control.Lens (Prism', preview, review)
import Data.ByteString (ByteString)
import Data.Dependent.Map (DMap, fromList)
import Data.Dependent.Map qualified as DMap
import Data.Dependent.Sum (DSum ((:=>)))
import Data.GADT.Compare (GCompare (..), GEq (..), GOrdering (..))

-- | Column definition for key-value pairs. This is a placeholder for the 2 types k v.
-- It is needed as a single type index.
data KV k v

-- | Selector type for columns
type Selector t k v = t (KV k v)

-- | Project key type from column
type family KeyOf c where
    KeyOf (KV k v) = k

-- | Project value type from column
type family ValueOf c where
    ValueOf (KV k v) = v

-- | Codecs for encoding/decoding keys and values
data Codecs c = Codecs
    { keyCodec :: Prism' ByteString (KeyOf c)
    , valueCodec :: Prism' ByteString (ValueOf c)
    }

-- | Column definition
data Column cf c = Column
    { family :: cf
    , codecs :: Codecs c
    }

mkColumns :: [cf] -> DMap k2 Codecs -> DMap k2 (Column cf)
mkColumns columnFamilies = snd . DMap.mapAccumLWithKey f columnFamilies
  where
    f (c : cfs) _ codec =
        (cfs, Column c codec)
    f [] _ _ =
        error "mkColumns: not enough column families in DB"

getColumn
    :: (GCompare k2, MonadFail f1) => k2 v -> DMap k2 f2 -> f1 (f2 v)
getColumn t columns =
    case DMap.lookup t columns of
        Just col -> pure col
        Nothing -> fail "query: column not found"

-- throw should never happen
decodeValueThrow
    :: MonadFail m => Codecs c -> ByteString -> m (ValueOf c)
decodeValueThrow codec bs =
    case preview (valueCodec codec) bs of
        Just v -> return v
        Nothing -> fail "Failed to decode value"

data Pos
    = PosLast
    | PosPrev
    | PosAny ByteString
    | PosNext
    | PosFirst
    | PosDestroy

data QueryIterator m = QueryIterator
    { step :: Pos -> m ()
    , isValid :: m Bool
    , entry :: m (Maybe (ByteString, ByteString))
    }

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

-- | DB 'interface
data Database m cf t op = Database
    { valueAt :: cf -> ByteString -> m (Maybe ByteString)
    , applyOps :: [op] -> m ()
    , mkOperation :: cf -> ByteString -> Maybe ByteString -> op
    , newIterator :: cf -> m (QueryIterator m)
    , columns :: DMap t (Column cf)
    }

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

mkCols :: GCompare t => [DSum t r] -> DMap t r
mkCols = DMap.fromList
