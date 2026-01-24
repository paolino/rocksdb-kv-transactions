module Database.KV.Query
    ( -- * Columns and Codecs

      -- * Querying program instructions and monad
      QueryInstruction
    , Querying
    , query
    , iterating

      -- * Querying interpreter in the context
    , interpretQuerying

      -- * Reexport
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
    , Database (Database, columns, newIterator, valueAt)
    , KeyOf
    , ValueOf
    , decodeValueThrow
    , hoistQueryIterator
    )

-- | Instructions for the transaction
data QueryInstruction m cf t op a where
    Query
        :: (GCompare t, Ord (KeyOf c))
        => t c
        -> KeyOf c
        -> QueryInstruction m cf t op (Maybe (ValueOf c))
    Iterating
        :: (GCompare t)
        => t c
        -> Cursor (Querying m cf t op) c a
        -> QueryInstruction m cf t op a

-- | Querying operational monad
type Querying m cf t op =
    ProgramT (QueryInstruction m cf t op) m

-- | Query a value for the given key in the given column
query
    :: (GCompare t, Ord (KeyOf c))
    => t c
    -- ^ column
    -> KeyOf c
    -- ^ key
    -> Querying m cf t op (Maybe (ValueOf c))
query t k = singleton $ Query t k

-- | Run a cursor program in the querying monad over the given column on a snapshot
iterating
    :: (GCompare t)
    => t c
    -- ^ column
    -> Cursor (Querying m cf t op) c a
    -- ^ cursor program
    -> Querying m cf t op a
iterating t cursorProg = singleton $ Iterating t cursorProg

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
    interpretQuerying db
        $ interpretCursor
            (hoistQueryIterator lift qi)
            column
            cursorProg

-- | Interpret the querting on the given database
-- Notice that everyt instruction is running on an indipendent snapshot of the database
-- There is currently no way to combine multiple instructions on the same snapshot.
-- Resort to using the iterator for that. i.e. querying ~ entryValue . seek
interpretQuerying
    :: (GCompare t, MonadFail m)
    => Database m cf t op
    -> Querying m cf t op a
    -> m a
interpretQuerying db prog = do
    v <- viewT prog
    case v of
        Return a -> pure a
        instr :>>= cont -> case instr of
            Query t key -> do
                r <- interpretQuery db t key
                interpretQuerying db $ cont r
            Iterating t cursorProg -> do
                r <- interpretIterating db t cursorProg
                interpretQuerying db $ cont r
