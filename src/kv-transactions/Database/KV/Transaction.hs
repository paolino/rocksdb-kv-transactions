module Database.KV.Transaction
    ( -- * Columns and Codecs
      Codecs (..)
    , Column (..)
    , Selector
    , KeyOf
    , ValueOf
    , KV

      -- * Transaction monadic context
    , Context

      -- * Transaction program instructions and monad
    , TransactionInstruction
    , Transaction
    , query
    , insert
    , delete
    , iterating

      -- * Transaction interpreter in the context
    , interpretTransaction
    , RunTransaction (..)
    , newRunTransaction
    , runTransactionUnguarded

      -- * Reexport
    , module Data.GADT.Compare
    , module Data.Dependent.Map
    , module Data.Dependent.Sum
    , mkCols
    , reset
    )
where

import Control.Concurrent (newMVar, putMVar, takeMVar)
import Control.Lens (review)
import Control.Monad.Catch (MonadMask, finally)
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Operational
    ( ProgramT
    , ProgramViewT (..)
    , singleton
    , viewT
    )
import Control.Monad.Trans.Class (MonadTrans (..))
import Control.Monad.Trans.Reader (ReaderT (..), ask)
import Control.Monad.Trans.State.Strict
    ( StateT (..)
    , get
    , modify
    )
import Data.Dependent.Map (DMap, fromList)
import Data.Dependent.Map qualified as DMap
import Data.Dependent.Sum (DSum ((:=>)))
import Data.GADT.Compare (GCompare (..), GEq (..), GOrdering (..))
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Database.KV.Cursor (Cursor, interpretCursor)
import Database.KV.Database
    ( Codecs (..)
    , Column (..)
    , Database (..)
    , KV
    , KeyOf
    , Selector
    , ValueOf
    , decodeValueThrow
    , hoistQueryIterator
    , mkCols
    , mkOp
    )

-- | Workspace for a single column, this iis where the changes are stored
newtype Workspace c = Workspace (Map (KeyOf c) (Maybe (ValueOf c)))

-- modify workspace
overWorkspace
    :: (Map (KeyOf c) (Maybe (ValueOf c)) -> Map (KeyOf c) (Maybe (ValueOf c)))
    -> Workspace c
    -> Workspace c
overWorkspace f (Workspace ws) = Workspace (f ws)

-- | All workspaces for all columns
type Workspaces t = DMap t Workspace

-- | Monad that read the DB before the transaction and modifies the workspaces
newtype Context cf t op m a = Context
    { unContext
        :: StateT (Workspaces t) (ReaderT (Database m cf t op) m) a
    }
    deriving newtype
        ( Functor
        , Applicative
        , Monad
        , MonadFail
        , MonadIO
        )

instance MonadTrans (Context cf t op) where
    lift f = Context $ do
        lift . lift $ f

-- | Instructions for the transaction
data TransactionInstruction m cf t op a where
    Query
        :: (GCompare t, Ord (KeyOf c))
        => t c
        -> KeyOf c
        -> TransactionInstruction m cf t op (Maybe (ValueOf c))
    Insert
        :: (GCompare t, Ord (KeyOf c))
        => t c
        -> KeyOf c
        -> ValueOf c
        -> TransactionInstruction m cf t op ()
    Delete
        :: (GCompare t, Ord (KeyOf c))
        => t c
        -> KeyOf c
        -> TransactionInstruction m cf t op ()
    Iterating
        :: (GCompare t)
        => t c
        -> Cursor (Transaction m cf t op) c a
        -> TransactionInstruction m cf t op a
    Reset
        :: Maybe (t c)
        -> TransactionInstruction m cf t op ()

-- | Transaction operational monad
type Transaction m cf t op =
    ProgramT (TransactionInstruction m cf t op) (Context cf t op m)

-- | Query a value for the given key in the given column
query
    :: (GCompare t, Ord (KeyOf c))
    => t c
    -- ^ column
    -> KeyOf c
    -- ^ key
    -> Transaction m cf t op (Maybe (ValueOf c))
query t k = singleton $ Query t k

-- | Insert a value for the given key in the given column
insert
    :: (GCompare t, Ord (KeyOf c))
    => t c
    -- ^ column
    -> KeyOf c
    -- ^ key
    -> ValueOf c
    -- ^ value
    -> Transaction m cf t op ()
insert t k v = singleton $ Insert t k v

-- | Delete a value for the given key in the given column
delete
    :: (GCompare t, Ord (KeyOf c))
    => t c
    -- ^ column
    -> KeyOf c
    -- ^ key
    -> Transaction m cf t op ()
delete t k = singleton $ Delete t k

iterating
    :: (GCompare t)
    => t c
    -- ^ column
    -> Cursor (Transaction m cf t op) c a
    -- ^ cursor operations
    -> Transaction m cf t op a
iterating t cursorProg = singleton $ Iterating t cursorProg

reset :: Maybe (t c) -> Transaction m cf t op ()
reset mc = singleton $ Reset mc

interpretQuery
    :: (GCompare t, Ord (KeyOf f), MonadFail m)
    => t f
    -> KeyOf f
    -> Context cf t op m (Maybe (ValueOf f))
interpretQuery t k = Context $ do
    workspaces <- get
    case DMap.lookup t workspaces of
        Just (Workspace ws) -> maybe fetchFromDB pure $ Map.lookup k ws
        Nothing -> fetchFromDB
  where
    fetchFromDB = do
        Database{valueAt, columns} <- lift ask
        Column{family = cf, codecs = codecs} <-
            case DMap.lookup t columns of
                Just col -> pure col
                Nothing -> fail "query: column not found"
        rvalue <- lift $ lift $ valueAt cf $ review (keyCodec codecs) k
        mapM (decodeValueThrow codecs) rvalue

interpretInsert
    :: (GCompare t, Ord (KeyOf c), Monad m)
    => t c
    -> KeyOf c
    -> ValueOf c
    -> Context cf t op m ()
interpretInsert t k v =
    Context
        $ modify
        $ DMap.adjust (overWorkspace (Map.insert k (Just v))) t

interpretDelete
    :: (GCompare t, Ord (KeyOf c), Monad m)
    => t c
    -> KeyOf c
    -> Context cf t op m ()
interpretDelete t k =
    Context
        $ modify
        $ DMap.adjust (overWorkspace (Map.insert k Nothing)) t

interpretIterating
    :: (GCompare t, MonadFail m)
    => t c
    -> Cursor (Transaction m cf t op) c a
    -> Context cf t op m a
interpretIterating t cursorProg = Context $ do
    Database{newIterator, columns} <- lift ask
    column <-
        case DMap.lookup t columns of
            Just col -> pure col
            Nothing -> fail "interpretIterating: column not found"
    qi <- lift $ lift $ newIterator (family column)
    unContext
        $ interpretTransaction
        $ interpretCursor
            (hoistQueryIterator (lift . lift) qi)
            column
            cursorProg

interpretReset
    :: (Monad m, GCompare t) => Maybe (t c) -> Context cf t op m ()
interpretReset mc =
    Context $ modify $ case mc of
        Just t ->
            DMap.adjust (const (Workspace Map.empty)) t
        Nothing -> DMap.map (const (Workspace Map.empty))

-- | Interpret the transaction as a value in the Context monad
interpretTransaction
    :: (GCompare t, MonadFail m)
    => Transaction m cf t op a
    -> Context cf t op m a
interpretTransaction prog = do
    v <- viewT prog
    case v of
        Return a -> pure a
        instr :>>= k -> case instr of
            Query t key -> do
                r <- interpretQuery t key
                interpretTransaction (k r)
            Insert t key value -> do
                interpretInsert t key value
                interpretTransaction (k ())
            Delete t key -> do
                interpretDelete t key
                interpretTransaction (k ())
            Iterating t cursorProg -> do
                r <- interpretIterating t cursorProg
                interpretTransaction (k r)
            Reset mc -> do
                interpretReset mc
                interpretTransaction (k ())

-- | Run a transaction in the given database context
runTransactionUnguarded
    :: forall m t cf op b
     . (GCompare t, MonadFail m)
    => Database m cf t op
    -> Transaction m cf t op b
    -> m b
runTransactionUnguarded db@Database{columns, applyOps} tx = do
    let emptyWorkspaces = DMap.map (const (Workspace Map.empty)) columns
    (result, workspaces) <-
        runReaderT
            (runStateT (unContext $ interpretTransaction tx) emptyWorkspaces)
            db
    ops <- mapM toBatchOps $ DMap.toList workspaces
    applyOps $ concat ops
    pure result
  where
    toBatchOps :: DSum t Workspace -> m [op]
    toBatchOps (sel :=> Workspace ws) =
        case DMap.lookup sel columns of
            Just column -> pure $ uncurry (mkOp db column) <$> Map.toList ws
            Nothing -> fail "runTransaction: column not found"

-- | A runner for transactions that ensures only one transaction runs at a time
newtype RunTransaction m cf t op = RunTransaction
    { runTransaction :: forall a. Transaction m cf t op a -> m a
    }

-- | Create a new RunTransaction that serializes transactions on the given database
newRunTransaction
    :: (MonadIO m, MonadIO n, MonadMask n, GCompare t, MonadFail n)
    => Database n cf t op
    -> m (RunTransaction n cf t op)
newRunTransaction db = do
    lock <- liftIO $ newMVar ()
    pure $ RunTransaction $ \tx -> do
        free <- liftIO $ takeMVar lock
        runTransactionUnguarded db tx `finally` liftIO (putMVar lock free)
