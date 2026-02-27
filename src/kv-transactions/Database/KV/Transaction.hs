{- |
Module      : Database.KV.Transaction
Description : Transactional operations on type-safe key-value databases
Copyright   : (c) Paolo Veronelli, 2024
License     : Apache-2.0

This module provides a transactional layer over the 'Database' abstraction,
enabling atomic read-modify-write operations across multiple typed columns.

= Transaction Semantics

Transactions use an optimistic approach with snapshot isolation:

1. All reads within a transaction operate on a consistent snapshot
2. Writes are buffered in per-column workspaces (read-your-writes)
3. All buffered writes are applied atomically at commit

= Speculation

Use 'runSpeculation' to execute a transaction that reads from a snapshot
and provides read-your-writes within the session, but discards all writes
at the end. Useful for computing derived results (e.g., trie roots, proofs)
without side effects.

= Concurrency Control

Use 'newRunTransaction' to create a serialized transaction runner that
ensures only one transaction executes at a time. For unguarded access
(e.g., in single-threaded contexts), use 'runTransactionUnguarded'.

= Example

@
data Cols c where
    Users :: Cols (KV UserId User)

runTx <- newRunTransaction db
runTx $ do
    mUser <- query Users userId
    case mUser of
        Just user -> insert Users userId (user { name = "New Name" })
        Nothing -> pure ()
@
-}
module Database.KV.Transaction
    ( -- * Column Types (re-exported from Database)
      Codecs (..)
    , Column (..)
    , Selector
    , KeyOf
    , ValueOf
    , KV

      -- * Transaction Context
    , Context

      -- * Transaction Monad
    , TransactionInstruction
    , Transaction
    , query
    , insert
    , delete
    , iterating
    , reset

      -- * Column Mapping
    , mapColumns

      -- * Running Transactions
    , interpretTransaction
    , RunTransaction (..)
    , newRunTransaction
    , runTransactionUnguarded
    , runSpeculation

      -- * Utilities
    , fromPairList

      -- * Re-exports
    , module Data.GADT.Compare
    , module Data.Dependent.Map
    , module Data.Dependent.Sum
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
    , buildOperation
    , decodeValueThrow
    , fromPairList
    , hoistQueryIterator
    )

{- |
Per-column workspace storing pending changes.
Maps keys to @Just value@ for inserts or @Nothing@ for deletes.
-}
newtype Workspace c = Workspace (Map (KeyOf c) (Maybe (ValueOf c)))

-- | Apply a function to the underlying map in a workspace.
overWorkspace
    :: (Map (KeyOf c) (Maybe (ValueOf c)) -> Map (KeyOf c) (Maybe (ValueOf c)))
    -> Workspace c
    -> Workspace c
overWorkspace f (Workspace ws) = Workspace (f ws)

-- | Collection of workspaces for all columns, indexed by column type.
type Workspaces t = DMap t Workspace

{- |
Transaction execution context.
Maintains workspaces for buffered writes and access to the underlying database.
-}
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

{- |
Low-level transaction instructions.
These are interpreted by 'interpretTransaction'.
-}
data TransactionInstruction m cf t op a where
    -- | Read a value from a column
    Query
        :: (GCompare t, Ord (KeyOf c))
        => t c
        -> KeyOf c
        -> TransactionInstruction m cf t op (Maybe (ValueOf c))
    -- | Buffer an insert operation
    Insert
        :: (GCompare t, Ord (KeyOf c))
        => t c
        -> KeyOf c
        -> ValueOf c
        -> TransactionInstruction m cf t op ()
    -- | Buffer a delete operation
    Delete
        :: (GCompare t, Ord (KeyOf c))
        => t c
        -> KeyOf c
        -> TransactionInstruction m cf t op ()
    -- | Run a cursor program over a column
    Iterating
        :: (GCompare t)
        => t c
        -> Cursor (Transaction m cf t op) c a
        -> TransactionInstruction m cf t op a
    -- | Clear workspace(s) - @Nothing@ clears all, @Just col@ clears one
    Reset
        :: Maybe (t c)
        -> TransactionInstruction m cf t op ()

{- |
Transaction monad for composing database operations.
Built using the operational monad pattern for easy interpretation.
-}
type Transaction m cf t op =
    ProgramT (TransactionInstruction m cf t op) (Context cf t op m)

{- |
Read a value from a column.
First checks the workspace for pending changes, then falls back to the database.
-}
query
    :: (GCompare t, Ord (KeyOf c))
    => t c
    -- ^ Column selector
    -> KeyOf c
    -- ^ Key to look up
    -> Transaction m cf t op (Maybe (ValueOf c))
query t k = singleton $ Query t k

{- |
Buffer an insert operation for the given key-value pair.
The actual write occurs when the transaction commits.
-}
insert
    :: (GCompare t, Ord (KeyOf c))
    => t c
    -- ^ Column selector
    -> KeyOf c
    -- ^ Key
    -> ValueOf c
    -- ^ Value to insert
    -> Transaction m cf t op ()
insert t k v = singleton $ Insert t k v

{- |
Buffer a delete operation for the given key.
The actual delete occurs when the transaction commits.
-}
delete
    :: (GCompare t, Ord (KeyOf c))
    => t c
    -- ^ Column selector
    -> KeyOf c
    -- ^ Key to delete
    -> Transaction m cf t op ()
delete t k = singleton $ Delete t k

{- |
Run a cursor program over a column within the transaction.
Enables range queries and iteration.
-}
iterating
    :: (GCompare t)
    => t c
    -- ^ Column selector
    -> Cursor (Transaction m cf t op) c a
    -- ^ Cursor program to execute
    -> Transaction m cf t op a
iterating t cursorProg = singleton $ Iterating t cursorProg

{- |
Clear pending changes in workspace(s).
@reset Nothing@ clears all workspaces, @reset (Just col)@ clears one column.
-}
reset :: Maybe (t c) -> Transaction m cf t op ()
reset mc = singleton $ Reset mc

{- |
Re-interpret a transaction from one column type to
another, given an injection from the original to the
target type. This enables composing transactions from
libraries that define their own column GADTs into a
single transaction over a unified type.

@
data CsmtCols c where
    Nodes :: CsmtCols (KV NodeKey Node)

data AllCols c where
    Csmt :: CsmtCols c -> AllCols c

liftCsmt
    :: Transaction m cf CsmtCols op a
    -> Transaction m cf AllCols op a
liftCsmt = mapColumns Csmt
@

__Restriction:__ The transaction must be built from
the standard combinators ('query', 'insert', 'delete',
'iterating', 'reset') and monadic sequencing.
Transactions that use 'lift' to embed raw 'Context'
effects are not supported.
-}
mapColumns
    :: forall m cf t t' op a
     . (Monad m, GCompare t')
    => (forall c. t c -> t' c)
    -- ^ Injection from original column type
    -> Transaction m cf t op a
    -- ^ Transaction in original column type
    -> Transaction m cf t' op a
mapColumns f = goTx
  where
    goTx
        :: forall b
         . Transaction m cf t op b
        -> Transaction m cf t' op b
    goTx prog = do
        v <- lift $ unsafeViewTx prog
        case v of
            Return a -> return a
            instr :>>= k -> case instr of
                Query t key -> do
                    r <- query (f t) key
                    goTx (k r)
                Insert t key val -> do
                    insert (f t) key val
                    goTx (k ())
                Delete t key -> do
                    delete (f t) key
                    goTx (k ())
                Iterating t cur -> do
                    r <-
                        iterating
                            (f t)
                            (goCur cur)
                    goTx (k r)
                Reset mc -> do
                    reset (fmap f mc)
                    goTx (k ())

    unsafeViewTx
        :: forall b
         . Transaction m cf t op b
        -> Context
            cf
            t'
            op
            m
            ( ProgramViewT
                ( TransactionInstruction
                    m
                    cf
                    t
                    op
                )
                (Context cf t op m)
                b
            )
    unsafeViewTx prog =
        Context
            $ StateT
            $ \s' ->
                ReaderT $ \_ -> do
                    (v, _) <-
                        runReaderT
                            ( runStateT
                                ( unContext
                                    (viewT prog)
                                )
                                DMap.empty
                            )
                            dummyDB
                    pure (v, s')

    dummyDB :: Database m cf t op
    dummyDB =
        Database
            { valueAt =
                \_ _ -> error msg
            , applyOps =
                \_ -> error msg
            , mkOperation =
                \_ _ _ -> error msg
            , newIterator =
                \_ -> error msg
            , columns = DMap.empty
            , withSnapshot =
                \_ -> error msg
            }
      where
        msg =
            "mapColumns: transaction"
                <> " uses lift"

    goCur
        :: forall c b
         . Cursor
            (Transaction m cf t op)
            c
            b
        -> Cursor
            (Transaction m cf t' op)
            c
            b
    goCur prog = do
        v <- lift $ goTx $ viewT prog
        case v of
            Return a -> return a
            instr :>>= k -> do
                r <- singleton instr
                goCur (k r)

{- |
Interpret a query instruction.
Checks workspace first, then reads from database.
-}
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

-- | Buffer an insert in the workspace.
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

-- | Buffer a delete in the workspace.
interpretDelete
    :: (GCompare t, Ord (KeyOf c), Monad m)
    => t c
    -> KeyOf c
    -> Context cf t op m ()
interpretDelete t k =
    Context
        $ modify
        $ DMap.adjust (overWorkspace (Map.insert k Nothing)) t

-- | Execute a cursor program within the transaction context.
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

-- | Clear workspace(s) for the given column(s).
interpretReset
    :: (Monad m, GCompare t) => Maybe (t c) -> Context cf t op m ()
interpretReset mc =
    Context $ modify $ case mc of
        Just t ->
            DMap.adjust (const (Workspace Map.empty)) t
        Nothing -> DMap.map (const (Workspace Map.empty))

{- |
Interpret a transaction program in the execution context.
Recursively processes instructions until the program completes.
-}
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

{- |
Run a transaction without concurrency control.
Executes the transaction and applies all buffered operations atomically.
All reads within the transaction see a consistent snapshot.

Use this only when you can guarantee single-threaded access,
or wrap with your own locking mechanism.
-}
runTransactionUnguarded
    :: forall m t cf op b
     . (GCompare t, MonadFail m)
    => Database m cf t op
    -- ^ Database to run against
    -> Transaction m cf t op b
    -- ^ Transaction to execute
    -> m b
runTransactionUnguarded db@Database{columns, applyOps} tx =
    withSnapshot db $ \snapDB -> do
        (result, workspaces) <-
            executeTransaction snapDB columns tx
        ops <- workspaceToOps db columns workspaces
        applyOps ops
        pure result

{- |
Run a transaction speculatively without concurrency control.
Reads from a consistent snapshot, buffers writes in the
workspace for read-your-writes, but discards all writes
at the end. No mutations are applied to the database.

Use this for computing results (e.g., trie roots, proofs)
without side effects.
-}
runSpeculation
    :: forall m t cf op b
     . (GCompare t, MonadFail m)
    => Database m cf t op
    -- ^ Database to run against
    -> Transaction m cf t op b
    -- ^ Transaction to execute
    -> m b
runSpeculation db@Database{columns} tx =
    withSnapshot db $ \snapDB ->
        fst <$> executeTransaction snapDB columns tx

{- | Run a transaction program against a database,
returning the result and final workspaces.
-}
executeTransaction
    :: (GCompare t, MonadFail m)
    => Database m cf t op
    -- ^ Database for reads (typically a snapshot)
    -> DMap t (Column cf)
    -- ^ Column definitions
    -> Transaction m cf t op b
    -> m (b, Workspaces t)
executeTransaction snapDB columns tx = do
    let emptyWorkspaces =
            DMap.map
                (const (Workspace Map.empty))
                columns
    runReaderT
        ( runStateT
            ( unContext
                $ interpretTransaction tx
            )
            emptyWorkspaces
        )
        snapDB

{- | Convert workspaces to a flat list of batch
operations.
-}
workspaceToOps
    :: (GCompare t, MonadFail m)
    => Database m cf t op
    -> DMap t (Column cf)
    -> Workspaces t
    -> m [op]
workspaceToOps db columns workspaces =
    concat <$> mapM toBatchOps (DMap.toList workspaces)
  where
    toBatchOps (sel :=> Workspace ws) =
        case DMap.lookup sel columns of
            Just column ->
                pure
                    $ uncurry
                        (buildOperation db column)
                        <$> Map.toList ws
            Nothing ->
                fail
                    "runTransaction: column not found"

{- |
Handle for running serialized transactions.
Ensures only one transaction executes at a time using a mutex.
-}
newtype RunTransaction m cf t op = RunTransaction
    { runTransaction :: forall a. Transaction m cf t op a -> m a
    -- ^ Execute a transaction with serialization guarantee
    }

{- |
Create a new serialized transaction runner.
Uses an MVar to ensure mutual exclusion between transactions.

@
runner <- newRunTransaction db
-- These transactions will not run concurrently:
forkIO $ runTransaction runner tx1
forkIO $ runTransaction runner tx2
@
-}
newRunTransaction
    :: (MonadIO m, MonadIO n, MonadMask n, GCompare t, MonadFail n)
    => Database n cf t op
    -- ^ Database to run transactions against
    -> m (RunTransaction n cf t op)
newRunTransaction db = do
    lock <- liftIO $ newMVar ()
    pure $ RunTransaction $ \tx -> do
        free <- liftIO $ takeMVar lock
        runTransactionUnguarded db tx `finally` liftIO (putMVar lock free)
