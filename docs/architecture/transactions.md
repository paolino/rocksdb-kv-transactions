# Transaction Architecture

## Operational Monad Pattern

Transactions are built using the operational monad pattern from the `operational` package. This separates the description of operations from their interpretation:

```haskell
data TransactionInstruction m cf t op a where
    Query    :: t c -> KeyOf c -> TransactionInstruction m cf t op (Maybe (ValueOf c))
    Insert   :: t c -> KeyOf c -> ValueOf c -> TransactionInstruction m cf t op ()
    Delete   :: t c -> KeyOf c -> TransactionInstruction m cf t op ()
    Iterating :: t c -> Cursor (...) c a -> TransactionInstruction m cf t op a
    Reset    :: Maybe (t c) -> TransactionInstruction m cf t op ()

type Transaction m cf t op = ProgramT (TransactionInstruction m cf t op) (Context cf t op m)
```

## Execution Model

Transactions use an optimistic approach with snapshot isolation:

```mermaid
sequenceDiagram
    participant App as Application
    participant Tx as Transaction
    participant WS as Workspace
    participant Snap as Snapshot
    participant DB as Database

    App->>Tx: runTransactionUnguarded tx
    Tx->>DB: withSnapshot
    DB-->>Snap: Create consistent snapshot
    Tx->>WS: Initialize empty workspaces
    loop For each operation
        alt Query
            Tx->>WS: Check workspace
            alt Found in workspace
                WS-->>Tx: Return cached value
            else Not in workspace
                Tx->>Snap: Read from snapshot
                Snap-->>Tx: Return value
            end
        else Insert/Delete
            Tx->>WS: Buffer operation
        end
    end
    Tx->>WS: Collect all buffered operations
    Tx->>DB: Apply atomically
    DB-->>App: Return result
```

## Snapshot Isolation

All reads within a transaction target the same consistent snapshot.
Concurrent writes to the database do not affect queries within an
in-flight transaction.

The snapshot is created at the start of `runTransactionUnguarded`
(or `runSpeculation`) via the `withSnapshot` field on `Database`.
The snapshot-backed `Database` is passed into the `ReaderT` that
`interpretTransaction` runs in, so every `interpretQuery` call
reads from the same frozen point.

## Workspace Buffering

Each column has its own workspace that buffers pending changes:

```haskell
newtype Workspace c = Workspace (Map (KeyOf c) (Maybe (ValueOf c)))

type Workspaces t = DMap t Workspace
```

- `Just value` indicates a pending insert
- `Nothing` indicates a pending delete

## Read-Your-Writes Semantics

Queries first check the workspace before hitting the database:

```haskell
interpretQuery t k = do
    workspaces <- get
    case DMap.lookup t workspaces of
        Just (Workspace ws) ->
            maybe fetchFromDB pure $ Map.lookup k ws
        Nothing -> fetchFromDB
```

This ensures you see your own uncommitted writes within a transaction.

## Concurrency Control

Two options for running transactions:

### Unguarded (Single-threaded)

```haskell
runTransactionUnguarded :: Database m cf t op -> Transaction m cf t op a -> m a
```

No synchronization - use only when you can guarantee single-threaded access.

### Serialized (Multi-threaded)

```haskell
newRunTransaction :: Database n cf t op -> m (RunTransaction n cf t op)

runner <- newRunTransaction db
forkIO $ runTransaction runner tx1
forkIO $ runTransaction runner tx2
```

Uses an MVar mutex to ensure only one transaction executes at a time.

## Atomic Commit

At the end of a transaction, all buffered operations are converted to batch operations and applied atomically:

```haskell
ops <- mapM toBatchOps $ DMap.toList workspaces
applyOps $ concat ops
```

RocksDB guarantees atomic batch writes.

## Speculation

`runSpeculation` executes a transaction against a snapshot with
read-your-writes in the workspace, but discards all writes at the
end. No mutations are applied to the database.

```mermaid
sequenceDiagram
    participant App as Application
    participant Tx as Transaction
    participant WS as Workspace
    participant Snap as Snapshot

    App->>Tx: runSpeculation tx
    Tx->>Snap: Create snapshot
    Tx->>WS: Initialize empty workspaces
    loop For each operation
        alt Query
            Tx->>WS: Check workspace
            alt Found in workspace
                WS-->>Tx: Return cached value
            else Not in workspace
                Tx->>Snap: Read from snapshot
                Snap-->>Tx: Return value
            end
        else Insert/Delete
            Tx->>WS: Buffer operation
        end
    end
    Note over WS: Workspace discarded
    Tx-->>App: Return result only
```

Use speculation for computing derived results (trie roots, proofs)
without side effects.
