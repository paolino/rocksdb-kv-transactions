# Cursor Architecture

## Overview

Cursors provide iterator-based access for range queries. They allow forward and backward navigation through sorted key-value entries.

## Cursor Operations

The cursor API provides five core operations:

| Operation | Description |
|-----------|-------------|
| `firstEntry` | Move to the first (smallest key) entry |
| `lastEntry` | Move to the last (largest key) entry |
| `nextEntry` | Move to the next entry |
| `prevEntry` | Move to the previous entry |
| `seekKey` | Seek to a specific key (or next if not found) |

## Operational Monad

Like transactions, cursors use the operational monad pattern:

```haskell
data CursorInstruction m c a where
    FirstEntry :: CursorInstruction m c (Maybe (Entry c))
    LastEntry  :: CursorInstruction m c (Maybe (Entry c))
    NextEntry  :: CursorInstruction m c (Maybe (Entry c))
    PrevEntry  :: CursorInstruction m c (Maybe (Entry c))
    SeekKey    :: KeyOf c -> CursorInstruction m c (Maybe (Entry c))

type Cursor m c = ProgramT (CursorInstruction m c) m
```

## Entry Type

Entries contain decoded key-value pairs:

```haskell
data Entry c = Entry
    { entryKey   :: KeyOf c
    , entryValue :: ValueOf c
    }
```

## Iterator Positions

The underlying iterator responds to position commands:

```haskell
data Pos
    = PosFirst      -- Move to first entry
    | PosLast       -- Move to last entry
    | PosNext       -- Move to next entry
    | PosPrev       -- Move to previous entry
    | PosAny BS     -- Seek to specific key
    | PosDestroy    -- Release iterator
```

## Backend Interface

The `QueryIterator` provides a backend-agnostic iterator:

```haskell
data QueryIterator m = QueryIterator
    { step    :: Pos -> m ()           -- Move to position
    , isValid :: m Bool                -- Check if pointing to valid entry
    , entry   :: m (Maybe (BS, BS))    -- Get current key-value pair
    }
```

## Example Usage

### Sequential Iteration

```haskell
runTransactionUnguarded database $ iterating Users $ do
    first <- firstEntry
    second <- nextEntry
    third <- nextEntry
    pure (first, second, third)
```

### Range Query

```haskell
runTransactionUnguarded database $ iterating Users $ do
    -- Start from "user_100"
    entry <- seekKey "user_100"
    -- Get next 10 entries
    entries <- replicateM 10 nextEntry
    pure entries
```

### Backward Iteration

```haskell
runTransactionUnguarded database $ iterating Users $ do
    last <- lastEntry
    prev1 <- prevEntry
    prev2 <- prevEntry
    pure (last, prev1, prev2)
```

## Lifecycle

Cursor operations are scoped within `iterating`:

1. Iterator is created when entering `iterating`
2. Cursor operations move the iterator position
3. Iterator is automatically destroyed when leaving `iterating`

```haskell
iterating :: t c -> Cursor (Transaction m cf t op) c a -> Transaction m cf t op a
```
