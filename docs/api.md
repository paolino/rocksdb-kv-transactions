# API Reference

## Database.KV.Transaction

### Core Operations

#### query

```haskell
query :: (GCompare t, Ord (KeyOf c))
      => t c           -- Column selector
      -> KeyOf c       -- Key to look up
      -> Transaction m cf t op (Maybe (ValueOf c))
```

Read a value from a column. First checks the workspace for pending changes, then falls back to the database.

#### insert

```haskell
insert :: (GCompare t, Ord (KeyOf c))
       => t c          -- Column selector
       -> KeyOf c      -- Key
       -> ValueOf c    -- Value to insert
       -> Transaction m cf t op ()
```

Buffer an insert operation. The actual write occurs when the transaction commits.

#### delete

```haskell
delete :: (GCompare t, Ord (KeyOf c))
       => t c          -- Column selector
       -> KeyOf c      -- Key to delete
       -> Transaction m cf t op ()
```

Buffer a delete operation. The actual delete occurs when the transaction commits.

### Cursor Operations

#### iterating

```haskell
iterating :: GCompare t
          => t c                                    -- Column selector
          -> Cursor (Transaction m cf t op) c a     -- Cursor program
          -> Transaction m cf t op a
```

Run a cursor program over a column within the transaction.

### Running Transactions

#### runTransactionUnguarded

```haskell
runTransactionUnguarded :: (GCompare t, MonadFail m)
                        => Database m cf t op
                        -> Transaction m cf t op b
                        -> m b
```

Run a transaction without concurrency control. Use only in single-threaded contexts.

#### newRunTransaction

```haskell
newRunTransaction :: (MonadIO m, MonadIO n, MonadMask n, GCompare t, MonadFail n)
                  => Database n cf t op
                  -> m (RunTransaction n cf t op)
```

Create a serialized transaction runner using mutex synchronization.

---

## Database.KV.Cursor

### Navigation

#### firstEntry

```haskell
firstEntry :: Cursor m c (Maybe (Entry c))
```

Move to the first entry (lexicographically smallest key).

#### lastEntry

```haskell
lastEntry :: Cursor m c (Maybe (Entry c))
```

Move to the last entry (lexicographically largest key).

#### nextEntry

```haskell
nextEntry :: Cursor m c (Maybe (Entry c))
```

Move to the next entry. Returns `Nothing` if at end.

#### prevEntry

```haskell
prevEntry :: Cursor m c (Maybe (Entry c))
```

Move to the previous entry. Returns `Nothing` if at beginning.

#### seekKey

```haskell
seekKey :: KeyOf c -> Cursor m c (Maybe (Entry c))
```

Seek to a specific key. If the exact key is not found, positions at the next key.

### Types

#### Entry

```haskell
data Entry c = Entry
    { entryKey   :: KeyOf c
    , entryValue :: ValueOf c
    }
```

A key-value pair with types determined by the column.

---

## Database.KV.Database

### Types

#### KV

```haskell
data KV k v
```

Phantom type representing a key-value pair in a collection.

#### Codecs

```haskell
data Codecs c = Codecs
    { keyCodec   :: Prism' ByteString (KeyOf c)
    , valueCodec :: Prism' ByteString (ValueOf c)
    }
```

Codecs for encoding and decoding keys and values.

#### Column

```haskell
data Column cf c = Column
    { family :: cf
    , codecs :: Codecs c
    }
```

A column definition with backend-specific identifier and codecs.

#### Database

```haskell
data Database m cf t op = Database
    { valueAt     :: cf -> ByteString -> m (Maybe ByteString)
    , applyOps    :: [op] -> m ()
    , mkOperation :: cf -> ByteString -> Maybe ByteString -> op
    , newIterator :: cf -> m (QueryIterator m)
    , columns     :: DMap t (Column cf)
    }
```

Backend-agnostic database interface.

### Functions

#### mkColumns

```haskell
mkColumns :: [cf] -> DMap k Codecs -> DMap k (Column cf)
```

Create columns from a list of column families and codecs.

#### fromPairList

```haskell
fromPairList :: GCompare t => [DSum t r] -> DMap t r
```

Create a DMap from a list of typed pairs.

---

## Database.KV.RocksDB

### Functions

#### mkRocksDBDatabase

```haskell
mkRocksDBDatabase :: DB -> DMap t (Column ColumnFamily) -> Database IO ColumnFamily t BatchOp
```

Create a RocksDB-backed database from a DB handle and column map.
