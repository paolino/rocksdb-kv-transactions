# RocksDB KV Transactions

Type-safe key-value transactions backed by RocksDB.

## Overview

This library provides a transactional layer over RocksDB with:

- **Type-safe columns** - Each column has its own key and value types, enforced at compile time
- **Atomic transactions** - Buffered writes are applied atomically on commit
- **Cursor iteration** - Range queries with forward/backward navigation
- **Serialization via Prisms** - Flexible encoding/decoding using lens prisms

## Installation

Add to your `build-depends`:

```cabal
build-depends:
    rocksdb-kv-transactions
    rocksdb-kv-transactions:kv-transactions
```

## Quick Start

```haskell
{-# LANGUAGE GADTs #-}

import Data.ByteString (ByteString)
import Data.Type.Equality ((:~:)(..))
import Database.KV.RocksDB (mkRocksDBDatabase)
import Database.KV.Transaction
import Database.RocksDB

-- Define your columns as a GADT
data Columns c where
    Users :: Columns (KV ByteString ByteString)
    Posts :: Columns (KV ByteString ByteString)

-- Required instances for DMap lookup
instance GCompare Columns where
    gcompare Users Users = GEQ
    gcompare Posts Posts = GEQ
    gcompare Users Posts = GLT
    gcompare Posts Users = GGT

instance GEq Columns where
    geq Users Users = Just Refl
    geq Posts Posts = Just Refl
    geq _ _ = Nothing

-- Define codecs (identity for ByteString)
codecs :: DMap Columns Codecs
codecs = fromPairList
    [ Users :=> Codecs id id
    , Posts :=> Codecs id id
    ]

main :: IO ()
main = do
    -- Open database with column families
    withDBCF "mydb" cfg [(\"users\", cfg), (\"posts\", cfg)] $ \db -> do
        let database = mkRocksDBDatabase db (mkColumns (columnFamilies db) codecs)

        -- Run transactions
        runTransactionUnguarded database $ do
            insert Users "user1" "Alice"
            insert Users "user2" "Bob"

        -- Query data
        result <- runTransactionUnguarded database $ do
            query Users "user1"

        print result  -- Just "Alice"
  where
    cfg = def { createIfMissing = True }
```

## Modules

### kv-transactions (sublibrary)

| Module | Description |
|--------|-------------|
| `Database.KV.Transaction` | Core transaction monad with `query`, `insert`, `delete` |
| `Database.KV.Database` | Abstract database interface and column definitions |
| `Database.KV.Query` | Read-only queries (no write buffering) |
| `Database.KV.Cursor` | Iterator-based navigation (`firstEntry`, `nextEntry`, `seekKey`) |

### rocksdb-kv-transactions (main library)

| Module | Description |
|--------|-------------|
| `Database.KV.RocksDB` | RocksDB backend implementation |

## Development

```bash
# Enter nix shell
nix develop

# Build
just build

# Run tests
just test

# Format code
just format

# Full CI
just ci
```

## License

Apache-2.0
