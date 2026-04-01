# rocksdb-kv-transactions Constitution

## Core Principles

### I. Backend-Agnostic Abstractions

The `kv-transactions` sublibrary defines all core types and logic without depending on any storage backend. The `Database` record, `Transaction` monad, `Query`, and `Cursor` modules must remain free of RocksDB (or any other backend) imports. Backend implementations live in separate libraries that populate the `Database` record.

### II. Type Safety via Dependent Types

Column families are indexed by GADTs (`t (KV k v)`), keys and values are encoded/decoded via `Prism'` codecs, and `DMap` enforces correct types per column at compile time. Never bypass the type-safe column layer with raw `ByteString` operations in public API.

### III. Minimal Surface Area

Each module exports only what consumers need. New functionality must justify its addition — no speculative features, no convenience wrappers that duplicate existing combinators. The library is small and focused: type-safe KV with transactions, cursors, and snapshots.

### IV. Test-Driven with Real Backends

Tests must exercise the actual `Database` contract, not mock internals. Each backend (RocksDB, in-memory) must pass the same behavioral test suite. Tests use temporary resources (temp directories, ephemeral state) and clean up after themselves.

### V. Pure Logic, Impure Edges

Transaction buffering, workspace merging, and codec application are pure. IO only appears at the `Database` record boundary (read/write/iterate). This separation enables testing pure logic without any backend and keeps the codebase easy to reason about.

## Architecture Constraints

- **Two-tier library structure**: `kv-transactions` (sublibrary, no backend deps) + backend libraries (e.g., `rocksdb-kv-transactions`). New backends are new libraries, not modifications to existing ones.
- **The `Database` record is the extension point**: backends implement `Database` by filling in `valueAt`, `applyOps`, `mkOperation`, `newIterator`, `columns`, and `withSnapshot`. No typeclasses — just records.
- **Codecs via Prisms**: All serialization goes through `Codecs` (`keyCodec`, `valueCodec`). No ad-hoc `Serialize`/`FromJSON` instances in the core.
- **Operational monad for transactions**: `Transaction` uses the `operational` package. Transaction interpreters (`runTransaction`, `runSpeculation`) are separate from transaction descriptions.

## Quality Gates

- `just ci` must pass locally before any push (build, test, format-check, hlint)
- Fourmolu formatting (run up to 3 times for convergence), cabal-fmt, nixfmt
- HLint clean
- All tests pass with `--test-show-details=direct`
- Haddock on all exported symbols

## Governance

This constitution defines the architectural boundaries of the project. Changes to the two-tier structure, the `Database` record interface, or the codec mechanism require explicit discussion and approval. Implementation details within these boundaries are flexible.

**Version**: 1.0.0 | **Ratified**: 2026-04-01
