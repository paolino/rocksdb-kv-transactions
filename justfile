# shellcheck shell=bash

set unstable := true

# Format all source files
format:
    #!/usr/bin/env bash
    set -euo pipefail
    for i in {1..3}; do
        fourmolu -i src test
    done
    cabal-fmt -i rocksdb-kv-transactions.cabal
    nixfmt ./*.nix
    nixfmt nix/*.nix

# Check formatting without modifying files
format-check:
    #!/usr/bin/env bash
    set -euo pipefail
    fourmolu -m check src test
    cabal-fmt -c rocksdb-kv-transactions.cabal

# Run hlint
hlint:
    #!/usr/bin/env bash
    hlint src test

# Build all components
build:
    #!/usr/bin/env bash
    cabal build all --enable-tests

# Run unit tests with optional match pattern
test match="":
    #!/usr/bin/env bash
    if [[ '{{ match }}' == "" ]]; then
        cabal test unit-tests --test-show-details=direct
    else
        cabal test unit-tests \
            --test-show-details=direct \
            --test-option=--match \
            --test-option="{{ match }}"
    fi

# Run tests via nix
test-nix:
    #!/usr/bin/env bash
    nix run .#unit-tests

# Full CI pipeline
ci:
    #!/usr/bin/env bash
    set -euo pipefail
    just build
    just test
    just format-check
    just hlint

# Build with nix
nix-build:
    #!/usr/bin/env bash
    nix build

# Generate haddock documentation
docs:
    #!/usr/bin/env bash
    cabal haddock all

# Clean build artifacts
clean:
    #!/usr/bin/env bash
    cabal clean
    rm -rf result

# Watch for changes and rebuild
watch:
    #!/usr/bin/env bash
    ghcid --command="cabal repl lib:kv-transactions lib:rocksdb-kv-transactions"

# List available recipes
default:
    @just --list
