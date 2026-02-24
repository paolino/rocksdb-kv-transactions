{
  description = "RocksDB backend for key-value transactions";
  nixConfig = {
    extra-substituters =
      [ "https://cache.iog.io" "https://paolino.cachix.org" ];
    extra-trusted-public-keys = [
      "hydra.iohk.io:f/Ea+s+dFdN+3Y/G+FDgSq+a5NEWhJGzdjvKNGv0/EQ="
      "paolino.cachix.org-1:iippTJnsYwrUfu4NJVnTdV+BNXS0bL0sfnnDPhw7WpI="
    ];
  };
  inputs = {
    haskellNix.url = "github:input-output-hk/haskell.nix";
    nixpkgs = { follows = "haskellNix/nixpkgs-unstable"; };
    flake-parts.url = "github:hercules-ci/flake-parts";
    flake-utils.url = "github:hamishmack/flake-utils/hkm/nested-hydraJobs";
    mkdocs.url = "github:paolino/dev-assets?dir=mkdocs";
  };

  outputs = inputs@{ self, nixpkgs, flake-utils, haskellNix, mkdocs, ... }:
    let
      lib = nixpkgs.lib;

      perSystem = system:
        let
          pkgs = import nixpkgs {
            overlays = [ haskellNix.overlay ];
            inherit system;
          };
          project = import ./nix/project.nix {
            indexState = "2025-08-07T00:00:00Z";
            inherit pkgs;
            mkdocs = mkdocs.packages.${system};
          };

        in {
          packages = project.packages // {
            default = project.packages.rocksdb-kv-transactions;
          };
          inherit (project) devShells;
        };

    in flake-utils.lib.eachSystem [ "x86_64-linux" "aarch64-darwin" ] perSystem;
}
