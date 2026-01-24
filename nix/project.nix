{ indexState, pkgs, ... }:

let
  shell = { pkgs, ... }: {
    tools = {
      cabal = { index-state = indexState; };
      cabal-fmt = { index-state = indexState; };
      haskell-language-server = { index-state = indexState; };
      hoogle = { index-state = indexState; };
      fourmolu = { index-state = indexState; };
      hlint = { index-state = indexState; };
      implicit-hie = { index-state = indexState; };
    };
    withHoogle = true;
    buildInputs = [
      pkgs.just
      pkgs.nixfmt-classic
    ];
    shellHook = ''
      echo "Entering shell for rocksdb-kv-transactions development"
    '';
  };

  mkProject = ctx@{ lib, pkgs, ... }: {
    name = "rocksdb-kv-transactions";
    src = ./..;
    compiler-nix-name = "ghc984";
    shell = shell { inherit pkgs; };
  };

  project = pkgs.haskell-nix.cabalProject' mkProject;

in {
  devShells.default = project.shell;
  inherit project;
  packages.rocksdb-kv-transactions = project.hsPkgs.rocksdb-kv-transactions.components.library;
  packages.kv-transactions = project.hsPkgs.rocksdb-kv-transactions.components.sublibs.kv-transactions;
  packages.unit-tests = project.hsPkgs.rocksdb-kv-transactions.components.tests.unit-tests;
}
