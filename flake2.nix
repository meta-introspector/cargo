{
  description = "Development shell with specific Rust toolchain";

  inputs = {
    #nixpkgs.url = "nixpkgs/nixos-unstable";
    nixpkgs.url = "github:meta-introspector/nixpkgs?ref=feature/CRQ-016-nixify";
  };

  outputs = { self, nixpkgs }:
    let
      inherit (nixpkgs) lib;
      system = "aarch64-linux";
      pkgs = import nixpkgs {
        inherit system;
      };
      myRustcToolchain = "/nix/store/7vzj2mc9rj6jlsx251822cxy683hq7vd-rustc-1.92.0-nightly-2025-09-16-aarch64-unknown-linux-gnu";
    in
    {
      devShells.${system}.default = pkgs.mkShell {
        buildInputs = [
          myRustcToolchain
        ];

        shellHook = ''
          echo "Entering development shell with rustc from: ${myRustcToolchain}"
          rustc --version
        '';
      };
    };
}
