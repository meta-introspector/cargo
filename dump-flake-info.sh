#!/usr/bin/env bash

echo "--- Nix Flake Show ---"
nix flake show

SYSTEM="aarch64-linux" # Assuming this is the target system

echo -e "\n--- Evaluating devShells.${SYSTEM}.default ---"
nix eval .#devShells.${SYSTEM}.default --json

echo -e "\n--- Evaluating packages.${SYSTEM}.default ---"
nix eval .#packages.${SYSTEM}.default --json

echo -e "\n--- Evaluating packages.${SYSTEM}.cargo ---"
nix eval .#packages.${SYSTEM}.cargo --json

echo -e "\n--- Full Dump of Rust Toolchain (myRustc) ---"
MY_RUSTC_DRV=$(nix eval --raw .#myRustcOutput)
echo "Derivation path for myRustc: ${MY_RUSTC_DRV}"

echo -e "\n--- Build Instructions (nix show-derivation) ---"
nix show-derivation "${MY_RUSTC_DRV}"

echo -e "\n--- Direct References (nix-store -q --references) ---"
nix-store -q --references "${MY_RUSTC_DRV}"

echo -e "\n--- Transitive References (nix-store -q --requisites) ---"
nix-store -q --requisites "${MY_RUSTC_DRV}"

echo -e "\n--- Path Info (nix path-info -S) ---"
nix path-info -S "${MY_RUSTC_DRV}"
