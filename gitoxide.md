# Gitoxide Submodule Nix Integration Plan

## Problem Encountered

During the attempt to integrate Nix with the `gitoxide` submodule, a limitation of the current environment was encountered: the AI agent is restricted to operating within its initial workspace directory (`/data/data/com.termux.nix/files/home/pick-up-nix2/vendor/rust/cargo2nix/submodules/cargo`). This prevents direct creation or modification of files within the `gitoxide` submodule directory (`/data/data/com.termux.nix/files/home/pick-up-nix2/vendor/rust/cargo2nix/submodules/gitoxide`).

## Proposed `flake.nix` for `gitoxide`

Below is the `flake.nix` content designed for the `gitoxide` submodule, incorporating the Nix setup previously applied to the `cargo` submodule.

```nix
{
  inputs = {
    nixpkgs.url = "github:meta-introspector/nixpkgs?ref=feature/CRQ-016-nixify";
    flake-utils.url = "github:meta-introspector/flake-utils?ref=feature/CRQ-016-nixify";
    cargo2nix.url = "github:cargo2nix/cargo2nix/release-0.12";
    rust-overlay.url = "github:meta-introspector/rust-overlay?ref=feature/CRQ-016-nixify";
  };

  outputs = inputs: with inputs;
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ cargo2nix.overlays.default rust-overlay.overlays.default ];
            config = {
              permittedInsecurePackages = [ "openssl-1.1.1w" ];
            };
          };

          myRustc = pkgs.rust-bin.nightly."2025-09-16".default;

          rustPkgs = pkgs.rustBuilder.makePackageSet {
            packageFun = import ./Cargo.nix;
            rustToolchain = myRustc;
            # rootFeatures = [ ... ]; # Add specific features if needed
            # packageOverrides = pkgs: [ ... ]; # Add specific overrides if needed
          };

          gitoxideCrate = rustPkgs.workspace.gitoxide { };

          workspaceShell = pkgs.mkShell {
            packages = [ pkgs.statix pkgs.openssl_1_1.dev ];
            shellHook = ''
              export PKG_CONFIG_PATH=${pkgs.openssl_1_1.dev}/lib/pkgconfig:$PKG_CONFIG_PATH
              export PATH=${myRustc}/bin:${gitoxideCrate}/bin:$PATH
            '';
          };

        in
        rec {
          devShells = {
            default = workspaceShell;
          };

          packages = rec {
            inherit gitoxideCrate;
            workspaceCrates = rustPkgs.workspace;
            default = gitoxideCrate;
          };

          apps = rec {
            gitoxide = { type = "app"; program = "${packages.gitoxideCrate}/bin/gitoxide"; };
            default = gitoxide;
          };
        }
      );
}
```

## Instructions for Manual Setup

To proceed with the Nix integration for `gitoxide`:

1.  **Navigate to the `gitoxide` submodule directory:**
    ```bash
    cd /data/data/com.termux.nix/files/home/pick-up-nix2/vendor/rust/cargo2nix/submodules/gitoxide
    ```

2.  **Create `flake.nix`:**
    Create a new file named `flake.nix` in this directory and paste the `Proposed flake.nix for gitoxide` content from above into it.

3.  **Generate `Cargo.nix`:**
    Run `cargo2nix` to generate the `Cargo.nix` file for `gitoxide`:
    ```bash
    ../../target/cargo2nix --overwrite -o Cargo.nix
    ```
    *(Note: The path to `cargo2nix` assumes you are in the `gitoxide` directory and `cargo2nix` is built in `../../target/cargo2nix`)*

4.  **Build the `gitoxide` project:**
    You can then build the project using Nix:
    ```bash
    nix build
    ```
    Or enter a development shell:
    ```bash
    nix develop
    ```
