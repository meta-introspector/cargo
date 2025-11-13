## Guide: Adapting the `flake.nix` Template for Other Rust Crates

This guide explains how to adapt the provided `flake.nix` template to build your own Rust libraries and applications using Nix and `cargo2nix`. This template aims to be self-contained, avoiding the need for a local `overlay/` directory, and provides a reproducible build environment for your Rust projects.

### 1. Introduction

The `flake.nix` template you now have is configured to build Rust projects using `cargo2nix`, a tool that translates your Rust project's `Cargo.lock` into a Nix expression. This setup offers:
*   **Reproducibility:** Your builds are consistent across different environments.
*   **Isolation:** Dependencies are managed by Nix, preventing conflicts with your system.
*   **Simplified Nix Setup:** No need for complex local overlays.

### 2. Prerequisites

Before you begin, ensure you have the following:
*   **Nix:** Installed and configured with [flakes enabled](https://nixos.wiki/wiki/Flakes).
*   **A Rust Project:** Your project should have a `Cargo.toml` and a `Cargo.lock` file. Ensure your `Cargo.lock` is up-to-date by running `cargo update` in your project directory.

### 3. Step 1: Generate `Cargo.nix`

`cargo2nix` is essential for this setup. It generates a `Cargo.nix` file that describes your Rust project's dependency graph in a format Nix can understand.

1.  Navigate to the root of your Rust project.
2.  Run `cargo2nix` using `nix run`:
    ```bash
    nix run github:cargo2nix/cargo2nix -- -o Cargo.nix
    ```
    This command will create a `Cargo.nix` file in your project's root directory.
3.  **Important:** You must re-run this command every time you modify your `Cargo.toml` or `Cargo.lock` (e.g., when adding or updating dependencies).

### 4. Step 2: Create/Update `flake.nix`

Now, create a `flake.nix` file in the root of your Rust project (or update your existing one) with the following structure:

#rust-bin.nightly."2025-09-16".default
#cat /nix/store/2087jpgp61d3yvkb2cvi1cqzs2x3sd6d-rustc-1.92.0-nightly-2025-09-16-aarch64-unknown-linux-gnu.drv

{
  inputs = {
    nixpkgs.url = "github:meta-introspector/nixpkgs?ref=feature/CRQ-016-nixify";
    flake-utils.url = "github:meta-introspector/flake-utils?ref=feature/CRQ-016-nixify";
    #    cargo2nix.url = "github:cargo2nix/cargo2nix/release-0.12";
    cargo2nix.url = "github:cargo2nix/cargo2nix/release-0.12";
    rust-overlay.url = "github:meta-introspector/rust-overlay?ref=feature/CRQ-016-nixify";

#    rust-bin = "/nix/store/7vzj2mc9rj6jlsx251822cxy683hq7vd-rustc-1.92.0-nightly-2025-09-16-aarch64-unknown-linux-gnu";


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

          #myRustc = rust-bin.nightly."2025-09-16".default;
          #myRustc = rust-bin.selectLatestNightlyWith (toolchain: toolchain.default);
          #myRustc = "/nix/store/2087jpgp61d3yvkb2cvi1cqzs2x3sd6d-rustc-1.92.0-nightly-2025-09-16-aarch64-unknown-linux-gnu.drv";
          #myRustc = "/nix/store/7vzj2mc9rj6jlsx251822cxy683hq7vd-rustc-1.92.0-nightly-2025-09-16-aarch64-unknown-linux-gnu/bin/rustc ";
          # myRustc = "/nix/store/7vzj2mc9rj6jlsx251822cxy683hq7vd-rustc-1.92.0-nightly-2025-09-16-aarch64-unknown-linux-gnu";
          myRustc = pkgs.rust-bin.nightly."2025-09-16".default;
          
          #          /nix/store/7vzj2mc9rj6jlsx251822cxy683hq7vd-rustc-1.92.0-nightly-2025-09-16-aarch64-unknown-linux-gnu
            
          rustPkgs = pkgs.rustBuilder.makePackageSet {
            packageFun = import ./Cargo.nix;
            rustToolchain = myRustc;
            #rustVersion = "2025-02-16";
            #rustChannel = "nightly";
            # rootFeatures = [
            #   "build-rs/default"
            #   "build-rs-test-lib/default"
            #   "cargo-platform/default"
            #   "cargo-test-macro/default"
            #   "cargo-test-support/default"
            #   "cargo-util/default"
            #   "crates-io/default"
            #   "cargo-util-schemas/default"
            #   "home/default"
            #   "mdman/default"
            #   "resolver-tests/default"
            #   "cargo/default"
            #   "cargo-credential/default"
            #   "rustfix/default"
            #   "cargo-credential-libsecret/default"
            #   "cargo-credential-macos-keychain/default"
            #   "cargo-credential-wincred/default"
            #   "semver-check/default"
            #   "xtask-build-man/default"
            #   "xtask-bump-check/default"
            #   "xtask-lint-docs/default"
            #   "xtask-stale-label/default"
            #   "cargo-credential-1password/default"
            #   "benchsuite/default"
            #   "capture/default"
            # ];
            # packageOverrides = pkgs: [
            #   (pkgs.rustBuilder.rustLib.makeOverride {
            #     name = "heapless";
            #     overrideAttrs = old: {
            #       rustcBuildFlags = (old.rustcBuildFlags or [ ]) ++ [ "--allow=warnings" "--allow=dead_code" ];
            #     };
            #   })
            # ];
          };

          cargo = rustPkgs.workspace.cargo { };

          workspaceShell = pkgs.mkShell {
            packages = [ pkgs.statix pkgs.openssl_1_1.dev ];
            shellHook = ''
              export PKG_CONFIG_PATH=${pkgs.openssl_1_1.dev}/lib/pkgconfig:$PKG_CONFIG_PATH
              export PATH=${myRustc}/bin:${cargo}/bin:$PATH
            '';
          };

        in
        rec {
          devShells = {
            default = workspaceShell;
          };

          packages = rec {
            inherit cargo;
            workspaceCrates = rustPkgs.workspace;
            default = cargo;
          };

          apps = rec {
            cargo = { type = "app"; program = "${packages.cargo}/bin/cargo"; };
            default = cargo;
          };
        }
      );
}
