// src/cargo/core/compiler/repro_script_generator.rs

use anyhow::Result;
use cargo_util::ProcessBuilder;
use crate::core::PackageId;
use crate::core::Target;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::ffi::OsString;
use std::borrow::Cow;

/// A trait for generating a reproduction artifact (e.g., a shell script, a Nix flake)
/// for a failed compilation.
pub trait GenerateReproArtifact: Send + Sync + 'static {
    /// Generates the artifact given the command, package ID, target, and captured outputs.
    fn generate_repro_artifact(
        &self,
        cmd: &ProcessBuilder,
        id: PackageId,
        target: &Target,
        stdout: &[String],
        stderr: &[String],
    ) -> Result<()>;
}

/// A dummy implementation of `GenerateReproArtifact` for now, to allow compilation.
pub struct DefaultReproArtifactGenerator;

impl GenerateReproArtifact for DefaultReproArtifactGenerator {
    fn generate_repro_artifact(
        &self,
        cmd: &ProcessBuilder,
        id: PackageId,
        target: &Target,
        stdout: &[String],
        stderr: &[String],
    ) -> Result<()> {
        let flake_content = format!(
r#"{{
  description = "Reproduction flake for {package_name}-{package_version}";

  inputs = {{
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  }};

  outputs = {{ self, nixpkgs }}:
    let
      pkgs = nixpkgs.legacyPackages.x86_64-linux; # Assuming x86_64-linux for now
    in
    {{
      devShell.x86_64-linux = pkgs.mkShell {{
        nativeBuildInputs = [ pkgs.rustc pkgs.cargo ];
        shellHook = ''
          echo "Reproducing build failure for {package_name}-{package_version} ({target_triple})"
          echo "Rustc command:"
          echo "{rustc_command}"
          echo "Environment variables:"
          {env_vars}
          echo "--- STDOUT ---"
          {captured_stdout}
          echo "--- STDERR ---"
          {captured_stderr}
          # You can add the actual rustc command here to re-run it

        '';
      }};
    }};
}}"#,
            package_name = id.name(),
            package_version = id.version(),
            target_triple = format!("{}", target.kind().description()), // TODO: Properly get the rustc_target triple.
            rustc_command = cmd.to_string(),
            env_vars = cmd.get_envs().iter().map(|(k, v)| {
                let key_str = k.to_string();
                let value_str = v.as_ref().map(|os_string| os_string.to_string_lossy()).unwrap_or_default();
                format!("          export {}=\"{}\"", key_str, value_str)
            }).collect::<Vec<_>>().join("\n"),
            captured_stdout = stdout.join("\n          "),
            captured_stderr = stderr.join("\n          "),
            // rustc_command_full = { /* This would be complex to reconstruct fully without more context */ cmd.to_string() }
        );

        let filename = format!("repro-{}-{}.nix", id.name(), id.version());
        let mut file = fs::File::create(&filename)?;
        file.write_all(flake_content.as_bytes())?;

        eprintln!("Generated reproduction flake: {}", filename);
        Ok(())
    }
}
