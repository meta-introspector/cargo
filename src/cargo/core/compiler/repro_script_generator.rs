// src/cargo/core/compiler/repro_script_generator.rs

use anyhow::Result;
use cargo_util::ProcessBuilder;
use crate::core::PackageId;
use crate::core::compiler::CompileKind;
use std::fs;
use std::io::Write;
use std::borrow::Cow;

/// A trait for generating a reproduction artifact (e.g., a shell script, a Nix flake)
/// for a failed compilation.
pub trait GenerateReproArtifact: Send + Sync + 'static {
    /// Generates the artifact given the command, package ID, target, and captured outputs.
    fn generate_repro_artifact(
        &self,
        cmd: &ProcessBuilder,
        id: PackageId,
        compile_kind: CompileKind,
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
        compile_kind: CompileKind, // Changed from &Target to CompileKind
        stdout: &[String],
        stderr: &[String],
    ) -> Result<()> {
        let rustc_target_triple = match compile_kind {
            CompileKind::Host => {
                // TODO: Get the actual host triple, perhaps from the process builder or environment
                // For now, assume common host for devShell
                "x86_64-unknown-linux-gnu".to_string()
            },
            CompileKind::Target(ct) => ct.rustc_target().to_string(),
        };

        let system = match compile_kind {
            CompileKind::Host => "x86_64-linux".to_string(), // Assume common host system
            CompileKind::Target(ct) => {
                // Attempt to map rustc_target to Nix system. This is a simplification.
                // A more robust solution might require a lookup table or Nix functionality.
                let target_str = ct.rustc_target();
                if target_str.contains("x86_64-") && target_str.contains("-linux") {
                    "x86_64-linux".to_string()
                } else if target_str.contains("aarch64-") && target_str.contains("-linux") {
                    "aarch64-linux".to_string()
                } else if target_str.contains("x86_64-") && target_str.contains("-darwin") {
                    "x86_64-darwin".to_string()
                } else if target_str.contains("aarch64-") && target_str.contains("-darwin") {
                    "aarch64-darwin".to_string()
                }
                else {
                    // Fallback, might need to be more specific or generate an error
                    eprintln!("Warning: Unknown rustc target triple for Nix system mapping: {}", target_str);
                    "x86_64-linux".to_string() // Default to x86_64-linux
                }
            }
        };

        let full_rustc_command = cmd.to_string();

        let flake_content = format!(
r#"{{
  description = "Reproduction flake for {package_name}-{package_version} (Target: {rustc_target_triple})";

  inputs = {{
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  }};

  outputs = {{ self, nixpkgs }}:
    let
      pkgs = nixpkgs.legacyPackages.{system};
    in
    {{
      devShell.{system} = pkgs.mkShell {{
        nativeBuildInputs = [ pkgs.rustc pkgs.cargo ];
        shellHook = ''
          echo "Reproducing build failure for {package_name}-{package_version} (Target: {rustc_target_triple})"
          echo "Rustc command:"
          echo "{full_rustc_command_escaped}"
          echo "Environment variables:"
          {env_vars}
          echo "--- STDOUT ---"
          {captured_stdout}
          echo "--- STDERR ---"
          {captured_stderr}

          echo "Attempting to re-run the rustc command..."
          {full_rustc_command_escaped}
        '';
      }};
    }};
}}"#,
            package_name = id.name(),
            package_version = id.version(),
            rustc_target_triple = rustc_target_triple,
            system = system,
            full_rustc_command_escaped = shell_escape(&full_rustc_command),
            env_vars = cmd.get_envs().iter().map(|(k, v)| {
                let k_string = k.to_string();
                let key_str = shell_escape(&k_string);
                let v_string = v.as_ref().map(|os_string| os_string.to_string_lossy()).unwrap_or_default();
                let value_str = shell_escape(&v_string);
                format!("          export {}=\"{}\"", key_str, value_str)
            }).collect::<Vec<_>>().join("\n"),
            captured_stdout = stdout.iter().map(|s| format!("          {}", shell_escape(s))).collect::<Vec<_>>().join("\n"),
            captured_stderr = stderr.iter().map(|s| format!("          {}", shell_escape(s))).collect::<Vec<_>>().join("\n"),
        );

        let filename = format!("repro-{}-{}.nix", id.name(), id.version());
        let mut file = fs::File::create(&filename)?;
        file.write_all(flake_content.as_bytes())?;

        eprintln!("Generated reproduction flake: {}", filename);
        Ok(())
    }
}

// Helper function to escape strings for shell.
fn shell_escape(s: &str) -> Cow<'_, str> {
    if s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '/' || c == '.') {
        Cow::Borrowed(s)
    } else {
        Cow::Owned(format!("'{}'", s.replace('\'', "'\\''")))
    }
}

