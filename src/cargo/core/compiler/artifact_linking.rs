//! Functions for handling the linking and final placement of compiled artifacts.
//!
//! This module defines `link_targets` to manage the hard-linking of compiled
//! artifacts from the `deps` directory to their final destination (e.g., `target/debug`),
//! and also handles the generation of JSON machine messages for these artifacts.

use std::path::PathBuf;


use crate::core::compiler::build_runner::BuildRunner;
use crate::core::compiler::unit::Unit;
use crate::core::manifest::TargetSourcePath;
use crate::util::errors::CargoResult as Result;
use crate::util::machine_message::{self, Message};
use crate::core::Target;
use cargo_util::paths;
use crate::core::compiler::job_queue::Work;

/// Link the compiled target (often of form `foo-{metadata_hash}`) to the
/// final target. This must happen during both "Fresh" and "Compile".
pub fn link_targets(
    build_runner: &mut BuildRunner<'_, '_>,
    unit: &Unit,
    fresh: bool,
) -> Result<Work> {
    let bcx = build_runner.bcx;
    let outputs = build_runner.outputs(unit)?;
    let export_dir = build_runner.files().export_dir();
    let package_id = unit.pkg.package_id();
    let manifest_path = PathBuf::from(unit.pkg.manifest_path());
    let profile = unit.profile.clone();
    let unit_mode = unit.mode;
    let features = unit.features.iter().map(|s| s.to_string()).collect();
    let json_messages = bcx.build_config.emit_json();
    let executable = build_runner.get_executable(unit)?;
    let mut target = Target::clone(&unit.target);
    if let TargetSourcePath::Metabuild = target.src_path() {
        // Give it something to serialize.
        let path = unit
            .pkg
            .manifest()
            .metabuild_path(build_runner.bcx.ws.build_dir());
        target.set_src_path(TargetSourcePath::Path(path));
    }

    Ok(Work::new(move |state| {
        // If we're a "root crate", e.g., the target of this compilation, then we
        // hard link our outputs out of the `deps` directory into the directory
        // above. This means that `cargo build` will produce binaries in
        // `target/debug` which one probably expects.
        let mut destinations = vec![];
        for output in outputs.iter() {
            let src = &output.path;
            // This may have been a `cargo rustc` command which changes the
            // output, so the source may not actually exist.
            if !src.exists() {
                continue;
            }
            let Some(dst) = output.hardlink.as_ref() else {
                destinations.push(src.clone());
                continue;
            };
            destinations.push(dst.clone());
            paths::link_or_copy(src, dst)?;
            if let Some(ref path) = output.export_path {
                let export_dir = export_dir.as_ref().unwrap();
                paths::create_dir_all(export_dir)?;

                paths::link_or_copy(src, path)?;
            }
        }

        if json_messages {
            let debuginfo = match profile.debuginfo.into_inner() {
                cargo_util_schemas::manifest::TomlDebugInfo::None => machine_message::ArtifactDebuginfo::Int(0),
                cargo_util_schemas::manifest::TomlDebugInfo::Limited => machine_message::ArtifactDebuginfo::Int(1),
                cargo_util_schemas::manifest::TomlDebugInfo::Full => machine_message::ArtifactDebuginfo::Int(2),
                cargo_util_schemas::manifest::TomlDebugInfo::LineDirectivesOnly => {
                    machine_message::ArtifactDebuginfo::Named("line-directives-only")
                }
                cargo_util_schemas::manifest::TomlDebugInfo::LineTablesOnly => {
                    machine_message::ArtifactDebuginfo::Named("line-tables-only")
                }
            };
            let art_profile = machine_message::ArtifactProfile {
                opt_level: profile.opt_level.as_str(),
                debuginfo: Some(debuginfo),
                debug_assertions: profile.debug_assertions,
                overflow_checks: profile.overflow_checks,
                test: unit_mode.is_any_test(),
            };

            let msg = machine_message::Artifact {
                package_id: package_id.to_spec(),
                manifest_path,
                target: &target,
                profile: art_profile,
                features,
                filenames: destinations,
                executable,
                fresh,
            }
            .to_json_string();
            state.stdout(msg)?;
        }
        Ok(())
    }))
}

/// Helper to convert strings for environment variables.
pub fn envify(s: &str) -> String {
    s.chars()
        .flat_map(|c| c.to_uppercase())
        .map(|c| if c == '-' { '_' } else { c })
        .collect()
}
