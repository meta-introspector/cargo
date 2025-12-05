//! Functions for managing compiler flags and environment variables related to dependencies.
//!
//! This module handles adding library search paths (`-L`), external crate
//! declarations (`--extern`), and custom build script outputs (e.g., `cfg`
//! flags and environment variables) to `rustc` and `rustdoc` invocations.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use anyhow::{Context as _, Error};
use itertools::Itertools;
use tracing::{debug, instrument, trace};

use crate::core::compiler::artifact;
use crate::core::compiler::build_context::BuildContext;
use crate::core::compiler::build_config::CompileMode;
use crate::core::compiler::build_runner::BuildRunner;
use crate::core::compiler::custom_build::{BuildScriptOutputs, BuildScripts, LibraryPath};
use crate::core::compiler::unit::Unit;
use crate::core::{Feature, PackageId, Target, Verbosity};
use crate::util::errors::CargoResult;
use crate::util::interning::InternedString;
use crate::core::compiler::unit_graph::UnitDep;
use crate::core::compiler::build_context::FileFlavor;
use annotate_snippets::Level;
use crate::util::{internal};
use cargo_util::{ProcessBuilder, paths};

/// Adds dependency-relevant rustc flags and environment variables
/// to the command to execute, such as [`-L`] and [`--extern`].
///
/// [`-L`]: https://doc.rust-lang.org/nightly/rustc/command-line-arguments.html#-l-add-a-directory-to-the-library-search-path
/// [`--extern`]: https://doc.rust-lang.org/nightly/rustc/command-line-arguments.html#--extern-specify-where-an-external-library-is-located
pub fn build_deps_args(
    cmd: &mut ProcessBuilder,
    build_runner: &BuildRunner<'_, '_>,
    unit: &Unit,
) -> CargoResult<()> {
    let bcx = build_runner.bcx;
    if build_runner.bcx.gctx.cli_unstable().build_dir_new_layout {
        let mut map = BTreeMap::new();

        // Recursively add all dependency args to rustc process
        add_dep_arg(&mut map, build_runner, unit);

        let paths = map.into_iter().map(|(_, path)| path).sorted_unstable();

        for path in paths {
            cmd.arg("-L").arg(&{
                let mut deps = OsString::from("dependency=");
                deps.push(path);
                deps
            });
        }
    } else {
        cmd.arg("-L").arg(&{
            let mut deps = OsString::from("dependency=");
            deps.push(build_runner.files().deps_dir(unit));
            deps
        });
    }

    // Be sure that the host path is also listed. This'll ensure that proc macro
    // dependencies are correctly found (for reexported macros).
    if !unit.kind.is_host() {
        cmd.arg("-L").arg(&{
            let mut deps = OsString::from("dependency=");
            deps.push(build_runner.files().host_deps(unit));
            deps
        });
    }

    let deps = build_runner.unit_deps(unit);

    // If there is not one linkable target but should, rustc fails later
    // on if there is an `extern crate` for it. This may turn into a hard
    // error in the future (see PR #4797).
    if !deps
        .iter()
        .any(|dep| !dep.unit.mode.is_doc() && dep.unit.target.is_linkable())
    {
        if let Some(dep) = deps.iter().find(|dep| {
            !dep.unit.mode.is_doc() && dep.unit.target.is_lib() && !dep.unit.artifact.is_true()
        }) {
            let dep_name = dep.unit.target.crate_name();
            let name = unit.target.crate_name();
            bcx.gctx.shell().print_report(&[
                Level::WARNING.secondary_title(format!("the package `{dep_name}` provides no linkable target"))
                    .elements([
                        Level::NOTE.message(format!("this might cause `{name}` to fail compilation")),
                        Level::NOTE.message("this warning might turn into a hard error in the future"),
                        Level::HELP.message(format!("consider adding 'dylib' or 'rlib' to key 'crate-type' in `{dep_name}`'s Cargo.toml"))
                    ])
            ], false)?;
        }
    }

    let mut unstable_opts = false;

    // Add `OUT_DIR` environment variables for build scripts
    let first_custom_build_dep = deps.iter().find(|dep| dep.unit.mode.is_run_custom_build());
    if let Some(dep) = first_custom_build_dep {
        let out_dir = &build_runner.files().build_script_out_dir(&dep.unit);
        cmd.env("OUT_DIR", &out_dir);
    }

    // Adding output directory for each build script
    let is_multiple_build_scripts_enabled = unit
        .pkg
        .manifest()
        .unstable_features()
        .require(Feature::multiple_build_scripts())
        .is_ok();

    if is_multiple_build_scripts_enabled {
        for dep in deps {
            if dep.unit.mode.is_run_custom_build() {
                let out_dir = &build_runner.files().build_script_out_dir(&dep.unit);
                let target_name = dep.unit.target.name();
                let out_dir_prefix = target_name
                    .strip_prefix("build-script-")
                    .unwrap_or(target_name);
                let out_dir_name = format!("{out_dir_prefix}_OUT_DIR");
                cmd.env(&out_dir_name, &out_dir);
            }
        }
    }
    for arg in extern_args(build_runner, unit, &mut unstable_opts)? {
        cmd.arg(arg);
    }

    for (var, env) in artifact::get_env(build_runner, deps)? {
        cmd.env(&var, env);
    }

    // This will only be set if we're already using a feature
    // requiring nightly rust
    if unstable_opts {
        cmd.arg("-Z").arg("unstable-options");
    }

    Ok(())
}

fn add_dep_arg<'a, 'b: 'a>(
    map: &mut BTreeMap<&'a Unit, PathBuf>,
    build_runner: &'b BuildRunner<'b, '_>,
    unit: &'a Unit,
) {
    if map.contains_key(&unit) {
        return;
    }
    map.insert(&unit, build_runner.files().deps_dir(&unit));

    for dep in build_runner.unit_deps(unit) {
        add_dep_arg(map, build_runner, &dep.unit);
    }
}

/// All relevant `-L` and `-l` flags from dependencies (now calculated and
/// present in `state`) to the command provided.
pub fn add_native_deps(
    rustc: &mut ProcessBuilder,
    build_script_outputs: &BuildScriptOutputs,
    build_scripts: &BuildScripts,
    pass_l_flag: bool,
    target: &Target,
    current_id: PackageId,
    mode: CompileMode,
) -> CargoResult<()> {
    let mut library_paths = vec![];

    for key in build_scripts.to_link.iter() {
        let output = build_script_outputs.get(key.1).ok_or_else(|| {
            internal(format!(
                "couldn't find build script output for {}/{}",
                key.0,
                key.1
            ))
        })?;
        library_paths.extend(output.library_paths.iter());
    }

    // NOTE: This very intentionally does not use the derived ord from LibraryPath because we need to
    // retain relative ordering within the same type (i.e. not lexicographic). The use of a stable sort
    // is also important here because it ensures that paths of the same type retain the same relative
    // ordering (for an unstable sort to work here, the list would need to retain the idx of each element
    // and then sort by that idx when the type is equivalent.
    library_paths.sort_by_key(|p| match p {
        LibraryPath::CargoArtifact(_) => 0,
        LibraryPath::External(_) => 1,
    });

    for path in library_paths.iter() {
        rustc.arg("-L").arg(path.as_ref());
    }

    for key in build_scripts.to_link.iter() {
        let output = build_script_outputs.get(key.1).ok_or_else(|| {
            internal(format!(
                "couldn't find build script output for {}/{}",
                key.0,
                key.1
            ))
        })?;

        if key.0 == current_id {
            if pass_l_flag {
                for name in output.library_links.iter() {
                    rustc.arg("-l").arg(name);
                }
            }
        }

        for (lt, arg) in &output.linker_args {
            // There was an unintentional change where cdylibs were
            // allowed to be passed via transitive dependencies. This
            // clause should have been kept in the `if` block above.
            // For now, continue allowing it for cdylib only.
            // See https://github.com/rust-lang/cargo/issues/9562
            if lt.applies_to(target, mode)
                && (key.0 == current_id || *lt == LinkArgTarget::Cdylib)
            {
                rustc.arg("-C").arg(format!("link-arg={}", arg));
            }
        }
    }
    Ok(())
}

/// For all plugin dependencies, add their -L paths (now calculated and present
/// in `build_script_outputs`) to the dynamic library load path for the command
/// to execute.
pub fn add_plugin_deps(
    rustc: &mut ProcessBuilder,
    build_script_outputs: &BuildScriptOutputs,
    build_scripts: &BuildScripts,
    root_output: &Path,
) -> CargoResult<()> {
    let var = paths::dylib_path_envvar();
    let search_path = rustc.get_env(var).unwrap_or_default();
    let mut search_path = env::split_paths(&search_path).collect::<Vec<_>>();
    for (pkg_id, metadata) in &build_scripts.plugins {
        let output = build_script_outputs
            .get(*metadata)
            .ok_or_else(|| internal(format!("couldn't find libs for plugin dep {}", pkg_id)))?;
        search_path.append(&mut filter_dynamic_search_path(
            output.library_paths.iter().map(AsRef::as_ref),
            root_output,
        ));
    }
    let search_path = paths::join_paths(&search_path, var)?;
    rustc.env(var, &search_path);
    Ok(())
}

pub fn get_dynamic_search_path(path: &Path) -> &Path {
    match path.to_str().and_then(|s| s.split_once("=")) {
        Some(("native" | "crate" | "dependency" | "framework" | "all", path)) => Path::new(path),
        _ => path,
    }
}

// Determine paths to add to the dynamic search path from -L entries
//
// Strip off prefixes like "native=" or "framework=" and filter out directories
// **not** inside our output directory since they are likely spurious and can cause
// clashes with system shared libraries (issue #3366).
pub fn filter_dynamic_search_path<'a, I>(paths: I, root_output: &Path) -> Vec<PathBuf>
where
    I: Iterator<Item = &'a PathBuf>,
{
    let mut search_path = vec![];
    for dir in paths {
        let dir = get_dynamic_search_path(dir);
        if dir.starts_with(&root_output) {
            search_path.push(dir.to_path_buf());
        } else {
            debug!(
                "Not including path {} in runtime library search path because it is a platform library.",
                dir.display()
            );
        }
    }
    search_path
}

/// Adds extra rustc flags and environment variables collected from the output
/// of a build-script to the command to execute, include custom environment
/// variables and `cfg`.
pub fn add_custom_flags(
    cmd: &mut ProcessBuilder,
    build_script_outputs: &BuildScriptOutputs,
    metadata_vec: Option<Vec<UnitHash>>,
) -> CargoResult<()> {
    if let Some(metadata_vec) = metadata_vec {
        for metadata in metadata_vec {
            if let Some(output) = build_script_outputs.get(metadata) {
                for cfg in output.cfgs.iter() {
                    cmd.arg("--cfg").arg(cfg);
                }
                for check_cfg in &output.check_cfgs {
                    cmd.arg("--check-cfg").arg(check_cfg);
                }
                for (name, value) in output.env.iter() {
                    cmd.env(name, value);
                }
            }
        }
    }

    Ok(())
}

/// Generates a list of `--extern` arguments.
pub fn extern_args(
    build_runner: &BuildRunner<'_, '_>,
    unit: &Unit,
    unstable_opts: &mut bool,
) -> CargoResult<Vec<OsString>> {
    let mut result = Vec::new();
    let deps = build_runner.unit_deps(unit);

    let no_embed_metadata = build_runner.bcx.gctx.cli_unstable().no_embed_metadata;

    // Closure to add one dependency to `result`.
    let mut link_to =
        |dep: &UnitDep, extern_crate_name: InternedString, noprelude: bool| -> CargoResult<()> {
            let mut value = OsString::new();
            let mut opts = Vec::new();
            let is_public_dependency_enabled = unit
                .pkg
                .manifest()
                .unstable_features()
                .require(Feature::public_dependency())
                .is_ok()
                || build_runner.bcx.gctx.cli_unstable().public_dependency;
            if !dep.public && unit.target.is_lib() && is_public_dependency_enabled {
                opts.push("priv");
                *unstable_opts = true;
            }
            if noprelude {
                opts.push("noprelude");
                *unstable_opts = true;
            }
            if !opts.is_empty() {
                value.push(opts.join(","));
                value.push(":");
            }
            value.push(extern_crate_name.as_str());
            value.push("=");

            let mut pass = |file| {
                let mut value = value.clone();
                value.push(file);
                result.push(OsString::from("--extern"));
                result.push(value);
            };

            let outputs = build_runner.outputs(&dep.unit)?;

            if build_runner.only_requires_rmeta(unit, &dep.unit) || dep.unit.mode.is_check() {
                // Example: rlib dependency for an rlib, rmeta is all that is required.
                let output = outputs
                    .iter()
                    .find(|output| output.flavor == crate::core::compiler::build_context::FileFlavor::Rmeta)
                    .expect("failed to find rmeta dep for pipelined dep");
                pass(&output.path);
            } else {
                // Example: a bin needs `rlib` for dependencies, it cannot use rmeta.
                for output in outputs.iter() {
                    if output.flavor == crate::core::compiler::build_context::FileFlavor::Linkable {
                        pass(&output.path);
                    }
                    // If we use -Zembed-metadata=no, we also need to pass the path to the
                    // corresponding .rmeta file to the linkable artifact, because the
                    // normal dependency (rlib) doesn't contain the full metadata.
                    else if no_embed_metadata && output.flavor == crate::core::compiler::build_context::FileFlavor::Rmeta {
                        pass(&output.path);
                    }
                }
            }
            Ok(())
        };

    for dep in deps {
        if dep.unit.target.is_linkable() && !dep.unit.mode.is_doc() {
            link_to(dep, dep.extern_crate_name, dep.noprelude)?;
        }
    }
    if unit.target.proc_macro() {
        // Automatically import `proc_macro`.
        result.push(OsString::from("--extern"));
        result.push(OsString::from("proc_macro"));
    }

    Ok(result)
}
