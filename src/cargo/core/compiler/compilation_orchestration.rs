//! High-level orchestration of the compilation process.
//!
//! This module contains the core logic for coordinating a build, including
//! the main `compile` function, the `Executor` trait and its default
//! implementation, and the `rustc` and `rustdoc` functions that prepare
//! and execute compilation and documentation tasks. It also defines
//! `OutputOptions` for handling compiler diagnostics.

use std::borrow::Cow;
use std::cell::OnceCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::ffi::{OsStr, OsString};
use std::fmt::Display;
use std::fs::{self, File};
use std::io::{BufRead, BufWriter, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::os::unix::ffi::OsStrExt;
use chrono::Local;

use annotate_snippets::{AnnotationKind, Group, Level, Renderer, Snippet};
use anyhow::{Context as _, Error};
use cargo_platform::{Cfg, Platform};
use itertools::Itertools;
use regex::Regex;
use tracing::{debug, instrument, trace};

use crate::core::compiler::artifact_linking::link_targets;
use crate::core::compiler::dependency_args::{add_custom_flags, add_native_deps, add_plugin_deps};
use crate::core::compiler::invocation_args::{prepare_rustc_process, prepare_rustdoc_process};
use crate::core::compiler::build_context::BuildContext;
use crate::core::compiler::build_config::{CompileMode, MessageFormat};
use crate::core::compiler::build_runner::{BuildRunner, UnitHash};
use crate::core::compiler::compilation::{Compilation, Doctest, UnitOutput};
use crate::core::compiler::compile_kind::{CompileKind, CompileKindFallback, CompileTarget};
use crate::core::compiler::crate_type::CrateType;
use crate::core::compiler::custom_build;
use crate::core::compiler::fingerprint;
use crate::core::compiler::job_queue::{Job, JobQueue, JobState, Work};
use crate::core::compiler::output_sbom;
use crate::core::compiler::rustdoc;
use crate::core::compiler::unit::Unit;
use crate::core::compiler::unit_graph::UnitDep;
use crate::core::manifest::TargetSourcePath;
use crate::core::profiles::{PanicStrategy, Profile, StripInner};
use crate::core::{Feature, PackageId, Target, Verbosity};
use crate::util::OnceExt;
use crate::util::context::WarningHandling;
use crate::util::errors::{CargoResult, VerboseError};
use crate::util::interning::InternedString;
use crate::util::machine_message::{self, Message};
use crate::util::{internal, paths};
use cargo_util::{ProcessBuilder, ProcessError};
use cargo_util_schemas::manifest::TomlDebugInfo; // Assuming this is needed.

// No RUSTDOC_CRATE_VERSION_FLAG here, as it's moved to invocation_args.rs

// ... (rest of the file content)

/// A glorified callback for executing calls to rustc. Rather than calling rustc
/// directly, we'll use an `Executor`, giving clients an opportunity to intercept
/// the build calls.
pub trait Executor: Send + Sync + 'static {
    /// Called after a rustc process invocation is prepared up-front for a given
    /// unit of work (may still be modified for runtime-known dependencies, when
    /// the work is actually executed).
    fn init(&self, _build_runner: &BuildRunner<'_, '_>, _unit: &Unit) {}

    /// In case of an `Err`, Cargo will not continue with the build process for
    /// this package.
    fn exec(
        &self,
        cmd: &ProcessBuilder,
        id: PackageId,
        target: &Target,
        mode: CompileMode,
        on_stdout_line: &mut dyn FnMut(&str) -> CargoResult<()>, 
        on_stderr_line: &mut dyn FnMut(&str) -> CargoResult<()>, 
    ) -> CargoResult<()>;

    /// Queried when queuing each unit of work. If it returns true, then the
    /// unit will always be rebuilt, independent of whether it needs to be.
    fn force_rebuild(&self, _unit: &Unit) -> bool {
        false
    }
}

/// A `DefaultExecutor` calls rustc without doing anything else. It is Cargo's
/// default behaviour.
#[derive(Copy, Clone)]
pub struct DefaultExecutor;

impl Executor for DefaultExecutor {
    #[instrument(name = "rustc", skip_all, fields(package = id.name().as_str(), process = cmd.to_string()))]
    fn exec(
        &self,
        cmd: &ProcessBuilder,
        id: PackageId,
        _target: &Target,
        _mode: CompileMode,
        on_stdout_line: &mut dyn FnMut(&str) -> CargoResult<()>, 
        on_stderr_line: &mut dyn FnMut(&str) -> CargoResult<()>, 
    ) -> CargoResult<()> {
        let mut stdout_buffer = Vec::new();
        let mut stderr_buffer = Vec::new();

        let mut captured_on_stdout_line = |line: &str| {
            stdout_buffer.push(line.to_string());
            on_stdout_line(line)
        };
        let mut captured_on_stderr_line = |line: &str| {
            stderr_buffer.push(line.to_string());
            on_stderr_line(line)
        };

        let result = cmd
            .exec_with_streaming(
                &mut captured_on_stdout_line,
                &mut captured_on_stderr_line,
                false,
            )
            .map(drop);

        if let Err(e) = &result {
            // Generate reproduction script on failure
            if let Err(script_err) = generate_repro_script(
                cmd,
                id,
                _target,
                &stdout_buffer,
                &stderr_buffer,
            ) {
                // Log the script generation error, but don't fail the build because of it
                eprintln!("Failed to generate reproduction script: {:?}", script_err);
            }
        }

        result
    }
}

/// Builds up and enqueue a list of pending jobs onto the `job` queue.
///
/// Starting from the `unit`, this function recursively calls itself to build
/// all jobs for dependencies of the `unit`. Each of these jobs represents
/// compiling a particular package.
///
/// Note that **no actual work is executed as part of this**, that's all done
/// next as part of [`JobQueue::execute`] function which will run everything
/// in order with proper parallelism.
#[tracing::instrument(skip(build_runner, jobs, exec))]
pub fn compile<'gctx>(
    build_runner: &mut BuildRunner<'_, 'gctx>,
    jobs: &mut JobQueue<'gctx>,
    unit: &Unit,
    exec: &Arc<dyn Executor>,
    force_rebuild: bool,
) -> CargoResult<()> {
    let bcx = build_runner.bcx;
    if !build_runner.compiled.insert(unit.clone()) {
        return Ok(());
    }

    // If we are in `--compile-time-deps` and the given unit is not a compile time
    // dependency, skip compiling the unit and jumps to dependencies, which still
    // have chances to be compile time dependencies
    if !unit.skip_non_compile_time_dep {
        // Build up the work to be done to compile this unit, enqueuing it once
        // we've got everything constructed.
        fingerprint::prepare_init(build_runner, unit)?;

        let job = if unit.mode.is_run_custom_build() {
            custom_build::prepare(build_runner, unit)?
        } else if unit.mode.is_doc_test() {
            // We run these targets later, so this is just a no-op for now.
            Job::new_fresh()
        } else {
            let force = exec.force_rebuild(unit) || force_rebuild;
            let mut job = fingerprint::prepare_target(build_runner, unit, force)?;
            job.before(if job.freshness().is_dirty() {
                let work = if unit.mode.is_doc() || unit.mode.is_doc_scrape() {
                    rustdoc_work(build_runner, unit)?
                } else {
                    rustc_work(build_runner, unit, exec)?
                };
                work.then(link_targets(build_runner, unit, false)?)
            } else {
                // We always replay the output cache,
                // since it might contain future-incompat-report messages
                let show_diagnostics = unit.show_warnings(bcx.gctx)
                    && build_runner.bcx.gctx.warning_handling()? != WarningHandling::Allow;
                let manifest = ManifestErrorContext::new(build_runner, unit);
                let work = replay_output_cache(
                    unit.pkg.package_id(),
                    manifest,
                    &unit.target,
                    build_runner.files().message_cache_path(unit),
                    build_runner.bcx.build_config.message_format,
                    show_diagnostics,
                );
                // Need to link targets on both the dirty and fresh.
                work.then(link_targets(build_runner, unit, true)?)
            });

            job
        };
        jobs.enqueue(build_runner, unit, job)?;
    }

    // Be sure to compile all dependencies of this target as well.
    let deps = Vec::from(build_runner.unit_deps(unit)); // Create vec due to mutable borrow.
    for dep in deps {
        compile(build_runner, jobs, &dep.unit, exec, false)?;
    }

    Ok(())
}

/// Generates the warning message used when fallible doc-scrape units fail,
/// either for rustdoc or rustc.
fn make_failed_scrape_diagnostic(
    build_runner: &BuildRunner<'_, '_>,
    unit: &Unit,
    top_line: impl Display,
) -> String {
    let manifest_path = unit.pkg.manifest_path();
    let relative_manifest_path = manifest_path
        .strip_prefix(build_runner.bcx.ws.root())
        .unwrap_or(&manifest_path);

    format!(
        "{}
    Try running with `--verbose` to see the error message.
    If an example should not be scanned, then consider adding `doc-scrape-examples = false` to its `[[example]]` definition in {}",
        top_line,
        relative_manifest_path.display()
    )
}

/// Creates a unit of work invoking `rustc` for building the `unit`.
fn rustc_work(
    build_runner: &mut BuildRunner<'_, '_>,
    unit: &Unit,
    exec: &Arc<dyn Executor>,
) -> CargoResult<Work> {
    let mut rustc = prepare_rustc_process(build_runner, unit)?;

    let name = unit.pkg.name();

    let outputs = build_runner.outputs(unit)?;
    let root = build_runner.files().out_dir(unit);

    // Prepare the native lib state (extra `-L` and `-l` flags).
    let build_script_outputs = Arc::clone(&build_runner.build_script_outputs);
    let current_id = unit.pkg.package_id();
    let manifest = ManifestErrorContext::new(build_runner, unit);
    let build_scripts = build_runner.build_scripts.get(unit).cloned();

    // If we are a binary and the package also contains a library, then we
    // don't pass the `-l` flags.
    let pass_l_flag = unit.target.is_lib() || !unit.pkg.targets().iter().any(|t| t.is_lib());

    let dep_info_name =
        if let Some(c_extra_filename) = build_runner.files().metadata(unit).c_extra_filename() {
            format!("{}-{}.d", unit.target.crate_name(), c_extra_filename)
        } else {
            format!("{}.d", unit.target.crate_name())
        };
    let rustc_dep_info_loc = root.join(dep_info_name);
    let dep_info_loc = fingerprint::dep_info_loc(build_runner, unit);

    let mut output_options = OutputOptions::new(build_runner, unit);
    let package_id = unit.pkg.package_id();
    let target = Target::clone(&unit.target);
    let mode = unit.mode;

    exec.init(build_runner, unit);
    let exec = exec.clone();

    let root_output = build_runner.files().host_dest().map(|v| v.to_path_buf());
    let build_dir = build_runner.bcx.ws.build_dir().into_path_unlocked();
    let pkg_root = unit.pkg.root().to_path_buf();
    let cwd = rustc
        .get_cwd()
        .unwrap_or_else(|| build_runner.bcx.gctx.cwd())
        .to_path_buf();
    let fingerprint_dir = build_runner.files().fingerprint_dir(unit);
    let script_metadatas = build_runner.find_build_script_metadatas(unit);
    let is_local = unit.is_local();
    let artifact = unit.artifact;
    let sbom_files = build_runner.sbom_output_files(unit)?;
    let sbom = output_sbom::build_sbom(build_runner, unit)?;

    let hide_diagnostics_for_scrape_unit = build_runner.bcx.unit_can_fail_for_docscraping(unit)
        && !matches!(
            build_runner.bcx.gctx.shell().verbosity(),
            Verbosity::Verbose
        );
    let failed_scrape_diagnostic = hide_diagnostics_for_scrape_unit.then(|| {
        // If this unit is needed for doc-scraping, then we generate a diagnostic that
        // describes the set of reverse-dependencies that cause the unit to be needed.
        let target_desc = unit.target.description_named();
        let mut for_scrape_units = build_runner
            .bcx
            .scrape_units_have_dep_on(unit)
            .into_iter()
            .map(|unit| unit.target.description_named())
            .collect::<Vec<_>>();
        for_scrape_units.sort();
        let for_scrape_units = for_scrape_units.join(", ");
        make_failed_scrape_diagnostic(build_runner, unit, format_args!("failed to check {target_desc} in package `{name}` as a prerequisite for scraping examples from: {for_scrape_units}"))
    });
    if hide_diagnostics_for_scrape_unit {
        output_options.show_diagnostics = false;
    }
    let env_config = Arc::clone(build_runner.bcx.gctx.env_config()?);
    return Ok(Work::new(move |state| {
        // Artifacts are in a different location than typical units,
        // hence we must assure the crate- and target-dependent
        // directory is present.
        if artifact.is_true() {
            paths::create_dir_all(&root)?;
        }

        // Only at runtime have we discovered what the extra -L and -l
        // arguments are for native libraries, so we process those here. We
        // also need to be sure to add any -L paths for our plugins to the
        // dynamic library load path as a plugin's dynamic library may be
        // located somewhere in there.
        // Finally, if custom environment variables have been produced by
        // previous build scripts, we include them in the rustc invocation.
        if let Some(build_scripts) = build_scripts {
            let script_outputs = build_script_outputs.lock().unwrap();
            add_native_deps(
                &mut rustc,
                &script_outputs,
                &build_scripts,
                pass_l_flag,
                &target,
                current_id,
                mode,
            )?;
            if let Some(ref root_output) = root_output {
                add_plugin_deps(&mut rustc, &script_outputs, &build_scripts, root_output)?;
            }
            add_custom_flags(&mut rustc, &script_outputs, script_metadatas)?;
        }

        for output in outputs.iter() {
            // If there is both an rmeta and rlib, rustc will prefer to use the
            // rlib, even if it is older. Therefore, we must delete the rlib to
            // force using the new rmeta.
            if output.path.extension() == Some(OsStr::new("rmeta")) {
                let dst = root.join(&output.path).with_extension("rlib");
                if dst.exists() {
                    paths::remove_file(&dst)?;
                }
            }

            // Some linkers do not remove the executable, but truncate and modify it.
            // That results in the old hard-link being modified even after renamed.
            // We delete the old artifact here to prevent this behavior from confusing users.
            // See rust-lang/cargo#8348.
            if output.hardlink.is_some() && output.path.exists() {
                _ = paths::remove_file(&output.path).map_err(|e| {
                    tracing::debug!(
                        "failed to delete previous output file `{:?}`: {e:?}",
                        output.path
                    );
                });
            }
        }

        state.running(&rustc);
        let timestamp = paths::set_invocation_time(&fingerprint_dir)?;
        for file in sbom_files {
            tracing::debug!("writing sbom to {}", file.display());
            let outfile = BufWriter::new(paths::create(&file)?);
            serde_json::to_writer(outfile, &sbom)?;
        }

        let result = exec
            .exec(
                &rustc,
                package_id,
                &target,
                mode,
                &mut |line| on_stdout_line(state, line, package_id, &target),
                &mut |line| {
                    on_stderr_line(
                        state,
                        line,
                        package_id,
                        &manifest,
                        &target,
                        &mut output_options,
                    )
                },
            )
            .map_err(|e| {
                if output_options.errors_seen == 0 {
                    // If we didn't expect an error, do not require --verbose to fail.
                    // This is intended to debug
                    // https://github.com/rust-lang/crater/issues/733, where we are seeing
                    // Cargo exit unsuccessfully while seeming to not show any errors.
                    e
                } else {
                    verbose_if_simple_exit_code(e)
                }
            })
            .with_context(|| {
                // adapted from rustc_errors/src/lib.rs
                let warnings_str = match output_options.warnings_seen {
                    0 => String::new(),
                    1 => "; 1 warning emitted".to_string(),
                    count => format!("; {} warnings emitted", count),
                };
                let errors_str = match output_options.errors_seen {
                    0 => String::new(),
                    1 => " due to 1 previous error".to_string(),
                    count => format!(" due to {} previous errors", count),
                };
                // Combine everything for the context message
                format!("could not compile `{}`{}{}", name, errors_str, warnings_str)
            });
        result
    }))
}
