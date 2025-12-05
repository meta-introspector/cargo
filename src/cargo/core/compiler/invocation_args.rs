//! Functions for preparing `rustc` and `rustdoc` command-line invocations.
//!
//! This module handles the construction of the `ProcessBuilder` objects
//! that represent calls to `rustc` and `rustdoc`, including adding
//! various flags related to compilation settings, error formatting,
//! feature flags, lint caps, and LTO.

use std::borrow::Cow;
use std::ffi::{OsStr, OsString};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::collections::HashSet;

use anyhow::{Context as _, Error};
use cargo_platform::{Cfg, Platform};
use itertools::Itertools;
use tracing::{debug, instrument, trace};

use crate::core::compiler::build_context::BuildContext;
use crate::core::compiler::build_config::{CompileMode, MessageFormat, TimingOutput};
use crate::core::compiler::build_runner::{BuildRunner, UnitHash};
use crate::core::compiler::compilation::{Compilation, Doctest, UnitOutput};
use crate::core::compiler::compile_kind::{CompileKind, CompileKindFallback, CompileTarget};
use crate::core::compiler::crate_type::CrateType;
use crate::core::compiler::lto::Lto;
use crate::core::compiler::path_remapping::{trim_paths_args, trim_paths_args_rustdoc};
use crate::core::compiler::{fingerprint, rustdoc, output_sbom, dependency_args};
use crate::core::compiler::timings::CompilationSection;
use crate::core::compiler::unit::{Unit, UnitInterner};
use crate::core::manifest::TargetSourcePath;
use crate::core::profiles::{PanicStrategy, Profile, StripInner};
use crate::core::{Feature, PackageId, Target, Verbosity};
use crate::util::context::WarningHandling;
use crate::util::errors::{CargoResult, VerboseError};
use crate::util::interning::InternedString;
use crate::util::lints::get_key_value;
use crate::util::machine_message::{self, Message};
use crate::util::{add_path_args, internal, path_args};
use cargo_util::{ProcessBuilder, ProcessError, paths};
use cargo_util_schemas::manifest::TomlDebugInfo;
use cargo_util_schemas::manifest::TomlTrimPaths;
use cargo_util_schemas::manifest::TomlTrimPathsValue;

const RUSTDOC_CRATE_VERSION_FLAG: &str = "--crate-version";

/// Prepares flags and environments we can compute for a `rustc` invocation
/// before the job queue starts compiling any unit.
///
/// This builds a static view of the invocation. Flags depending on the
/// completion of other units will be added later in runtime, such as flags
/// from build scripts.
pub fn prepare_rustc_process(build_runner: &BuildRunner<'_, '_>, unit: &Unit) -> CargoResult<ProcessBuilder> {
    let gctx = build_runner.bcx.gctx;
    let is_primary = build_runner.is_primary_package(unit);
    let is_workspace = build_runner.bcx.ws.is_member(&unit.pkg);

    let mut base = build_runner
        .compilation
        .rustc_process(unit, is_primary, is_workspace)?;
    build_base_args(build_runner, &mut base, unit)?;
    if unit.pkg.manifest().is_embedded() {
        if !gctx.cli_unstable().script {
            anyhow::bail!(
                "parsing `{}` requires `-Zscript`",
                unit.pkg.manifest_path().display()
            );
        }
        base.arg("-Z").arg("crate-attr=feature(frontmatter)");
    }

    base.inherit_jobserver(&build_runner.jobserver);
    dependency_args::build_deps_args(&mut base, build_runner, unit)?;
    add_cap_lints(build_runner.bcx, unit, &mut base);
    if let Some(args) = build_runner.bcx.extra_args_for(unit) {
        base.args(args);
    }
    base.args(&unit.rustflags);
    if gctx.cli_unstable().binary_dep_depinfo {
        base.arg("-Z").arg("binary-dep-depinfo");
    }
    if build_runner.bcx.gctx.cli_unstable().checksum_freshness {
        base.arg("-Z").arg("checksum-hash-algorithm=blake3");
    }

    if is_primary {
        base.env("CARGO_PRIMARY_PACKAGE", "1");
        let file_list = std::env::join_paths(build_runner.sbom_output_files(unit)?)?;
        base.env("CARGO_SBOM_PATH", file_list);
    }

    if unit.target.is_test() || unit.target.is_bench() {
        let tmp = build_runner
            .files()
            .layout(unit.kind)
            .build_dir()
            .prepare_tmp()?;
        base.env("CARGO_TARGET_TMPDIR", tmp.display().to_string());
    }

    Ok(base)
}

/// Prepares flags and environments we can compute for a `rustdoc` invocation
/// before the job queue starts compiling any unit.
///
/// This builds a static view of the invocation. Flags depending on the
/// completion of other units will be added later in runtime, such as flags
/// from build scripts.
pub fn prepare_rustdoc_process(build_runner: &BuildRunner<'_, '_>, unit: &Unit) -> CargoResult<ProcessBuilder> {
    let bcx = build_runner.bcx;
    // script_metadata is not needed here, it is only for tests.
    let mut rustdoc = build_runner.compilation.rustdoc_process(unit, None)?;
    if unit.pkg.manifest().is_embedded() {
        if !bcx.gctx.cli_unstable().script {
            anyhow::bail!(
                "parsing `{}` requires `-Zscript`",
                unit.pkg.manifest_path().display()
            );
        }
        rustdoc.arg("-Z").arg("crate-attr=feature(frontmatter)");
    }
    rustdoc.inherit_jobserver(&build_runner.jobserver);
    let crate_name = unit.target.crate_name();
    rustdoc.arg("--crate-name").arg(&crate_name);
    add_path_args(bcx.ws, unit, &mut rustdoc);
    add_cap_lints(bcx, unit, &mut rustdoc);

    if let CompileKind::Target(target) = unit.kind {
        rustdoc.arg("--target").arg(target.rustc_target());
    }
    let doc_dir = build_runner.files().out_dir(unit);
    rustdoc.arg("-o").arg(&doc_dir);
    rustdoc.args(&features_args(unit));
    rustdoc.args(&check_cfg_args(unit));

    add_error_format_and_color(build_runner, &mut rustdoc);
    add_allow_features(build_runner, &mut rustdoc);

    if build_runner.bcx.gctx.cli_unstable().rustdoc_depinfo {
        // toolchain-shared-resources is required for keeping the shared styling resources
        // invocation-specific is required for keeping the original rustdoc emission
        let mut arg = if build_runner.bcx.gctx.cli_unstable().rustdoc_mergeable_info {
            // toolchain resources are written at the end, at the same time as merging
            OsString::from("--emit=invocation-specific,dep-info=")
        } else {
            // if not using mergeable CCI, everything is written every time
            OsString::from("--emit=toolchain-shared-resources,invocation-specific,dep-info=")
        };
        // arg.push(fingerprint::rustdoc_dep_info_loc(build_runner, unit));
        rustdoc.arg(arg);

        if build_runner.bcx.gctx.cli_unstable().checksum_freshness {
            rustdoc.arg("-Z").arg("checksum-hash-algorithm=blake3");
        }

        rustdoc.arg("-Zunstable-options");
    } else if build_runner.bcx.gctx.cli_unstable().rustdoc_mergeable_info {
        // toolchain resources are written at the end, at the same time as merging
        rustdoc.arg("--emit=invocation-specific");
        rustdoc.arg("-Zunstable-options");
    }

    if build_runner.bcx.gctx.cli_unstable().rustdoc_mergeable_info {
        // write out mergeable data to be imported
        rustdoc.arg("--merge=none");
        let mut arg = OsString::from("--parts-out-dir=");
        // `-Zrustdoc-mergeable-info` always uses the new layout.
        arg.push(build_runner.files().deps_dir_new_layout(unit));
        rustdoc.arg(arg);
    }

    if let Some(trim_paths) = unit.profile.trim_paths.as_ref() {
        trim_paths_args_rustdoc(&mut rustdoc, build_runner, unit, trim_paths)?;
    }

    rustdoc.args(unit.pkg.manifest().lint_rustflags());

    let metadata = build_runner.metadata_for_doc_units[unit];
    rustdoc
        .arg("-C")
        .arg(format!("-C metadata={}", metadata.c_metadata()));

    if unit.mode.is_doc_scrape() {
        debug_assert!(build_runner.bcx.scrape_units.contains(unit));

        if unit.target.is_test() {
            rustdoc.arg("--scrape-tests");
        }

        rustdoc.arg("-Zunstable-options");

        // rustdoc
        //     .arg("--scrape-examples-output-path")
        //     .arg(rustdoc::scrape_output_path(build_runner, unit)?);

        // Only scrape example for items from crates in the workspace, to reduce generated file size
        for pkg in build_runner.bcx.packages.packages() {
            let names = pkg
                .targets()
                .iter()
                .map(|target| target.crate_name())
                .collect::<HashSet<_>>();
            for name in names {
                rustdoc.arg("--scrape-examples-target-crate").arg(name);
            }
        }
    }

    // if rustdoc::should_include_scrape_units(build_runner.bcx, unit) {
    //     rustdoc.arg("-Zunstable-options");
    // }

    dependency_args::build_deps_args(&mut rustdoc, build_runner, unit)?;
    rustdoc::add_root_urls(build_runner, unit, &mut rustdoc)?;

    rustdoc::add_output_format(build_runner, &mut rustdoc)?;

    if let Some(args) = build_runner.bcx.extra_args_for(unit) {
        rustdoc.args(args);
    }
    rustdoc.args(&unit.rustdocflags);

    if !crate_version_flag_already_present(&rustdoc) {
        append_crate_version_flag(unit, &mut rustdoc);
    }

    Ok(rustdoc)
}

/// The --crate-version flag could have already been passed in RUSTDOCFLAGS
/// or as an extra compiler argument for rustdoc
pub fn crate_version_flag_already_present(rustdoc: &ProcessBuilder) -> bool {
    rustdoc.get_args().any(|flag| {
        flag.to_str()
            .map_or(false, |flag| flag.starts_with(RUSTDOC_CRATE_VERSION_FLAG))
    })
}

pub fn append_crate_version_flag(unit: &Unit, rustdoc: &mut ProcessBuilder) {
    rustdoc
        .arg(RUSTDOC_CRATE_VERSION_FLAG)
        .arg(unit.pkg.version().to_string());
}

/// Adds [`--cap-lints`] to the command to execute.
///
/// [`--cap-lints`]: https://doc.rust-lang.org/nightly/rustc/lints/levels.html#capping-lints
pub fn add_cap_lints(bcx: &BuildContext<'_, '_>, unit: &Unit, cmd: &mut ProcessBuilder) {
    // Check if --quiet is set to suppress all warnings
    if bcx.gctx.shell().verbosity() == Verbosity::Quiet {
        cmd.arg("--cap-lints").arg("allow");
    // If this is an upstream dep we don't want warnings from, turn off all
    // lints.
    } else if !unit.show_warnings(bcx.gctx) {
        cmd.arg("--cap-lints").arg("allow");

    // If this is an upstream dep but we *do* want warnings, make sure that they
    // don't fail compilation.
    } else if !unit.is_local() {
        cmd.arg("--cap-lints").arg("warn");
    }
}

/// Forwards [`-Zallow-features`] if it is set for cargo.
///
/// [`-Zallow-features`]: https://doc.rust-lang.org/nightly/cargo/reference/unstable.html#allow-features
pub fn add_allow_features(build_runner: &BuildRunner<'_, '_>, cmd: &mut ProcessBuilder) {
    if let Some(allow) = &build_runner.bcx.gctx.cli_unstable().allow_features {
        use std::fmt::Write;
        let mut arg = String::from("-Zallow-features=");
        for f in allow {
            let _ = write!(&mut arg, "{f},");
        }
        cmd.arg(arg.trim_end_matches(','));
    }
}

/// Adds [`--error-format`] to the command to execute.
///
/// Cargo always uses JSON output. This has several benefits, such as being
/// easier to parse, handles changing formats (for replaying cached messages),
/// ensures atomic output (so messages aren't interleaved), allows for
/// intercepting messages like rmeta artifacts, etc. rustc includes a
/// "rendered" field in the JSON message with the message properly formatted,
/// which Cargo will extract and display to the user.
///
/// [`--error-format`]: https://doc.rust-lang.org/nightly/rustc/command-line-arguments.html#--error-format-control-how-errors-are-produced
pub fn add_error_format_and_color(build_runner: &BuildRunner<'_, '_>, cmd: &mut ProcessBuilder) {
    let enable_timings = build_runner.bcx.gctx.cli_unstable().section_timings
        && (!build_runner.bcx.build_config.timing_outputs.is_empty()
            || build_runner.bcx.logger.is_some());
    if enable_timings {
        cmd.arg("-Zunstable-options");
    }

    cmd.arg("--error-format=json");
    let mut json = String::from("--json=diagnostic-rendered-ansi,artifacts,future-incompat");

    if let MessageFormat::Short | MessageFormat::Json { short: true, .. } =
        build_runner.bcx.build_config.message_format
    {
        json.push_str(",diagnostic-short");
    } else if build_runner.bcx.gctx.shell().err_unicode()
        && build_runner.bcx.gctx.cli_unstable().rustc_unicode
    {
        json.push_str(",diagnostic-unicode");
    }

    if enable_timings {
        json.push_str(",timings");
    }

    cmd.arg(json);

    let gctx = build_runner.bcx.gctx;
    if let Some(width) = gctx.shell().err_width().diagnostic_terminal_width() {
        cmd.arg(format!("--diagnostic-width={width}"));
    }
}

/// Adds essential rustc flags and environment variables to the command to execute.
pub fn build_base_args(
    build_runner: &BuildRunner<'_, '_>,
    cmd: &mut ProcessBuilder,
    unit: &Unit,
) -> CargoResult<()> {
    assert!(!unit.mode.is_run_custom_build());

    let bcx = build_runner.bcx;
    let Profile {
        ref opt_level,
        codegen_backend,
        codegen_units,
        debuginfo,
        debug_assertions,
        split_debuginfo,
        overflow_checks,
        rpath,
        ref panic,
        incremental,
        strip,
        rustflags: profile_rustflags,
        trim_paths,
        hint_mostly_unused: profile_hint_mostly_unused,
        .. 
    } = unit.profile.clone();
    let hints = unit.pkg.hints().cloned().unwrap_or_default();
    let test = unit.mode.is_any_test();

    let warn = |msg: &str| {
        bcx.gctx.shell().warn(format!(
            "{}@{}: {msg}",
            unit.pkg.package_id().name(),
            unit.pkg.package_id().version()
        ))
    };
    let unit_capped_warn = |msg: &str| {
        if unit.show_warnings(bcx.gctx) {
            warn(msg)
        } else {
            Ok(())
        }
    };

    cmd.arg("--crate-name").arg(&unit.target.crate_name());

    let edition = unit.target.edition();
    edition.cmd_edition_arg(cmd);

    add_path_args(bcx.ws, unit, cmd);
    add_error_format_and_color(build_runner, cmd);
    add_allow_features(build_runner, cmd);

    let mut contains_dy_lib = false;
    if !test {
        for crate_type in &unit.target.rustc_crate_types() {
            cmd.arg("--crate-type").arg(crate_type.as_str());
            contains_dy_lib |= crate_type == &CrateType::Dylib;
        }
    }

    if unit.mode.is_check() {
        cmd.arg("--emit=dep-info,metadata");
    } else if build_runner.bcx.gctx.cli_unstable().no_embed_metadata {
        // Nightly rustc supports the -Zembed-metadata=no flag, which tells it to avoid including
        // full metadata in rlib/dylib artifacts, to save space on disk. In this case, metadata
        // will only be stored in .rmeta files.
        // When we use this flag, we should also pass --emit=metadata to all artifacts that
        // contain useful metadata (rlib/dylib/proc macros), so that a .rmeta file is actually
        // generated. If we didn't do this, the full metadata would not get written anywhere.
        // However, we do not want to pass --emit=metadata to artifacts that never produce useful
        // metadata, such as binaries, because that would just unnecessarily create empty .rmeta
        // files on disk.
        if unit.benefits_from_no_embed_metadata() {
            cmd.arg("--emit=dep-info,metadata,link");
            cmd.args(&["-Z", "embed-metadata=no"]);
        } else {
            cmd.arg("--emit=dep-info,link");
        }
    } else {
        // If we don't use -Zembed-metadata=no, we emit .rmeta files only for rlib outputs.
        // This metadata may be used in this session for a pipelined compilation, or it may
        // be used in a future Cargo session as part of a pipelined compile.
        if !unit.requires_upstream_objects() {
            cmd.arg("--emit=dep-info,metadata,link");
        } else {
            cmd.arg("--emit=dep-info,link");
        }
    }

    let prefer_dynamic = (unit.target.for_host() && !unit.target.is_custom_build()) 
        || (contains_dy_lib && !build_runner.is_primary_package(unit));
    if prefer_dynamic {
        cmd.arg("-C").arg("prefer-dynamic");
    }

    if opt_level.as_str() != "0" {
        cmd.arg("-C").arg(&format!("opt-level={}", opt_level));
    }

    if *panic != PanicStrategy::Unwind {
        cmd.arg("-C").arg(format!("panic={}", panic));
    }
    if *panic == PanicStrategy::ImmediateAbort {
        cmd.arg("-Z").arg("panic-abort-tests");
    }

    cmd.args(&lto_args(build_runner, unit));

    if let Some(backend) = codegen_backend {
        cmd.arg("-Z").arg(&format!("codegen-backend={}", backend));
    }

    if let Some(n) = codegen_units {
        cmd.arg("-C").arg(&format!("codegen-units={}", n));
    }

    let debuginfo = debuginfo.into_inner();
    // Shorten the number of arguments if possible.
    if debuginfo != TomlDebugInfo::None {
        cmd.arg("-C").arg(format!("debuginfo={debuginfo}"));
        // This is generally just an optimization on build time so if we don't
        // pass it then it's ok. The values for the flag (off, packed, unpacked)
        // may be supported or not depending on the platform, so availability is
        // checked per-value. For example, at the time of writing this code, on
        // Windows the only stable valid value for split-debuginfo is "packed",
        // while on Linux "unpacked" is also stable.
        if let Some(split) = split_debuginfo {
            if build_runner
                .bcx
                .target_data
                .info(unit.kind)
                .supports_debuginfo_split(split)
            {
                cmd.arg("-C").arg(format!("split-debuginfo={split}"));
            }
        }
    }

    if let Some(trim_paths) = trim_paths {
        trim_paths_args(cmd, build_runner, unit, &trim_paths)?;
    }

    cmd.args(unit.pkg.manifest().lint_rustflags());
    cmd.args(&profile_rustflags);

    // `-C overflow-checks` is implied by the setting of `-C debug-assertions`,
    // so we only need to provide `-C overflow-checks` if it differs from
    // the value of `-C debug-assertions` we would provide.
    if opt_level.as_str() != "0" {
        if debug_assertions {
            cmd.args(&["-C", "debug-assertions=on"]);
            if !overflow_checks {
                cmd.args(&["-C", "overflow-checks=off"]);
            }
        } else if overflow_checks {
            cmd.args(&["-C", "overflow-checks=on"]);
        }
    } else if !debug_assertions {
        cmd.args(&["-C", "debug-assertions=off"]);
        if overflow_checks {
            cmd.args(&["-C", "overflow-checks=on"]);
        }
    } else if !overflow_checks {
        cmd.args(&["-C", "overflow-checks=off"]);
    }

    if test && unit.target.harness() {
        cmd.arg("--test");

        // Cargo has historically never compiled `--test` binaries with
        // `panic=abort` because the `test` crate itself didn't support it.
        // Support is now upstream, however, but requires an unstable flag to be
        // passed when compiling the test. We require, in Cargo, an unstable
        // flag to pass to rustc, so register that here. Eventually this flag
        // will simply not be needed when the behavior is stabilized in the Rust
        // compiler itself.
        if *panic == PanicStrategy::Abort || *panic == PanicStrategy::ImmediateAbort {
            cmd.arg("-Z").arg("panic-abort-tests");
        }
    } else if test {
        cmd.arg("--cfg").arg("test");
    }

    cmd.args(&features_args(unit));
    cmd.args(&check_cfg_args(unit));

    let meta = build_runner.files().metadata(unit);
    cmd.arg("-C")
        .arg(&format!("metadata={}", meta.c_metadata()));
    if let Some(c_extra_filename) = meta.c_extra_filename() {
        cmd.arg("-C")
            .arg(&format!("extra-filename=-{}", c_extra_filename));
    }

    if rpath {
        cmd.arg("-C").arg("rpath");
    }

    cmd.arg("--out-dir")
        .arg(&build_runner.files().out_dir(unit));

    fn opt(cmd: &mut ProcessBuilder, key: &str, prefix: &str, val: Option<&OsStr>) {
        if let Some(val) = val {
            let mut joined = OsString::from(prefix);
            joined.push(val);
            cmd.arg(key).arg(joined);
        }
    }

    if let CompileKind::Target(n) = unit.kind {
        cmd.arg("--target").arg(n.rustc_target());
    }

    opt(
        cmd,
        "-C",
        "linker=",
        build_runner
            .compilation
            .target_linker(unit.kind)
            .as_ref()
            .map(|s| s.as_ref()),
    );
    if incremental {
        let dir = build_runner.files().incremental_dir(&unit);
        opt(cmd, "-C", "incremental=", Some(dir.as_os_str()));
    }

    let pkg_hint_mostly_unused = match hints.mostly_unused {
        None => None,
        Some(toml::Value::Boolean(b)) => Some(b),
        Some(v) => {
            unit_capped_warn(&format!(
                "ignoring unsupported value type ({}) for 'hints.mostly-unused', which expects a boolean",
                v.type_str()
            ))?;
            None
        }
    };
    if profile_hint_mostly_unused
        .or(pkg_hint_mostly_unused)
        .unwrap_or(false)
    {
        if bcx.gctx.cli_unstable().profile_hint_mostly_unused {
            cmd.arg("-Zhint-mostly-unused");
        } else {
            if profile_hint_mostly_unused.is_some() {
                // Profiles come from the top-level unit, so we don't use `unit_capped_warn` here.
                warn(
                    "ignoring 'hint-mostly-unused' profile option, pass `-Zprofile-hint-mostly-unused` to enable it",
                )?;
            } else if pkg_hint_mostly_unused.is_some() {
                unit_capped_warn(
                    "ignoring 'hints.mostly-unused', pass `-Zprofile-hint-mostly-unused` to enable it",
                )?;
            }
        }
    }

    let strip = strip.into_inner();
    if strip != StripInner::None {
        cmd.arg("-C").arg(format!("strip={}", strip));
    }

    if unit.is_std {
        // -Zforce-unstable-if-unmarked prevents the accidental use of
        // unstable crates within the sysroot (such as "extern crate libc" or
        // any non-public crate in the sysroot).
        //
        // RUSTC_BOOTSTRAP allows unstable features on stable.
        cmd.arg("-Z")
            .arg("force-unstable-if-unmarked")
            .env("RUSTC_BOOTSTRAP", "1");
    }

    // Add `CARGO_BIN_EXE_` environment variables for building tests.
    if unit.target.is_test() || unit.target.is_bench() {
        for bin_target in unit
            .pkg
            .manifest()
            .targets()
            .iter()
            .filter(|target| target.is_bin())
        {
            // For `cargo check` builds we do not uplift the CARGO_BIN_EXE_ artifacts to the
            // artifact-dir. We do not want to provide a path to a non-existent binary but we still
            // need to provide *something* so `env!("CARGO_BIN_EXE_...")` macros will compile.
            let exe_path = build_runner
                .files()
                .bin_link_for_target(bin_target, unit.kind, build_runner.bcx)?
                .map(|path| path.as_os_str().to_os_string())
                .unwrap_or_else(|| OsString::from(format!("placeholder:{}", bin_target.name())));

            let name = bin_target
                .binary_filename()
                .unwrap_or(bin_target.name().to_string());
            let key = format!("CARGO_BIN_EXE_{}", name);
            cmd.env(&key, exe_path);
        }
    }
    Ok(())
}

/// All active features for the unit passed as `--cfg features=<feature-name>`.
pub fn features_args(unit: &Unit) -> Vec<OsString> {
    let mut args = Vec::with_capacity(unit.features.len() * 2);

    for feat in &unit.features {
        args.push(OsString::from("--cfg"));
        args.push(OsString::from(format!("feature=\"{}\"", feat)));
    }

    args
}

/// Generates the `--check-cfg` arguments for the `unit`.
pub fn check_cfg_args(unit: &Unit) -> Vec<OsString> {
    // The routine below generates the --check-cfg arguments. Our goals here are to
    // enable the checking of conditionals and pass the list of declared features.
    //
    // In the simplified case, it would resemble something like this:
    //
    //   --check-cfg=cfg() --check-cfg=cfg(feature, values(...))
    //
    // but having `cfg()` is redundant with the second argument (as well-known names
    // and values are implicitly enabled when one or more `--check-cfg` argument is
    // passed) so we don't emit it and just pass:
    //
    //   --check-cfg=cfg(feature, values(...))
    //
    // This way, even if there are no declared features, the config `feature` will
    // still be expected, meaning users would get "unexpected value" instead of name.
    // This wasn't always the case, see rust-lang#119930 for some details.

    let gross_cap_estimation = unit.pkg.summary().features().len() * 7 + 25;
    let mut arg_feature = OsString::with_capacity(gross_cap_estimation);

    arg_feature.push("cfg(feature, values(");
    for (i, feature) in unit.pkg.summary().features().keys().enumerate() {
        if i != 0 {
            arg_feature.push(", ");
        }
        arg_feature.push("\"");
        arg_feature.push(feature);
        arg_feature.push("\"");
    }
    arg_feature.push("))");

    // In addition to the package features, we also include the `test` cfg (since
    // compiler-team#785, as to be able to someday apply it conditionally), as well
    // the `docsrs` cfg from the docs.rs service.
    //
    // We include `docsrs` here (in Cargo) instead of rustc, since there is a much closer
    // relationship between Cargo and docs.rs than rustc and docs.rs. In particular, all
    // users of docs.rs use Cargo, but not all users of rustc (like Rust-for-Linux) use docs.rs.

    vec![
        OsString::from("--check-cfg"),
        OsString::from("cfg(docsrs,test)"),
        OsString::from("--check-cfg"),
        arg_feature,
    ]
}

/// Adds LTO related codegen flags.
pub fn lto_args(build_runner: &BuildRunner<'_, '_>, unit: &Unit) -> Vec<OsString> {
    let mut result = Vec::new();
    let mut push = |arg: &str| {
        result.push(OsString::from("-C"));
        result.push(OsString::from(arg));
    };
    match build_runner.lto[unit] {
        Lto::Run(None) => push("lto"),
        Lto::Run(Some(s)) => push(&format!("lto={}", s)),
        Lto::Off => {
            push("lto=off");
            push("embed-bitcode=no");
        }
        Lto::ObjectAndBitcode => {} // this is rustc's default
        Lto::OnlyBitcode => push("linker-plugin-lto"),
        Lto::OnlyObject => push("embed-bitcode=no"),
    }
    result
}
