//! # Interact with the compiler
//!
//! If you consider [`ops::cargo_compile::compile`] as a `rustc` driver but on
//! Cargo side, this module is kinda the `rustc_interface` for that merits.
//! It contains all the interaction between Cargo and the rustc compiler,
//! from preparing the context for the entire build process, to scheduling
//! and executing each unit of work (e.g. running `rustc`), to managing and
//! caching the output artifact of a build.
//!
//! However, it hasn't yet exposed a clear definition of each phase or session,
//! like what rustc has done. Also, no one knows if Cargo really needs that.
//! To be pragmatic, here we list a handful of items you may want to learn:
//!
//! * [`BuildContext`] is a static context containing all information you need
//!   before a build gets started.
//! * [`BuildRunner`] is the center of the world, coordinating a running build and
//!   collecting information from it.
//! * [`custom_build`] is the home of build script executions and output parsing.
//! * [`fingerprint`] not only defines but also executes a set of rules to
//!   determine if a re-compile is needed.
//! * [`job_queue`] is where the parallelism, job scheduling, and communication
//!   machinery happen between Cargo and the compiler.
//! * [`layout`] defines and manages output artifacts of a build in the filesystem.
//! * [`unit_dependencies`] is for building a dependency graph for compilation
//!   from a result of dependency resolution.
//! * [`Unit`] contains sufficient information to build something, usually
//!   turning into a compiler invocation in a later phase.
//!
//! [`ops::cargo_compile::compile`]: crate::ops::compile

pub mod artifact;
pub mod build_config;
pub(crate) mod build_context;
pub(crate) mod build_runner;
mod compilation;
mod compile_kind;
mod crate_type;
mod custom_build;
pub(crate) mod fingerprint;
pub mod future_incompat;
pub(crate) mod job_queue;
pub(crate) mod layout;
mod links;
mod lto;
mod output_depinfo;
mod output_sbom;
pub mod rustdoc;
pub mod standard_lib;
mod timings;
mod unit;
pub mod unit_dependencies;
pub mod unit_graph;

// New modules
mod compilation_orchestration;
mod invocation_args;
mod dependency_args;
mod path_remapping;
mod artifact_linking;




pub use build_config::{BuildConfig, CompileMode, MessageFormat, TimingOutput};
pub use build_context::{BuildContext, FileFlavor, FileType, RustcTargetData, TargetInfo};
pub use build_runner::{BuildRunner, Metadata, UnitHash};
pub use compilation::{Compilation, Doctest, UnitOutput};
pub use compile_kind::{CompileKind, CompileKindFallback, CompileTarget};
pub use crate_type::CrateType;
pub use custom_build::{LinkArgTarget, BuildOutput, BuildScriptOutputs, BuildScripts, LibraryPath};
pub(crate) use fingerprint::DirtyReason;
pub use fingerprint::RustdocFingerprint;
pub use job_queue::Freshness;
pub(crate) use layout::Layout;
pub use lto::Lto;



pub use future_incompat::FutureIncompatReport;

pub use unit::{Unit, UnitInterner};

pub use timings::CompilationSection;
pub use crate::core::{Feature, PackageId, Target, Verbosity};
pub use crate::core::manifest::TargetSourcePath;
pub use crate::core::profiles::{PanicStrategy, Profile, StripInner};
pub use crate::util::context::WarningHandling;
pub use crate::util::errors::{CargoResult, VerboseError};
pub use crate::util::interning::InternedString;
pub use crate::util::lints::get_key_value;
pub use crate::util::machine_message::{self, Message};
pub use crate::util::OnceExt;
pub use crate::util::{add_path_args, internal};
pub use cargo_util::{ProcessBuilder, ProcessError, paths};
pub use cargo_util_schemas::manifest::{TomlDebugInfo, TomlTrimPaths, TomlTrimPathsValue};
pub use rustfix::diagnostics::Applicability;


// Re-exports from new modules
pub use compilation_orchestration::{compile, Executor, DefaultExecutor, OutputOptions};
pub use invocation_args::{prepare_rustc_process, prepare_rustdoc_process};
pub use artifact_linking::{link_targets, envify}; // Assuming envify might be needed outside
pub use dependency_args::{add_native_deps, add_plugin_deps, add_custom_flags, build_deps_args, extern_args};
pub use path_remapping::{trim_paths_args, trim_paths_args_rustdoc, sysroot_remap, package_remap, build_dir_remap};
