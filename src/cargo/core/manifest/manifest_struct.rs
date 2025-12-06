use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc; // Changed from Rc
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use cargo_util_schemas::manifest::RustVersion;
use cargo_util_schemas::manifest::{Hints, TomlManifest, TomlProfiles};
use semver::Version;
use url::Url;

use crate::core::compiler::{CompileKind};
use crate::core::{Dependency, PackageId, PackageIdSpec, SourceId, Summary};
use crate::core::{Edition, Features, WorkspaceConfig};
use crate::util::errors::*;
use crate::util::interning::InternedString;
use crate::util::{Filesystem, GlobalContext, short_hash};

use super::Target; // Assuming Target is still in the original manifest.rs
use super::Warnings; // Assuming Warnings is still in the original manifest.rs
use super::ManifestMetadata; // Assuming ManifestMetadata is still in the original manifest.rs
use super::MANIFEST_PREAMBLE; // Assuming MANIFEST_PREAMBLE is still in the original manifest.rs
use crate::core::resolver::ResolveBehavior;
use crate::core::Feature;


/// Contains all the information about a package, as loaded from a `Cargo.toml`.
///
/// This is deserialized using the [`TomlManifest`] type.
#[derive(Clone, Debug)]
pub struct Manifest {
    // alternate forms of manifests:
    pub(crate) contents: Arc<String>,
    pub(crate) document: Arc<toml::Spanned<toml::de::DeTable<'static>>>,
    pub(crate) original_toml: Arc<TomlManifest>,
    pub(crate) normalized_toml: Arc<TomlManifest>,
    summary: Summary,

    // this form of manifest:
    targets: Vec<Target>,
    default_kind: Option<CompileKind>,
    forced_kind: Option<CompileKind>,
    links: Option<String>,
    warnings: Warnings,
    exclude: Vec<String>,
    include: Vec<String>,
    metadata: ManifestMetadata,
    custom_metadata: Option<toml::Value>,
    publish: Option<Vec<String>>,
    replace: Vec<(PackageIdSpec, Dependency)>,
    patch: HashMap<Url, Vec<Dependency>>,
    workspace: WorkspaceConfig,
    unstable_features: Features,
    edition: Edition,
    rust_version: Option<RustVersion>,
    im_a_teapot: Option<bool>,
    default_run: Option<String>,
    metabuild: Option<Vec<String>>,
    resolve_behavior: Option<ResolveBehavior>,
    lint_rustflags: Vec<String>,
    hints: Option<Hints>,
    embedded: bool,
}

impl Manifest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        contents: Arc<String>,
        document: Arc<toml::Spanned<toml::de::DeTable<'static>>>,
        original_toml: Arc<TomlManifest>,
        normalized_toml: Arc<TomlManifest>,
        summary: Summary,

        default_kind: Option<CompileKind>,
        forced_kind: Option<CompileKind>,
        targets: Vec<Target>,
        exclude: Vec<String>,
        include: Vec<String>,
        links: Option<String>,
        metadata: ManifestMetadata,
        custom_metadata: Option<toml::Value>,
        publish: Option<Vec<String>>,
        replace: Vec<(PackageIdSpec, Dependency)>,
        patch: HashMap<Url, Vec<Dependency>>,
        workspace: WorkspaceConfig,
        unstable_features: Features,
        edition: Edition,
        rust_version: Option<RustVersion>,
        im_a_teapot: Option<bool>,
        default_run: Option<String>,
        metabuild: Option<Vec<String>>,
        resolve_behavior: Option<ResolveBehavior>,
        lint_rustflags: Vec<String>,
        hints: Option<Hints>,
        embedded: bool,
    ) -> Manifest {
        Manifest {
            contents,
            document,
            original_toml,
            normalized_toml,
            summary,

            default_kind,
            forced_kind,
            targets,
            warnings: Warnings::new(),
            exclude,
            include,
            links,
            metadata,
            custom_metadata,
            publish,
            replace,
            patch,
            workspace,
            unstable_features,
            edition,
            rust_version,
            im_a_teapot,
            default_run,
            metabuild,
            resolve_behavior,
            lint_rustflags,
            hints,
            embedded,
        }
    }

    /// The raw contents of the original TOML
    pub fn contents(&self) -> &str {
        self.contents.as_str()
    }
    /// See [`Manifest::normalized_toml`] for what "normalized" means
    pub fn to_normalized_contents(&self) -> CargoResult<String> {
        let toml = toml::to_string_pretty(self.normalized_toml())?;
        Ok(format!("{}
{}", MANIFEST_PREAMBLE, toml))
    }
    /// Collection of spans for the original TOML
    pub fn document(&self) -> &toml::Spanned<toml::de::DeTable<'static>> {
        &self.document
    }
    /// The [`TomlManifest`] as parsed from [`Manifest::document`]
    pub fn original_toml(&self) -> &TomlManifest {
        &self.original_toml
    }
    /// The [`TomlManifest`] with all fields expanded
    ///
    /// This is the intersection of what fields need resolving for cargo-publish that also are
    /// useful for the operation of cargo, including
    /// - workspace inheritance
    /// - target discovery
    pub fn normalized_toml(&self) -> &TomlManifest {
        &self.normalized_toml
    }
    pub fn summary(&self) -> &Summary {
        &self.summary
    }
    pub fn summary_mut(&mut self) -> &mut Summary {
        &mut self.summary
    }

    pub fn dependencies(&self) -> &[Dependency] {
        self.summary.dependencies()
    }
    pub fn default_kind(&self) -> Option<CompileKind> {
        self.default_kind
    }
    pub fn forced_kind(&self) -> Option<CompileKind> {
        self.forced_kind
    }
    pub fn exclude(&self) -> &[String] {
        &self.exclude
    }
    pub fn include(&self) -> &[String] {
        &self.include
    }
    pub fn metadata(&self) -> &ManifestMetadata {
        &self.metadata
    }
    pub fn name(&self) -> InternedString {
        self.package_id().name()
    }
    pub fn package_id(&self) -> PackageId {
        self.summary.package_id()
    }
    pub fn targets(&self) -> &[Target] {
        &self.targets
    }
    // It is used by cargo-c, please do not remove it
    pub fn targets_mut(&mut self) -> &mut [Target] {
        &mut self.targets
    }
    pub fn version(&self) -> &Version {
        self.package_id().version()
    }
    pub fn warnings_mut(&mut self) -> &mut Warnings {
        &mut self.warnings
    }
    pub fn warnings(&self) -> &Warnings {
        &self.warnings
    }
    pub fn profiles(&self) -> Option<&TomlProfiles> {
        self.normalized_toml.profile.as_ref()
    }
    pub fn publish(&self) -> &Option<Vec<String>> {
        &self.publish
    }
    pub fn replace(&self) -> &[(PackageIdSpec, Dependency)] {
        &self.replace
    }
    pub fn patch(&self) -> &HashMap<Url, Vec<Dependency>> {
        &self.patch
    }
    pub fn links(&self) -> Option<&str> {
        self.links.as_deref()
    }
    pub fn is_embedded(&self) -> bool {
        self.embedded
    }

    pub fn workspace_config(&self) -> &WorkspaceConfig {
        &self.workspace
    }

    /// Unstable, nightly features that are enabled in this manifest.
    pub fn unstable_features(&self) -> &Features {
        &self.unstable_features
    }

    /// The style of resolver behavior to use, declared with the `resolver` field.
    ///
    /// Returns `None` if it is not specified.
    pub fn resolve_behavior(&self) -> Option<ResolveBehavior> {
        self.resolve_behavior
    }

    /// `RUSTFLAGS` from the `[lints]` table
    pub fn lint_rustflags(&self) -> &[String] {
        self.lint_rustflags.as_slice()
    }

    pub fn hints(&self) -> Option<&Hints> {
        self.hints.as_ref()
    }

    pub fn map_source(self, to_replace: SourceId, replace_with: SourceId) -> Manifest {
        Manifest {
            summary: self.summary.map_source(to_replace, replace_with),
            ..self
        }
    }

    pub fn feature_gate(&self) -> CargoResult<()> {
        if self.im_a_teapot.is_some() {
            self.unstable_features
                .require(Feature::test_dummy_unstable())
                .with_context(|| {
                    "the `im-a-teapot` manifest key is unstable and may \n                     not work properly in England"
                })?;
        }

        if self.default_kind.is_some() || self.forced_kind.is_some() {
            self.unstable_features
                .require(Feature::per_package_target())
                .with_context(|| {
                    "the `package.default-target` and `package.forced-target` \n                     manifest keys are unstable and may not work properly"
                })?;
        }

        Ok(())
    }

    // Just a helper function to test out `-Z` flags on Cargo
    pub fn print_teapot(&self, gctx: &GlobalContext) {
        if let Some(teapot) = self.im_a_teapot {
            if gctx.cli_unstable().print_im_a_teapot {
                crate::drop_println!(gctx, "im-a-teapot = {}", teapot);
            }
        }
    }

    pub fn edition(&self) -> Edition {
        self.edition
    }

    pub fn rust_version(&self) -> Option<&RustVersion> {
        self.rust_version.as_ref()
    }

    pub fn custom_metadata(&self) -> Option<&toml::Value> {
        self.custom_metadata.as_ref()
    }

    pub fn default_run(&self) -> Option<&str> {
        self.default_run.as_deref()
    }

    pub fn metabuild(&self) -> Option<&Vec<String>> {
        self.metabuild.as_ref()
    }

    pub fn metabuild_path(&self, target_dir: Filesystem) -> PathBuf {
        let hash = short_hash(&self.package_id());
        target_dir
            .into_path_unlocked()
            .join(".metabuild")
            .join(format!("metabuild-{}-{}.rs", self.name(), hash))
    }
}
