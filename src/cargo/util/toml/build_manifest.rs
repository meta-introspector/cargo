use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc; // Changed from Rc
use std::str::{self, FromStr};

use anyhow::{Context as _, anyhow, bail};
use cargo_platform::Platform;
use cargo_util::paths;
use cargo_util_schemas::manifest::{
    self, PackageName, PathBaseName, TomlDependency, TomlDetailedDependency, TomlManifest,
    TomlPackageBuild,
    TomlWorkspace,
};
use cargo_util_schemas::manifest::{RustVersion, StringOrBool};
use itertools::Itertools;
use pathdiff::diff_paths;
use url::Url;

use crate::core::compiler::{CompileKind, CompileTarget};
use crate::core::dependency::{Artifact, ArtifactTarget, DepKind};
use crate::core::manifest::{ManifestMetadata, TargetSourcePath};
use crate::core::resolver::ResolveBehavior;
use crate::core::{CliUnstable, FeatureValue, find_workspace_root, resolve_relative_path};
use crate::core::{Dependency, Manifest, Package, PackageId, Summary, Target};
use crate::core::{Edition, EitherManifest, Feature, Features, VirtualManifest, Workspace};
use crate::core::{GitReference, PackageIdSpec, SourceId, WorkspaceConfig, WorkspaceRootConfig};
use crate::sources::{CRATES_IO_INDEX, CRATES_IO_REGISTRY};
use crate::util::errors::{CargoResult, ManifestError};
use crate::util::interning::InternedString;
use crate::util::lints::{get_key_value_span, rel_cwd_manifest_path};
use crate::util::{
    self, GlobalContext, IntoUrl, OnceExt, OptVersionReq, context::ConfigRelativePath,
    context::TOP_LEVEL_CONFIG_KEYS,
};

// Functions from mod.rs that will be needed:
use super::{ManifestContext, dep_to_dependency, lints_to_rustflags, normalize_dependencies, normalize_features, normalize_patch, normalize_package_toml, normalize_package_readme, deprecated_underscore, unique_build_targets, validate_dependencies, validate_profiles, warn_on_unused, emit_toml_diagnostic, emit_frontmatter_diagnostic, lookup_path_base, field_inherit_with, lints_inherit_with, dependency_inherit_with, inner_dependency_inherit_with, deprecated_ws_default_features, to_workspace_config, to_workspace_root_config, load_inheritable_fields, InheritableFields};
use super::targets::to_targets;
use super::embedded; // For embedded::expand_manifest and embedded::sanitize_name

#[tracing::instrument(skip_all)]
pub fn to_real_manifest(
    contents: String,
    document: toml::Spanned<toml::de::DeTable<'static>>,
    original_toml: manifest::TomlManifest,
    normalized_toml: manifest::TomlManifest,
    features: Features,
    workspace_config: WorkspaceConfig,
    source_id: SourceId,
    manifest_file: &Path,
    is_embedded: bool,
    gctx: &GlobalContext,
    warnings: &mut Vec<String>,
    _errors: &mut Vec<String>,
) -> CargoResult<Manifest> {
    let package_root = manifest_file.parent().unwrap();
    if !package_root.is_dir() {
        bail!(
            "package root '{}' is not a directory",
            package_root.display()
        );
    };

    let normalized_package = normalized_toml
        .package()
        .expect("previously verified to have a `[package]`");
    let package_name = normalized_package
        .normalized_name()
        .expect("previously normalized");
    if package_name.contains(':') {
        features.require(Feature::open_namespaces())?;
    }
    let rust_version = normalized_package
        .normalized_rust_version()
        .expect("previously normalized")
        .cloned();

    let edition = if let Some(edition) = normalized_package
        .normalized_edition()
        .expect("previously normalized")
    {
        let edition: Edition = edition
            .parse()
            .context("failed to parse the `edition` key")?;
        if let Some(pkg_msrv) = &rust_version {
            if let Some(edition_msrv) = edition.first_version() {
                let edition_msrv = RustVersion::try_from(edition_msrv).unwrap();
                if !edition_msrv.is_compatible_with(pkg_msrv.as_partial()) {
                    bail!(
                        "rust-version {} is incompatible with the version ({}) required by \
                            the specified edition ({})",
                        pkg_msrv,
                        edition_msrv,
                        edition,
                    )
                }
            }
        }
        edition
    } else {
        let msrv_edition = if let Some(pkg_msrv) = &rust_version {
            Edition::ALL
                .iter()
                .filter(|e| {
                    e.first_version()
                        .map(|e| {
                            let e = RustVersion::try_from(e).unwrap();
                            e.is_compatible_with(pkg_msrv.as_partial())
                        })
                        .unwrap_or_default()
                })
                .max()
                .copied()
        } else {
            None
        }
        .unwrap_or_default();
        let default_edition = Edition::default();
        let latest_edition = Edition::LATEST_STABLE;

        // We're trying to help the user who might assume they are using a new edition,
        // so if they can't use a new edition, don't bother to tell them to set it.
        // This also avoids having to worry about whether `package.edition` is compatible with
        // their MSRV.
        if msrv_edition != default_edition || rust_version.is_none() {
            let tip = if msrv_edition == latest_edition || rust_version.is_none() {
                format!(" while the latest is {latest_edition}")
            } else {
                format!(" while {msrv_edition} is compatible with `rust-version`")
            };
            warnings.push(format!(
                "no edition set: defaulting to the {default_edition} edition{tip}",
            ));
        }
        default_edition
    };
    if !edition.is_stable() {
        features.require(Feature::unstable_editions())?;
    }

    if original_toml.project.is_some() {
        if Edition::Edition2024 <= edition {
            anyhow::bail!(
                "`[project]` is not supported as of the 2024 Edition, please use `[package]`"
            );
        } else {
            warnings.push(format!("`[project]` is deprecated in favor of `[package]`"));
        }
    }

    if normalized_package.metabuild.is_some() {
        features.require(Feature::metabuild())?;
    }

    if is_embedded {
        let manifest::TomlManifest {
            cargo_features: _,
            package: _,
            project: _,
            badges: _,
            features: _,
            lib,
            bin,
            example,
            test,
            bench,
            dependencies: _,
            dev_dependencies: _,
            dev_dependencies2: _,
            build_dependencies,
            build_dependencies2,
            target: _,
            lints: _,
            hints: _,
            workspace,
            profile: _,
            patch: _,
            replace: _,
            _unused_keys: _,
        } = &original_toml;
        let mut invalid_fields = vec![
            ("`workspace`", workspace.is_some()),
            ("`lib`", lib.is_some()),
            ("`bin`", bin.is_some()),
            ("`example`", example.is_some()),
            ("`test`", test.is_some()),
            ("`bench`", bench.is_some()),
            ("`build-dependencies`", build_dependencies.is_some()),
            ("`build_dependencies`", build_dependencies2.is_some()),
        ];
        if let Some(package) = original_toml.package() {
            let manifest::TomlPackage {
                edition: _,
                rust_version: _,
                name: _,
                version: _,
                authors: _,
                build,
                metabuild,
                default_target: _,
                forced_target: _,
                links,
                exclude: _,
                include: _,
                publish: _,
                workspace,
                im_a_teapot: _,
                autolib,
                autobins,
                autoexamples,
                autotests,
                autobenches,
                default_run,
                description: _,
                homepage: _,
                documentation: _,
                readme: _,
                keywords: _,
                categories: _,
                license: _,
                license_file: _,
                repository: _,
                resolver: _,
                metadata: _,
                _invalid_cargo_features: _,
            } = package.as_ref();
            invalid_fields.extend([
                ("`package.workspace`", workspace.is_some()),
                ("`package.build`", build.is_some()),
                ("`package.metabuild`", metabuild.is_some()),
                ("`package.links`", links.is_some()),
                ("`package.autolib`", autolib.is_some()),
                ("`package.autobins`", autobins.is_some()),
                ("`package.autoexamples`", autoexamples.is_some()),
                ("`package.autotests`", autotests.is_some()),
                ("`package.autobenches`", autobenches.is_some()),
                ("`package.default-run`", default_run.is_some()),
            ]);
        }
        let invalid_fields = invalid_fields
            .into_iter()
            .filter_map(|(name, invalid)| invalid.then_some(name))
            .collect::<Vec<_>>();
        if !invalid_fields.is_empty() {
            let fields = invalid_fields.join(", ");
            let are = if invalid_fields.len() == 1 {
                "is"
            } else {
                "are"
            };
            anyhow::bail!("{fields} {are} not allowed in embedded manifests")
        }
    }

    let resolve_behavior = match (
        normalized_package.resolver.as_ref(),
        normalized_toml
            .workspace
            .as_ref()
            .and_then(|ws| ws.resolver.as_ref()),
    ) {
        (None, None) => None,
        (Some(s), None) | (None, Some(s)) => Some(ResolveBehavior::from_manifest(s)?),
        (Some(_), Some(_)) => {
            bail!("cannot specify `resolver` field in both `[workspace]` and `[package]`")
        }
    };

    // If we have no lib at all, use the inferred lib, if available.
    // If we have a lib with a path, we're done.
    // If we have a lib with no path, use the inferred lib or else the package name.
    let targets = to_targets(
        &features,
        &original_toml,
        &normalized_toml,
        package_root,
        edition,
        &normalized_package.metabuild,
        warnings,
    )?;

    if targets.iter().all(|t| t.is_custom_build()) {
        bail!(
            "no targets specified in the manifest\n\        either src/lib.rs, src/main.rs, a [lib] section, or \        [[bin]] section must be present"
        )
    }

    if let Err(conflict_targets) = unique_build_targets(&targets, package_root) {
        conflict_targets
            .iter() 
            .for_each(|(target_path, conflicts)| {
                warnings.push(format!(
                    "file `{}` found to be present in multiple \        build targets:\n{}",
                    target_path.display(),
                    conflicts
                        .iter()
                        .map(|t| format!("  * `{}` target `{}`", t.kind().description(), t.name(),))
                        .join("\n")
                ));
            })
    }

    if let Some(links) = &normalized_package.links {
        if !targets.iter().any(|t| t.is_custom_build()) {
            bail!(
                "package specifies that it links to `{links}` but does not have a custom build script"
            )
        }
    }

    validate_dependencies(original_toml.dependencies.as_ref(), None, None, warnings)?;
    validate_dependencies(
        original_toml.dev_dependencies(),
        None,
        Some(DepKind::Development),
        warnings,
    )?;
    validate_dependencies(
        original_toml.build_dependencies(),
        None,
        Some(DepKind::Build),
        warnings,
    )?;
    for (name, platform) in original_toml.target.iter().flatten() {
        let platform_kind: Platform = name.parse()?;
        platform_kind.check_cfg_attributes(warnings);
        platform_kind.check_cfg_keywords(warnings, manifest_file);
        let platform_kind = Some(platform_kind);
        validate_dependencies(
            platform.dependencies.as_ref(),
            platform_kind.as_ref(),
            None,
            warnings,
        )?;
        validate_dependencies(
            platform.build_dependencies(),
            platform_kind.as_ref(),
            Some(DepKind::Build),
        )?;
        validate_dependencies(
            platform.dev_dependencies(),
            platform_kind.as_ref(),
            Some(DepKind::Development),
            warnings,
        )?;
    }

    // Collect the dependencies.
    let mut deps = Vec::new();
    let mut manifest_ctx = ManifestContext {
        deps: &mut deps,
        source_id,
        gctx,
        warnings,
        platform: None,
        root: package_root,
    };
    gather_dependencies(
        &mut manifest_ctx,
        normalized_toml.dependencies.as_ref(),
        None,
    )?;
    gather_dependencies(
        &mut manifest_ctx,
        normalized_toml.dev_dependencies(),
        Some(DepKind::Development),
    )?;
    gather_dependencies(
        &mut manifest_ctx,
        normalized_toml.build_dependencies(),
        Some(DepKind::Build),
    )?;
    for (name, platform) in normalized_toml.target.iter().flatten() {
        manifest_ctx.platform = Some(name.parse()?);
        gather_dependencies(&mut manifest_ctx, platform.dependencies.as_ref(), None)?;
        gather_dependencies(
            &mut manifest_ctx,
            platform.build_dependencies(),
            Some(DepKind::Build),
        )?;
        gather_dependencies(
            &mut manifest_ctx,
            platform.dev_dependencies(),
            Some(DepKind::Development),
        )?;
    }
    let replace = replace(&normalized_toml, &mut manifest_ctx)?;
    let patch = patch(&normalized_toml, &mut manifest_ctx)?;

    {
        let mut names_sources = BTreeMap::new();
        for dep in &deps {
            let name = dep.name_in_toml();
            let prev = names_sources.insert(name, dep.source_id());
            if prev.is_some() && prev != Some(dep.source_id()) {
                bail!(
                    "Dependency '{}' has different source paths depending on the build \        target. Each dependency must have a single canonical source path \        irrespective of build target.",
                    name
                );
            }
        }
    }

    verify_lints(
        normalized_toml
            .normalized_lints()
            .expect("previously normalized"),
        gctx,
        warnings,
    )?;
    let default = manifest::TomlLints::default();
    let rustflags = lints_to_rustflags(
        normalized_toml
            .normalized_lints()
            .expect("previously normalized")
            .unwrap_or(&default),
    )?;

    let hints = normalized_toml.hints.clone();

    let metadata = ManifestMetadata {
        description: normalized_package
            .normalized_description()
            .expect("previously normalized")
            .cloned(),
        homepage: normalized_package
            .normalized_homepage()
            .expect("previously normalized")
            .cloned(),
        documentation: normalized_package
            .normalized_documentation()
            .expect("previously normalized")
            .cloned(),
        readme: normalized_package
            .normalized_readme()
            .expect("previously normalized")
            .cloned(),
        authors: normalized_package
            .normalized_authors()
            .expect("previously normalized")
            .cloned()
            .unwrap_or_default(),
        license: normalized_package
            .normalized_license()
            .expect("previously normalized")
            .cloned(),
        license_file: normalized_package
            .normalized_license_file()
            .expect("previously normalized")
            .cloned(),
        repository: normalized_package
            .normalized_repository()
            .expect("previously normalized")
            .cloned(),
        keywords: normalized_package
            .normalized_keywords()
            .expect("previously normalized")
            .cloned()
            .unwrap_or_default(),
        categories: normalized_package
            .normalized_categories()
            .expect("previously normalized")
            .cloned()
            .unwrap_or_default(),
        badges: normalized_toml.badges.clone().unwrap_or_default(),
        links: normalized_package.links.clone(),
        rust_version: rust_version.clone(),
    };

    if let Some(profiles) = &normalized_toml.profile {
        let cli_unstable = gctx.cli_unstable();
        validate_profiles(profiles, cli_unstable, &features, warnings)?;
    }

    let version = normalized_package
        .normalized_version()
        .expect("previously normalized");
    let publish = match normalized_package
        .normalized_publish()
        .expect("previously normalized")
    {
        Some(manifest::VecStringOrBool::VecString(vecstring)) => Some(vecstring.clone()),
        Some(manifest::VecStringOrBool::Bool(false)) => Some(vec![]),
        Some(manifest::VecStringOrBool::Bool(true)) => None,
        None => version.is_none().then_some(vec![]),
    };

    if version.is_none() && publish != Some(vec![]) {
        bail!("`package.publish` requires `package.version` be specified");
    }

    let pkgid = PackageId::new(
        package_name.as_str().into(),
        version
            .cloned()
            .unwrap_or_else(|| semver::Version::new(0, 0, 0)),
        source_id,
    );
    let summary = {
        let summary = Summary::new(
            pkgid,
            deps,
            &normalized_toml
                .features
                .as_ref()
                .unwrap_or(&Default::default())
                .iter()
                .map(|(k, v)| {
                    (
                        k.to_string().into(),
                        v.iter().map(InternedString::from).collect(),
                    )
                })
                .collect(),
            normalized_package.links.as_deref(),
            rust_version.clone(),
        );
        // edition2024 stops exposing implicit features, which will strip weak optional dependencies from `dependencies`,
        // need to check whether `dep_name` is stripped as unused dependency
        if let Err(ref err) = summary {
            if let Some(missing_dep) = err.downcast_ref::<MissingDependencyError>() {
                missing_dep_diagnostic(
                    missing_dep,
                    &original_toml,
                    &document,
                    &contents,
                    manifest_file,
                    gctx,
                )?;
            }
        }
        summary? 
    };

    if summary.features().contains_key("default-features") {
        warnings.push(
            "`[features]` defines a feature named `default-features`\nnote: only a feature named `default` will be enabled by default"
                .to_string(),
        )
    }

    if let Some(run) = &normalized_package.default_run {
        if !targets
            .iter()
            .filter(|t| t.is_bin())
            .any(|t| t.name() == run)
        {
            let suggestion = util::closest_msg(
                run,
                targets.iter().filter(|t| t.is_bin()),
                |t| t.name(),
                "target",
            );
            bail!("default-run target `{}` not found{}", run, suggestion);
        }
    }

    let default_kind = normalized_package
        .default_target
        .as_ref()
        .map(|t| CompileTarget::new(&*t))
        .transpose()?
        .map(CompileKind::Target);
    let forced_kind = normalized_package
        .forced_target
        .as_ref()
        .map(|t| CompileTarget::new(&*t))
        .transpose()?
        .map(CompileKind::Target);
    let include = normalized_package
        .normalized_include()
        .expect("previously normalized")
        .cloned()
        .unwrap_or_default();
    let exclude = normalized_package
        .normalized_exclude()
        .expect("previously normalized")
        .cloned()
        .unwrap_or_default();
    let links = normalized_package.links.clone();
    let custom_metadata = normalized_package.metadata.clone();
    let im_a_teapot = normalized_package.im_a_teapot;
    let default_run = normalized_package.default_run.clone();
    let metabuild = normalized_package.metabuild.clone().map(|sov| sov.0);
    let manifest = Manifest::new(
        Arc::new(contents), // Changed from Rc::new
        Arc::new(document), // Changed from Rc::new
        Arc::new(original_toml), // Changed from Rc::new
        Arc::new(normalized_toml), // Changed from Rc::new
        summary,
        default_kind,
        forced_kind,
        targets,
        exclude,
        include,
        links,
        metadata,
        custom_metadata,
        publish,
        replace,
        patch,
        workspace_config,
        features,
        edition,
        rust_version,
        im_a_teapot,
        default_run,
        metabuild,
        resolve_behavior,
        rustflags,
        hints,
        is_embedded,
    );
    if manifest
        .normalized_toml()
        .package()
        .unwrap()
        .license_file
        .is_some()
        && manifest
            .normalized_toml()
            .package()
            .unwrap()
            .license
            .is_some()
    {
        warnings.push(
            "only one of `license` or `license-file` is necessary\n\        `license` should be used if the package license can be expressed \        with a standard SPDX expression.\n\        `license-file` should be used if the package uses a non-standard license.\n\        See https://doc.rust-lang.org/cargo/reference/manifest.html#the-license-and-license-file-fields \        for more information."
                .to_owned(),
        );
    }
    warn_on_unused(&manifest.original_toml()._unused_keys, warnings);

    manifest.feature_gate()?;

    Ok(manifest)
}

fn missing_dep_diagnostic(
    missing_dep: &MissingDependencyError,
    orig_toml: &TomlManifest,
    document: &toml::Spanned<toml::de::DeTable<'static>>,
    contents: &str,
    manifest_file: &Path,
    gctx: &GlobalContext,
) -> CargoResult<()> {
    let dep_name = missing_dep.dep_name;
    let manifest_path = rel_cwd_manifest_path(manifest_file, gctx);
    let feature_span =
        get_key_value_span(&document, &["features", missing_dep.feature.as_str()]).unwrap();

    let title = format!(
        "feature `{}` includes `{}`, but `{}` is not a dependency",
        missing_dep.feature,
        missing_dep.feature_value,
        &dep_name
    );
    let help = format!("enable the dependency with `dep:{dep_name}`");
    let info_label = format!(
        "`{}` is an unused optional dependency since no feature enables it",
        &dep_name
    );
    let group = Group::with_title(Level::ERROR.primary_title(&title));
    let snippet = Snippet::source(contents)
        .path(manifest_path)
        .annotation(AnnotationKind::Primary.span(feature_span.value));
    let group = if missing_dep.weak_optional {
        let mut orig_deps = vec![
            (
                orig_toml.dependencies.as_ref(),
                vec![DepKind::Normal.kind_table()],
            ),
            (
                orig_toml.build_dependencies.as_ref(),
                vec![DepKind::Build.kind_table()],
            ),
        ];
        for (name, platform) in orig_toml.target.iter().flatten() {
            orig_deps.push((
                platform.dependencies.as_ref(),
                vec!["target", name, DepKind::Normal.kind_table()],
            ));
            orig_deps.push((
                platform.build_dependencies.as_ref(),
                vec!["target", name, DepKind::Normal.kind_table()],
            ));
        }

        if let Some((_, toml_path)) = orig_deps.iter().find(|(deps, _)| {
            if let Some(deps) = deps {
                deps.keys().any(|p| *p.as_str() == *dep_name)
            } else {
                false
            }
        }) {
            let toml_path = toml_path
                .iter()
                .map(|s| *s)
                .chain(std::iter::once(dep_name.as_str()))
                .collect::<Vec<_>>();
            let dep_span = get_key_value_span(&document, &toml_path).unwrap();

            group
                .element(
                    snippet
                        .annotation(AnnotationKind::Context.span(dep_span.key).label(info_label)),
                )
                .element(Level::HELP.message(help))
        } else {
            group.element(snippet)
        }
    } else {
        group.element(snippet)
    };

    if let Err(err) = gctx.shell().print_report(&[group], true) {
        return Err(err.into());
    }
    Err(AlreadyPrintedError::new(anyhow!("").into()).into())
}

fn to_virtual_manifest(
    contents: String,
    document: toml::Spanned<toml::de::DeTable<'static>>,
    original_toml: manifest::TomlManifest,
    normalized_toml: manifest::TomlManifest,
    features: Features,
    workspace_config: WorkspaceConfig,
    source_id: SourceId,
    manifest_file: &Path,
    gctx: &GlobalContext,
    warnings: &mut Vec<String>,
    _errors: &mut Vec<String>,
) -> CargoResult<VirtualManifest> {
    let root = manifest_file.parent().unwrap();

    let mut deps = Vec::new();
    let (replace, patch) = {
        let mut manifest_ctx = ManifestContext {
            deps: &mut deps,
            source_id,
            gctx,
            warnings,
            platform: None,
            root,
        };
        (
            replace(&normalized_toml, &mut manifest_ctx)?,
            patch(&normalized_toml, &mut manifest_ctx)?,
        )
    };
    if let Some(profiles) = &normalized_toml.profile {
        validate_profiles(profiles, gctx.cli_unstable(), &features, warnings)?;
    }
    let resolve_behavior = normalized_toml
        .workspace
        .as_ref()
        .and_then(|ws| ws.resolver.as_deref())
        .map(|r| ResolveBehavior::from_manifest(r))
        .transpose()?;
    if let WorkspaceConfig::Member { .. } = &workspace_config {
        bail!("virtual manifests must be configured with [workspace]");
    }
    let manifest = VirtualManifest::new(
        Arc::new(contents), // Changed from Rc::new
        Arc::new(document),
        Arc::new(original_toml),
        Arc::new(normalized_toml),
        replace,
        patch,
        workspace_config,
        features,
        resolve_behavior,
    );

    warn_on_unused(&manifest.original_toml()._unused_keys, warnings);

    Ok(manifest)
}
