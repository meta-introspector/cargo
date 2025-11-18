use std::{collections::HashMap, env, fs, path::{Path, PathBuf}};
use std::process::Command;
use toml_edit;

pub trait WorkspaceGenerator {
    fn generate_workspace_dependencies(&self, root_dir: &Path, dry_run: bool) -> Result<(), Box<dyn std::error::Error>>;
}

pub struct DefaultWorkspaceGenerator;

impl WorkspaceGenerator for DefaultWorkspaceGenerator {
    fn generate_workspace_dependencies(&self, root_dir: &Path, dry_run: bool) -> Result<(), Box<dyn std::error::Error>> {
        println!("Generating comprehensive [workspace.dependencies] section...");

        let submodules_dir = root_dir.join("submodules");
        let generated_deps_path = root_dir.join("generated_workspace_deps.toml");

        // 1. Run `cargo metadata`
        let output = Command::new("cargo")
            .arg("metadata")
            .arg("--format-version")
            .arg("1")
            .current_dir(root_dir)
            .output()
            .map_err(|e| format!("Failed to execute cargo metadata: {}", e))?;

        if !output.status.success() {
            return Err(format!(
                "cargo metadata failed:\nStdout: {}\nStderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ).into());
        }

        let metadata: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| format!("Failed to parse cargo metadata output: {}", e))?;

        let mut all_dependencies: HashMap<String, String> = HashMap::new(); // name -> version

        // Iterate over all packages in the workspace
        if let Some(packages) = metadata["packages"].as_array() {
            for pkg in packages {
                if let Some(name) = pkg["name"].as_str() {
                    if let Some(version) = pkg["version"].as_str() {
                        // Only add if not already present or if new version is higher
                        let current_version = all_dependencies.get(name);
                        if current_version.is_none() || (current_version.is_some() && version > current_version.unwrap()) {
                            all_dependencies.insert(name.to_string(), version.to_string());
                        }
                    }
                }
            }
        }

        // Get a list of submodule names
        let mut submodule_names: Vec<String> = fs::read_dir(&submodules_dir)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                if entry.file_type().ok()?.is_dir() {
                    entry.file_name().into_string().ok()
                } else {
                    None
                }
            })
            .collect();
        submodule_names.sort();

        let mut doc = toml_edit::DocumentMut::new();
        let mut workspace_deps_table = toml_edit::Table::new();

        for (dep_name, dep_version) in all_dependencies.iter() {
            let mut is_submodule = false;
            let mut submodule_path = PathBuf::new();

            // Check for direct submodule match
            if submodule_names.contains(dep_name) {
                is_submodule = true;
                submodule_path = submodules_dir.join(dep_name);
            } else {
                // Check for nested submodules (e.g., time-rs/time)
                for sm_name in &submodule_names {
                    let potential_path = submodules_dir.join(sm_name).join(dep_name);
                    if potential_path.exists() && potential_path.is_dir() {
                        is_submodule = true;
                        submodule_path = potential_path;
                        break;
                    }
                }
            }

            let mut dep_table = toml_edit::Table::new();
            if is_submodule {
                dep_table.insert("path", toml_edit::value(format!("./{}", submodule_path.strip_prefix(root_dir).unwrap().to_string_lossy())));
            } else {
                dep_table.insert("version", toml_edit::value(dep_version.clone()));
            }
            workspace_deps_table.insert(dep_name, toml_edit::Item::Table(dep_table));
        }

        doc.insert("workspace", toml_edit::Item::Table(toml_edit::Table::new()));
        doc["workspace"].as_table_mut().unwrap().insert("dependencies", toml_edit::Item::Table(workspace_deps_table));

        if dry_run {
            println!("--- DRY RUN: Generated [workspace.dependencies] content ---");
            println!("{}", doc.to_string());
            println!("---------------------------------------------");
        } else {
            fs::write(&generated_deps_path, doc.to_string())?;
            println!("Successfully generated [workspace.dependencies] to {:?}", generated_deps_path);
        }

        Ok(())
    }
}
