use std::{
    fs::{self, File},
    io::Write,
    path::Path,
};

use git2;
use crate::git_operations::GitRepositoryOperations;

// Helper function to commit and push changes in a single submodule
pub fn commit_and_push_submodule(
    submodule_path: &Path,
    commit_message: &str,
    log_file: &mut File,
    dry_run: bool,
) -> Result<(), String> {
    writeln!(log_file, "--- Processing submodule: {:?} ---", submodule_path).map_err(|e| e.to_string())?;
    println!("--- Processing submodule: {:?} ---", submodule_path);

    let repo = git2::Repository::open(submodule_path)
        .map_err(|e| format!("Failed to open submodule repository at {:?}: {}", submodule_path, e))?;

    // Check for local changes
    let mut opts = git2::StatusOptions::new();
    opts.include_untracked(true);
    let statuses = repo.statuses(Some(&mut opts))
        .map_err(|e| format!("Failed to get submodule status: {}", e))?;

    if statuses.is_empty() {
        writeln!(log_file, "No local changes in submodule {:?}. Skipping.", submodule_path).map_err(|e| e.to_string())?;
        println!("No local changes in submodule {:?}. Skipping.", submodule_path);
        return Ok(())
    }

    writeln!(log_file, "Local changes detected in submodule {:?}.", submodule_path).map_err(|e| e.to_string())?;
    println!("Local changes detected in submodule {:?}.", submodule_path);

    if dry_run {
        writeln!(log_file, "[DRY RUN] Would stage all changes in {:?}.", submodule_path).map_err(|e| e.to_string())?;
        writeln!(log_file, "[DRY RUN] Would commit with message: '{}' in {:?}.", commit_message, submodule_path).map_err(|e| e.to_string())?;
        writeln!(log_file, "[DRY RUN] Would push changes to remote in {:?}.", submodule_path).map_err(|e| e.to_string())?;
        println!("[DRY RUN] Would stage all changes in {:?}.", submodule_path);
        println!("[DRY RUN] Would commit with message: '{}' in {:?}.", commit_message, submodule_path);
        println!("[DRY RUN] Would push changes to remote in {:?}.", submodule_path);
        return Ok(())
    }

    // Stage all changes
    let mut index = repo.index()
        .map_err(|e| format!("Failed to get submodule index: {}", e))?;
    index.add_all([""].iter(), git2::IndexAddOption::ADD_REMOVE | git2::IndexAddOption::ADD_UNTRACKED, None)
        .map_err(|e| format!("Failed to stage changes in submodule: {}", e))?;
    index.write()
        .map_err(|e| format!("Failed to write submodule index: {}", e))?;
    writeln!(log_file, "Staged all changes in submodule {:?}.", submodule_path).map_err(|e| e.to_string())?;
    println!("Staged all changes in submodule {:?}.", submodule_path);

    // Commit changes
    let tree_id = index.write_tree()
        .map_err(|e| format!("Failed to write submodule tree: {}", e))?;
    let tree = repo.find_tree(tree_id)
        .map_err(|e| format!("Failed to find submodule tree: {}", e))?;
    let signature = repo.signature()
        .map_err(|e| format!("Failed to get submodule signature: {}", e))?;
    let parent_commit = repo.head()
        .and_then(|head| head.peel_to_commit())
        .map_err(|e| format!("Failed to get submodule HEAD commit: {}", e))?;

    repo.commit(
        Some("HEAD"), // Update HEAD
        &signature,
        &signature,
        commit_message,
        &tree,
        &[&parent_commit],
    )
    .map_err(|e| format!("Failed to commit changes in submodule: {}", e))?;
    writeln!(log_file, "Committed changes in submodule {:?} with message: '{}'.", submodule_path, commit_message).map_err(|e| e.to_string())?;
    println!("Committed changes in submodule {:?} with message: '{}'.", submodule_path, commit_message);

    // Verify remote origin and branch (simplified check)
    let remote = repo.find_remote("origin")
        .map_err(|e| format!("Failed to find 'origin' remote in submodule {:?}: {}", submodule_path, e))?;
    let remote_url = remote.url().unwrap_or("unknown");
    writeln!(log_file, "Submodule {:?} remote origin URL: {}", submodule_path, remote_url).map_err(|e| e.to_string())?;
    println!("Submodule {:?} remote origin URL: {}", submodule_path, remote_url);

    // Basic check for meta-introspector org (can be made more robust)
    if !remote_url.contains("meta-introspector") {
        writeln!(log_file, "[WARNING] Submodule {:?} remote URL does not contain 'meta-introspector'.", submodule_path).map_err(|e| e.to_string())?;
        println!("[WARNING] Submodule {:?} remote URL does not contain 'meta-introspector'.", submodule_path);
    }

    let head_ref = repo.head()
        .map_err(|e| format!("Failed to get HEAD reference in submodule {:?}: {}", submodule_path, e))?;
    let current_branch = head_ref.shorthand().unwrap_or("unknown");
    writeln!(log_file, "Submodule {:?} current branch: {}", submodule_path, current_branch).map_err(|e| e.to_string())?;
    println!("Submodule {:?} current branch: {}", submodule_path, current_branch);

    // Basic check for feature/CRQ-016-nixify branch (can be made more robust)
    if current_branch != "feature/CRQ-016-nixify" {
        writeln!(log_file, "[WARNING] Submodule {:?} is not on 'feature/CRQ-016-nixify' branch.", submodule_path).map_err(|e| e.to_string())?;
        println!("[WARNING] Submodule {:?} is not on 'feature/CRQ-016-nixify' branch.", submodule_path);
    }

    // Push changes
    let mut callbacks = git2::RemoteCallbacks::new();
    callbacks.credentials(|_url, _username_from_url, _allowed_types| {
        // You might need to implement a more robust credential helper here
        // For now, assuming credentials are handled by git config or agent
        Err(git2::Error::from_str("No credential callback configured"))
    });

    let mut push_options = git2::PushOptions::new();
    push_options.remote_callbacks(callbacks);

    let mut remote = repo.find_remote("origin")
        .map_err(|e| format!("Failed to find 'origin' remote for push in submodule {:?}: {}", submodule_path, e))?;
    
    let refspec = format!("HEAD:refs/heads/{}", current_branch);
    remote.push(&[refspec], Some(&mut push_options))
        .map_err(|e| format!("Failed to push changes in submodule {:?}: {}", submodule_path, e))?;
    writeln!(log_file, "Pushed changes in submodule {:?}.", submodule_path).map_err(|e| e.to_string())?;
    println!("Pushed changes in submodule {:?}.", submodule_path);

    Ok(())
}

// Function to generate [patch.crates-io] entries for submodules
pub fn generate_submodule_patches(
    current_dir: &Path,
    output_file: &str, // This will now be the base name for generated config files
    dry_run: bool,
    overwrite: bool,
    _git_url_template: Option<&String>, // Not used in this iteration
    _branch_template: Option<&String>, // Not used in this iteration
    recursive: bool,
) -> Result<String, String> {
    let repo = git2::Repository::open_repository(current_dir)?;

    let mut generated_files_info = Vec::new();

    for submodule_info in repo.submodules()? {
        let submodule_path = current_dir.join(&submodule_info.path);
        let submodule_name = &submodule_info.name;

        println!("Processing submodule: {}", submodule_name);

        let submodule_cargo_dir = submodule_path.join(".cargo");
        let submodule_config_path = submodule_cargo_dir.join(output_file); // output_file is now the config filename

        if !submodule_cargo_dir.exists() {
            if dry_run {
                println!("[DRY RUN] Would create directory: {:?}", submodule_cargo_dir);
            } else {
                fs::create_dir_all(&submodule_cargo_dir)
                    .map_err(|e| format!("Failed to create .cargo directory for submodule {:?}: {}", submodule_path, e))?;
                println!("  Created directory: {:?}", submodule_cargo_dir);
            }
        }

        let mut config_content = String::new();
        config_content.push_str("[source.crates-io]\n");
        config_content.push_str("replace-with = \"vendored-sources\"\n\n");
        config_content.push_str("[source.vendored-sources]\n");
        config_content.push_str("directory = \"vendor\"\n");

        // TODO: Add [patch.crates-io] entries here later

        if dry_run {
            println!("\n--- Generated .cargo/config.toml for {} (Dry Run) ---", submodule_name);
            println!("{}", config_content);
            println!("----------------------------------------------------\n");
            generated_files_info.push(format!("[DRY RUN] Would generate config for {} at {:?}", submodule_name, submodule_config_path));
        } else {
            if submodule_config_path.exists() && !overwrite {
                println!("  Config file {:?} already exists. Skipping (use --overwrite to force).", submodule_config_path);
                generated_files_info.push(format!("Skipped config for {} at {:?}", submodule_name, submodule_config_path));
            } else {
                fs::write(&submodule_config_path, config_content)
                    .map_err(|e| format!("Failed to write config file for submodule {:?}: {}", submodule_path, e))?;
                println!("  Generated config file: {:?}", submodule_config_path);
                generated_files_info.push(format!("Generated config for {} at {:?}", submodule_name, submodule_config_path));
            }
        }

        // Handle recursive submodules if enabled
        if recursive {
            // TODO: Implement recursive submodule processing
            println!("  Recursive processing for nested submodules is not yet implemented.");
        }
    }

    Ok(format!("Submodule config generation completed.\n{}", generated_files_info.join("\n")))
}
