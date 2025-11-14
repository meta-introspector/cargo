use std::{env, fs::File, path::{Path, PathBuf}};

// Import modules from the library crate
use cargo_git_manage::{
    cli,
    submodule_manager::{self, commit_and_push_submodule, generate_submodule_patches},
    plan_manager::{self, CargoUpdateCommand, CargoVendorCommand, Cargo2NixCommand, CargoCommand, Plan, Task, get_cargo_command, rename_cargo_config, restore_cargo_config},
    git_operations::{self, GitRepositoryOperations}, // Assuming GitRepositoryOperations is needed in main for some reason, otherwise remove
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = cli::cli().get_matches();
    let dry_run = *matches.get_one::<bool>("dry-run").unwrap_or(&false);

    match matches.subcommand() {
        Some(("submodule", sub_matches)) => {
            match sub_matches.subcommand() {
                Some(("add", add_matches)) => {
                    let url = add_matches.get_one::<String>("url").expect("URL is required");
                    let path = add_matches.get_one::<String>("path").expect("Path is required");
                    println!("Adding submodule: URL={}, Path={}", url, path);
                    // Call submodule_manager::add_submodule(...)
                },
                Some(("remove", remove_matches)) => {
                    let path = remove_matches.get_one::<String>("path").expect("Path is required");
                    println!("Removing submodule: Path={}", path);
                    // Call submodule_manager::remove_submodule(...)
                },
                Some(("commit-and-push", commit_matches)) => {
                    let message = commit_matches.get_one::<String>("message").expect("Commit message is required");
                    println!("Committing and pushing submodules with message: {}", message);

                    let current_dir = env::current_dir().expect("Failed to get current directory");

                    // Create logs directory if it doesn't exist
                    let logs_dir = current_dir.join("logs");
                    fs::create_dir_all(&logs_dir)
                        .map_err(|e| format!("Failed to create logs directory: {}", e))?;

                    let log_file_path = logs_dir.join("submodule_commit_push.log");
                    let mut log_file = File::create(&log_file_path)
                        .map_err(|e| format!("Failed to create log file at {:?}: {}", log_file_path, e))?;

                    writeln!(log_file, "--- Submodule Commit and Push Log (Dry Run: {})", dry_run)?;
                    println!("Logging submodule commit and push output to {:?}", log_file_path);

                    let repo = git2::Repository::open(&current_dir)
                        .map_err(|e| format!("Failed to open parent repository at {:?}: {}", current_dir, e))?;

                    for submodule in repo.submodules()
                        .map_err(|e| format!("Failed to iterate submodules: {}", e))?
                    {
                        let submodule_path = current_dir.join(submodule.path());
                        match commit_and_push_submodule(&submodule_path, message, &mut log_file, dry_run) {
                            Ok(_) => writeln!(log_file, "Successfully processed submodule: {:?}", submodule_path)?,
                            Err(e) => {
                                writeln!(log_file, "Error processing submodule {:?}: {}", submodule_path, e)?;
                                eprintln!("Error processing submodule {:?}: {}", submodule_path, e);
                                return Err(e.into());
                            }
                        }
                    }
                    writeln!(log_file, "Submodule commit and push process completed.")?;
                    println!("Submodule commit and push process completed.");
                },
                Some(("patch", patch_matches)) => {
                    let output_file = patch_matches.get_one::<String>("output").expect("Output file is required");
                    let dry_run_patch = *patch_matches.get_one::<bool>("dry-run").unwrap_or(&false);
                    let overwrite = *patch_matches.get_one::<bool>("overwrite").unwrap_or(&false);
                    let git_url_template = patch_matches.get_one::<String>("git-url-template");
                    let branch_template = patch_matches.get_one::<String>("branch-template");
                    let recursive = *patch_matches.get_one::<bool>("recursive").unwrap_or(&false);

                    let current_dir = env::current_dir().expect("Failed to get current directory");

                    match generate_submodule_patches(
                        &current_dir,
                        output_file,
                        dry_run_patch, // Use dry_run_patch for this subcommand
                        overwrite,
                        git_url_template,
                        branch_template,
                        recursive,
                    ) {
                        Ok(message) => println!("{}", message),
                        Err(e) => eprintln!("Error generating submodule patches: {}", e),
                    }
                },
                _ => unreachable!(),
            }
        },
        Some(("plan", sub_matches)) => {
            match sub_matches.subcommand() {
                Some(("generate", _)) => {
                    println!("Plan generate subcommand invoked.");
                    let current_dir = env::current_dir().expect("Failed to get current directory");
                    let tasks_dir = current_dir.join("../../../tasks"); // Adjust path to reach the root tasks directory

                    let mut tasks_map = HashMap::new();

                    for entry in walkdir::WalkDir::new(&tasks_dir)
                        .into_iter()
                        .filter_map(|e| e.ok())
                        .filter(|e| e.file_type().is_file() && e.path().extension().map_or(false, |ext| ext == "toml"))
                    {
                        let task_path = entry.path();
                        let task_content = fs::read_to_string(task_path)
                            .map_err(|e| format!("Failed to read task file {:?}: {}", task_path, e))?;
                        let task: Task = toml::from_str(&task_content)
                            .map_err(|e| format!("Failed to parse task file {:?}: {}", task_path, e))?;
                        tasks_map.insert(task.name.clone(), task);
                    }

                    let plan = Plan { tasks: tasks_map };
                    let plan_toml = toml::to_string_pretty(&plan)
                        .map_err(|e| format!("Failed to serialize plan to TOML: {}", e))?;

                    let plan_lock_path = current_dir.join("plan.lock");
                    fs::write(&plan_lock_path, plan_toml)
                        .map_err(|e| format!("Failed to write plan.lock file {:?}: {}", plan_lock_path, e))?;

                    println!("Generated plan.lock at {:?}", plan_lock_path);
                },
                Some(("run", run_matches)) => {
                    println!("Plan run subcommand invoked.");
                    let current_dir = env::current_dir().expect("Failed to get current directory");
                    let plan_lock_path = current_dir.join("plan.lock");
                    let dry_run_plan = *matches.get_one::<bool>("dry-run").unwrap_or(&false);

                    if !plan_lock_path.exists() {
                        return Err(format!("Error: plan.lock not found at {:?}", plan_lock_path).into());
                    }

                    let plan_content = fs::read_to_string(&plan_lock_path)
                        .map_err(|e| format!("Failed to read plan.lock file {:?}: {}", plan_lock_path, e))?;
                    let plan: Plan = toml::from_str(&plan_content)
                        .map_err(|e| format!("Failed to parse plan.lock file {:?}: {}", plan_lock_path, e))?;

                    let step_name = run_matches.get_one::<String>("step");

                    // Create logs directory if it doesn't exist
                    let logs_dir = current_dir.join("logs");
                    fs::create_dir_all(&logs_dir)
                        .map_err(|e| format!("Failed to create logs directory: {}", e))?;

                    let plan_log_file_path = logs_dir.join("plan_run.log");
                    let mut plan_log_file = File::create(&plan_log_file_path)
                        .map_err(|e| format!("Failed to create plan_run.log file {:?}: {}", plan_log_file_path, e))?;

                    writeln!(plan_log_file, "--- Plan Run Log (Dry Run: {})", dry_run_plan)?;
                    println!("Logging plan run output to {:?}", plan_log_file_path);

                    let execute_task = |task: &Task, log_file: &mut File| -> Result<(), Box<dyn std::error::Error>> {
                        writeln!(log_file, "Processing task: {}", task.name)?;
                        println!("Processing task: {}", task.name);

                        let task_path = if let Some(p) = &task.path {
                            current_dir.join(p)
                        } else {
                            current_dir.clone()
                        };

                        if let Some(cmd_str) = &task.command {
                            if let Some(cargo_command) = get_cargo_command(cmd_str) {
                                if dry_run_plan {
                                    writeln!(log_file, "--- DRY RUN MODE ---")?;
                                    println!("--- DRY RUN MODE ---");
                                    cargo_command.dry_run(&task_path, log_file)?;
                                } else {
                                    match cargo_command.needs_execution(&task_path) {
                                        Ok(true) => {
                                            cargo_command.execute(&task_path, log_file)?;
                                        },
                                        Ok(false) => {
                                            writeln!(log_file, "Skipping command '{}' as it does not need execution.", cmd_str)?;
                                            println!("Skipping command '{}' as it does not need execution.", cmd_str);
                                        },
                                        Err(e) => {
                                            writeln!(log_file, "Error checking needs_execution for '{}': {}", cmd_str, e)?;
                                            return Err(e.into());
                                        }
                                    }
                                }
                            } else {
                                writeln!(log_file, "Unknown command in task '{}': {}", task.name, cmd_str)?;
                                return Err(format!("Unknown command in task '{}': {}", task.name, cmd_str).into());
                            }
                        } else {
                            writeln!(log_file, "No command specified for task: {}", task.name)?;
                            println!("No command specified for task: {}", task.name);
                        }
                        Ok(())
                    };

                    match step_name {
                        Some(name) => {
                            if let Some(task) = plan.tasks.get(name) {
                                execute_task(task, &mut plan_log_file)?;
                            } else {
                                return Err(format!("Error: Step '{}' not found in plan.lock", name).into());
                            }
                        },
                        None => {
                            writeln!(plan_log_file, "Executing all steps in plan.lock with dependency resolution.")?;
                            println!("Executing all steps in plan.lock with dependency resolution.");

                            let mut graph: HashMap<&String, Vec<&String>> = HashMap::new();
                            let mut in_degree: HashMap<&String, usize> = HashMap::new();

                            for (name, task) in &plan.tasks {
                                in_degree.entry(name).or_insert(0); // Initialize all tasks with in-degree 0
                                for dep_name in &task.depends_on {
                                    graph.entry(dep_name).or_insert_with(Vec::new).push(name);
                                    *in_degree.entry(name).or_insert(0) += 1;
                                }
                            }

                            let mut queue: Vec<&String> = in_degree.iter()
                                .filter(|&(_, &degree)| degree == 0)
                                .map(|(&name, _)| name)
                                .collect();
                            queue.sort_unstable(); // For deterministic order

                            let mut sorted_tasks = Vec::new();
                            while let Some(task_name) = queue.pop() {
                                sorted_tasks.push(task_name);

                                if let Some(dependents) = graph.get(task_name) {
                                    for &dependent_name in dependents {
                                        let degree = in_degree.get_mut(dependent_name).unwrap();
                                        *degree -= 1;
                                        if *degree == 0 {
                                            queue.push(dependent_name);
                                            queue.sort_unstable(); // Re-sort to maintain deterministic order
                                        }
                                    }
                                }
                            }

                            if sorted_tasks.len() != plan.tasks.len() {
                                return Err("Error: Circular dependency detected or some tasks are unreachable.".into());
                            }

                            for task_name in sorted_tasks {
                                let task = plan.tasks.get(task_name).unwrap();
                                writeln!(plan_log_file, "--- Starting Task: {}", task_name)?;
                                println!("--- Starting Task: {} ---", task_name);
                                execute_task(task, &mut plan_log_file)?;
                                writeln!(plan_log_file, "--- Finished Task: {}", task_name)?;
                                println!("--- Finished Task: {} ---", task_name);
                            }
                        }
                    }
                    writeln!(plan_log_file, "Plan run completed successfully!")?;
                    println!("Plan run completed successfully!");
                },
                _ => unreachable!(),
            }
        },
        Some(("report", sub_matches)) => {
            println!("Report subcommand invoked.");
            let current_dir = env::current_dir().expect("Failed to get current directory");
            let submodule_name_opt = sub_matches.get_one::<String>("submodule");
            let log_file_path_arg_opt = sub_matches.get_one::<String>("log-file");

            let log_file_to_read = if let Some(log_file_path_arg) = log_file_path_arg_opt {
                PathBuf::from(log_file_path_arg)
            } else if let Some(submodule_name) = submodule_name_opt {
                let logs_dir = current_dir.join("logs");
                logs_dir.join(format!("{}.log", submodule_name))
            } else {
                // Default to current submodule's log if no args provided
                let submodule_name = current_dir.file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown_submodule");
                let logs_dir = current_dir.join("logs");
                logs_dir.join(format!("{}.log", submodule_name))
            };

            println!("Attempting to read log file: {:?}", log_file_to_read);
            match fs::read_to_string(&log_file_to_read) {
                Ok(content) => {
                    println!("\n--- Report for {:?} ---", log_file_to_read);

                    let mut overall_status = "Unknown";
                    let mut commands_executed = Vec::new();
                    let mut dry_run_commands = Vec::new();
                    let mut errors_found = Vec::new();
                    let mut is_dry_run_report = false;

                    const MAX_LINE_WIDTH: usize = 75; // Max width for truncated lines

                    for line in content.lines() {
                        if line.contains("--- DRY RUN MODE ---") {
                            is_dry_run_report = true;
                        } else if line.starts_with("[COMMAND_START]") {
                            commands_executed.push(line.to_string());
                        } else if line.starts_with("[COMMAND_STATUS]") {
                            if line.contains("failed") {
                                overall_status = "Failed";
                            } else if line.contains("succeeded") && overall_status != "Failed" {
                                overall_status = "Succeeded";
                            }
                        } else if line.starts_with("[DRY_RUN_COMMAND]") {
                            dry_run_commands.push(line.to_string());
                        } else if line.starts_with("[ERROR]") {
                            errors_found.push(line.to_string());
                            overall_status = "Failed"; // Any error means failure
                        }
                    }

                    println!("Overall Status: {}", overall_status);
                    println!("Is Dry Run: {}", is_dry_run_report);

                    if !commands_executed.is_empty() {
                        println!("\nCommands Executed:");
                        for cmd in commands_executed {
                            let display_cmd = if cmd.len() > MAX_LINE_WIDTH {
                                format!("{}...", &cmd[..MAX_LINE_WIDTH - 3])
                            } else {
                                cmd
                            };
                            println!("  {}", display_cmd);
                        }
                    }

                    if !dry_run_commands.is_empty() {
                        println!("\nDry Run Commands:");
                        for cmd in dry_run_commands {
                            let display_cmd = if cmd.len() > MAX_LINE_WIDTH {
                                format!("{}...", &cmd[..MAX_LINE_WIDTH - 3])
                            } else {
                                cmd
                            };
                            println!("  {}", display_cmd);
                        }
                    }

                    if !errors_found.is_empty() {
                        println!("\nErrors Found:");
                        for err in errors_found {
                            let display_err = if err.len() > MAX_LINE_WIDTH {
                                format!("{}...", &err[..MAX_LINE_WIDTH - 3])
                            } else {
                                err
                            };
                            println!("  {}", display_err);
                        }
                    }

                    println!("--- End of Report ---");
                },
                Err(e) => {
                    eprintln!("Error reading log file {:?}: {}", log_file_to_read, e);
                    return Err(e.into());
                }
            }
        },
        _ => { // This block now handles the case where no subcommand is provided
            let current_dir = env::current_dir().expect("Failed to get current directory");
            println!("Running in directory: {:?}", current_dir);

            // Extract submodule name from current_dir for log file naming
            let submodule_name = current_dir.file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown_submodule");

            // Create logs directory if it doesn't exist
            let logs_dir = Path::new("logs");
            fs::create_dir_all(logs_dir)
                .map_err(|e| format!("Failed to create logs directory: {}", e))
                .unwrap(); // Handle error appropriately

            let log_file_path = logs_dir.join(format!("{}.log", submodule_name));
            let mut log_file = File::create(&log_file_path)
                .map_err(|e| format!("Failed to create log file at {:?}: {}", log_file_path, e))
                .unwrap(); // Handle error appropriately

            writeln!(log_file, "--- Log for submodule: {}", submodule_name)
                .map_err(|e| format!("Failed to write to log file: {}", e))
                .unwrap(); // Handle error appropriately
            println!("Logging output for {} to {:?}", submodule_name, log_file_path);


            let commands: Vec<Box<dyn CargoCommand>> = vec![
                Box::new(CargoUpdateCommand),
                Box::new(CargoVendorCommand),
                Box::new(Cargo2NixCommand),
            ];

            let mut cargo_config_was_renamed = false; // Flag to track if config was renamed

            for (i, command) in commands.iter().enumerate() {
                // Temporarily rename .cargo/config.toml before cargo update and cargo vendor
                if i == 0 { // Before CargoUpdateCommand
                    match rename_cargo_config(&current_dir, &mut log_file) {
                        Ok(renamed) => cargo_config_was_renamed = renamed,
                        Err(e) => {
                            eprintln!("{}", e);
                            writeln!(log_file, "Error: {}", e).unwrap();
                            // Ensure config is restored even on error
                            match restore_cargo_config(&current_dir, &mut log_file, cargo_config_was_renamed) {
                                Ok(_) => {{}},
                                Err(e_restore) => {
                                    eprintln!("Error restoring config: {}", e_restore);
                                    writeln!(log_file, "Error restoring config: {}", e_restore).unwrap();
                                }
                            }
                            std::process::exit(1);
                        }
                    }
                }

                if dry_run {
                    writeln!(log_file, "--- DRY RUN MODE ---")
                        .map_err(|e| format!("Failed to write to log file: {}", e))
                        .unwrap();
                    println!("--- DRY RUN MODE ---");
                    command.dry_run(&current_dir, &mut log_file)?;
                } else {
                    match command.needs_execution(&current_dir) {
                        Ok(true) => {
                            match command.execute(&current_dir, &mut log_file) {
                                Ok(_) => {
                                    // Restore .cargo/config.toml after cargo vendor
                                    if i == 1 { // After CargoVendorCommand
                                        match restore_cargo_config(&current_dir, &mut log_file, cargo_config_was_renamed) {
                                            Ok(_) => {{}},
                                            Err(e) => {
                                                eprintln!("{}", e);
                                                writeln!(log_file, "Error: {}", e).unwrap();
                                                std::process::exit(1);
                                            }
                                        }
                                    }
                                },
                                Err(e) => {
                                    eprintln!("{}", e);
                                    writeln!(log_file, "Error: {}", e).unwrap();
                                    // Ensure config is restored even on error
                                    match restore_cargo_config(&current_dir, &mut log_file, cargo_config_was_renamed) {
                                        Ok(_) => {{}},
                                        Err(e_restore) => {
                                            eprintln!("Error restoring config: {}", e_restore);
                                            writeln!(log_file, "Error restoring config: {}", e_restore).unwrap();
                                        }
                                    }
                                    std::process::exit(1);
                                }
                            }
                        },
                        Ok(false) => {
                            writeln!(log_file, "Skipping command as it does not need execution.")
                                .map_err(|e| format!("Failed to write to log file: {}", e))
                                .unwrap();
                            println!("Skipping command as it does not need execution.");
                            // Restore .cargo/config.toml if skipped cargo vendor
                            if i == 1 { // After CargoVendorCommand
                                match restore_cargo_config(&current_dir, &mut log_file, cargo_config_was_renamed) {
                                    Ok(_) => {{}},
                                    Err(e) => {
                                        eprintln!("{}", e);
                                        writeln!(log_file, "Error: {}", e).unwrap();
                                        std::process::exit(1);
                                    }
                                }
                            }
                        },
                        Err(e) => {
                            eprintln!("{}", e);
                            writeln!(log_file, "Error: {}", e).unwrap();
                            // Ensure config is restored even on error
                            match restore_cargo_config(&current_dir, &mut log_file, cargo_config_was_renamed) {
                                Ok(_) => {{}},
                                Err(e_restore) => {
                                    eprintln!("Error restoring config: {}", e_restore);
                                    writeln!(log_file, "Error restoring config: {}", e_restore).unwrap();
                                }
                            }
                            std::process::exit(1);
                        }
                    }
                }
            }

            // Final restore in case of no errors and all commands skipped
            match restore_cargo_config(&current_dir, &mut log_file, cargo_config_was_renamed) {
                Ok(_) => {{}},
                Err(e) => {
                    eprintln!("{}", e);
                    writeln!(log_file, "Error: {}", e).unwrap();
                    std::process::exit(1);
                }
            }

            writeln!(log_file, "Submodule tasks completed successfully!")
                .map_err(|e| format!("Failed to write to log file: {}", e))
                .unwrap();
            println!("Submodule tasks completed successfully!");
        }
    }
    Ok(())
}
