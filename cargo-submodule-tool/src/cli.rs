use clap::{Arg, ArgAction, Command as ClapCommand};

pub fn cli() -> ClapCommand {
    ClapCommand::new("cargo-git-manage")
        .about("A cargo subcommand for managing Git repositories and Nix integration.")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .arg(
            Arg::new("dry-run")
                .long("dry-run")
                .action(ArgAction::SetTrue)
                .help("Perform a dry run without making actual changes."),
        )
        .subcommand(
            ClapCommand::new("update")
                .about("Updates Cargo.lock and Cargo.nix files.")
                .arg(
                    Arg::new("force")
                        .long("force")
                        .short('f')
                        .action(ArgAction::SetTrue)
                        .help("Force update even if not needed."),
                ),
        )
        .subcommand(
            ClapCommand::new("vendor")
                .about("Vendors Cargo dependencies.")
                .arg(
                    Arg::new("force")
                        .long("force")
                        .short('f')
                        .action(ArgAction::SetTrue)
                        .help("Force vendoring even if not needed."),
                ),
        )
        .subcommand(
            ClapCommand::new("cargo2nix")
                .about("Generates Cargo.nix from Cargo.lock.")
                .arg(
                    Arg::new("force")
                        .long("force")
                        .short('f')
                        .action(ArgAction::SetTrue)
                        .help("Force cargo2nix even if not needed."),
                ),
        )
        .subcommand(
            ClapCommand::new("report")
                .about("Generates a report from log files.")
                .arg(
                    Arg::new("log-file")
                        .long("log-file")
                        .short('l')
                        .value_name("FILE")
                        .help("Specify the log file to generate the report from.")
                        .default_value("run.log"),
                ),
        )
        .subcommand(
            ClapCommand::new("plan")
                .about("Manages and executes task plans.")
                .subcommand_required(true)
                .arg_required_else_help(true)
                .subcommand(
                    ClapCommand::new("generate")
                        .about("Generates a plan.lock file from task TOMLs.")
                        .arg(
                            Arg::new("tasks-dir")
                                .long("tasks-dir")
                                .short('t')
                                .value_name("DIR")
                                .help("Directory containing task TOML files.")
                                .default_value("tasks"),
                        ),
                )
                .subcommand(
                    ClapCommand::new("run")
                        .about("Executes tasks from a plan.lock file.")
                        .arg(
                            Arg::new("plan-file")
                                .long("plan-file")
                                .short('p')
                                .value_name("FILE")
                                .help("Path to the plan.lock file.")
                                .default_value("plan.lock"),
                        )
                        .arg(
                            Arg::new("step")
                                .long("step")
                                .short('s')
                                .value_name("STEP_ID")
                                .help("Execute a specific step by ID."),
                        ),
                ),
        )
        .subcommand(
            ClapCommand::new("submodule")
                .about("Manages Git submodules.")
                .subcommand_required(true)
                .arg_required_else_help(true)
                .subcommand(
                    ClapCommand::new("add")
                        .about("Adds a Git submodule.")
                        .arg(
                            Arg::new("url")
                                .long("url")
                                .short('u')
                                .value_name("URL")
                                .help("URL of the submodule repository.")
                                .required(true),
                        )
                        .arg(
                            Arg::new("path")
                                .long("path")
                                .short('p')
                                .value_name("PATH")
                                .help("Path where the submodule will be added.")
                                .required(true),
                        )
                        .arg(
                            Arg::new("branch")
                                .long("branch")
                                .short('b')
                                .value_name("BRANCH")
                                .help("Branch to checkout in the submodule."),
                        ),
                )
                .subcommand(
                    ClapCommand::new("remove")
                        .about("Remove a Git submodule")
                        .arg(Arg::new("path")
                            .help("Path of the submodule to remove")
                            .required(true)),
                )
                .subcommand(
                    ClapCommand::new("commit-and-push")
                        .about("Commit and push changes in all submodules")
                        .arg(Arg::new("message")
                            .long("message")
                            .short('m')
                            .value_name("MESSAGE")
                            .help("Commit message for submodule changes")
                            .default_value("chore: Update submodule")),
                )
                .subcommand(
                    ClapCommand::new("patch")
                        .about("Generate [patch.crates-io] entries for submodules")
                        .arg(Arg::new("output")
                            .long("output")
                            .short('o')
                            .value_name("FILE")
                            .help("Specify the output .cargo/config.toml file")
                            .default_value("config.toml")) // Changed default to config.toml
                        .arg(Arg::new("dry-run")
                            .long("dry-run")
                            .action(ArgAction::SetTrue)
                            .help("Print generated patches to stdout without writing to a file"))
                        .arg(Arg::new("overwrite")
                            .long("overwrite")
                            .action(ArgAction::SetTrue)
                            .help("Overwrite existing [patch.crates-io] sections for submodules"))
                        .arg(Arg::new("git-url-template")
                            .long("git-url-template")
                            .value_name("TEMPLATE")
                            .help("Template string to construct the Git URL for submodules"))
                        .arg(Arg::new("branch-template")
                            .long("branch-template")
                            .value_name("TEMPLATE")
                            .help("Template string to construct the branch name for submodules"))
                        .arg(Arg::new("recursive")
                            .long("recursive")
                            .action(ArgAction::SetTrue)
                            .help("Recursively process nested submodules")),
                )
        )
}