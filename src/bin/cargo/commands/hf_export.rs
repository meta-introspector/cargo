use crate::command_prelude::*;
use cargo2hf_extractor::{Cargo2HfExtractor, CargoExtractionPhase};
use std::path::Path;

pub fn cli() -> Command {
    subcommand("hf-export")
        .about("Extracts Cargo project data for Hugging Face datasets")
        .arg(Arg::new("path")
            .value_name("PATH")
            .help("Path to the Cargo project root (containing Cargo.toml)")
            .required(true))
        .arg(Arg::new("output")
            .value_name("OUTPUT_DIR")
            .help("Directory where Parquet files will be written")
            .default_value("hf-dataset-output"))
        .arg(Arg::new("include-deps")
            .long("include-deps")
            .help("Recursively analyze dependencies"))
        .arg(Arg::new("phases")
            .long("phases")
            .value_name("PHASES")
            .help("Comma-separated list of phases to extract (e.g., metadata,dependencies)")
            .default_value("metadata,dependencies,source_code,build,ecosystem,version_history"))
}

pub fn exec(gctx: &mut GlobalContext, args: &ArgMatches) -> CliResult {
    gctx.shell().status("Extracting", "Hugging Face dataset")?;

    let project_path_str = args.get_one::<String>("path").unwrap();
    let project_path = Path::new(project_path_str);

    let output_dir_str = args.get_one::<String>("output").unwrap();
    let output_dir = Path::new(output_dir_str);

    let include_deps = args.get_flag("include-deps");

    let phases_str = args.get_one::<String>("phases").unwrap();
    let phases: Vec<CargoExtractionPhase> = phases_str.split(',')
        .map(|s| s.trim())
        .filter_map(|s| match s {
            "metadata" => Some(CargoExtractionPhase::ProjectMetadata),
            "dependencies" => Some(CargoExtractionPhase::DependencyAnalysis),
            "source_code" => Some(CargoExtractionPhase::SourceCodeAnalysis),
            "build" => Some(CargoExtractionPhase::BuildExtractionPhase),
            "ecosystem" => Some(CargoExtractionPhase::EcosystemAnalysis),
            "version_history" => Some(CargoExtractionPhase::VersionHistory),
            _ => None,
        })
        .collect();

    if phases.is_empty() {
        return Err(CliError::new("No valid phases specified.", 1));
    }

    let mut extractor = Cargo2HfExtractor::new()
        .map_err(|e| CliError::new(format!("Failed to create extractor: {}", e), 1))?;

    // Run the async extraction logic
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        extractor.extract_project_to_parquet(
            project_path,
            &phases,
            output_dir,
            include_deps,
        ).map_err(|e| CliError::new(format!("Extraction failed: {}", e), 1))
    })?;

    gctx.shell().status("Finished", "Hugging Face dataset extraction")?;

    Ok(())
}
