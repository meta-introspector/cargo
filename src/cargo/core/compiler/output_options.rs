// src/cargo/core/compiler/output_options.rs
use crate::core::compiler::build_runner::BuildRunner;
use crate::core::compiler::unit::Unit;

pub struct OutputOptions {
    pub errors_seen: usize,
    pub warnings_seen: usize,
    pub show_diagnostics: bool,
    // Add other fields as needed based on usage
}

impl OutputOptions {
    pub fn new(_build_runner: &BuildRunner<'_, '_>, _unit: &Unit) -> Self {
        Self {
            errors_seen: 0,
            warnings_seen: 0,
            show_diagnostics: true, // Default to true based on `!options.show_diagnostics`
        }
    }
}