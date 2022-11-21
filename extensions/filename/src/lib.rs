#[macro_use]
extern crate tracing;

use std::io::Error;
use std::path::{Path, PathBuf};
use std::time::Duration;

use file_source::paths_provider::glob::{Glob, MatchOptions};
use file_source::paths_provider::PathsProvider;
use file_source::FileSourceInternalEvents;
use metrics::counter;
use vector::config::{DataType, Output, SourceConfig, SourceContext, SourceDescription};
use vector::internal_events::prelude::{error_stage, error_type};
use vector::internal_events::StreamClosedError;
use vector::sources::Source;
use vector::{emit, impl_generate_config_from_default};
use vector_config::configurable_component;
use vector_core::event::LogEvent;
use vector_core::internal_event::InternalEvent;

/// Configuration for the `filename` source.
#[configurable_component(source)]
#[derive(Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct FilenameConfig {
    /// Array of file patterns to include. [Globbing](https://vector.dev/docs/reference/configuration/sources/file/#globbing) is supported.
    pub include: Vec<PathBuf>,

    /// Array of file patterns to exclude. [Globbing](https://vector.dev/docs/reference/configuration/sources/file/#globbing) is supported.
    ///
    /// Takes precedence over the `include` option. Note that the `exclude` patterns are applied _after_ Vector attempts to glob everything
    /// in `include`. That is, Vector will still try to list all of the files matched by `include` and then filter them by the `exclude`
    /// patterns. This can be impactful if `include` contains directories with contents that vector does not have access to.
    pub exclude: Vec<PathBuf>,

    /// Delay between file discovery calls, in milliseconds.
    ///
    /// This controls the interval at which Vector searches for files. Higher value result in greater chances of some short living files being missed between searches, but lower value increases the performance impact of file discovery.
    #[serde(alias = "glob_minimum_cooldown")]
    pub glob_minimum_cooldown_ms: u64,
}

impl Default for FilenameConfig {
    fn default() -> Self {
        Self {
            include: vec![],
            exclude: vec![],
            glob_minimum_cooldown_ms: 1000,
        }
    }
}

inventory::submit! {
    SourceDescription::new::<FilenameConfig>("filename")
}

impl_generate_config_from_default!(FilenameConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "filename")]
impl SourceConfig for FilenameConfig {
    async fn build(&self, mut cx: SourceContext) -> vector::Result<Source> {
        let glob_minimum_cooldown = Duration::from_millis(self.glob_minimum_cooldown_ms);

        let paths_provider = Glob::new(
            &self.include,
            &self.exclude,
            MatchOptions::default(),
            OnlyGlob,
        )
        .expect("invalid glob patterns");

        Ok(Box::pin(async move {
            loop {
                let events = paths_provider
                    .paths()
                    .into_iter()
                    .filter(|path| path.metadata().map(|m| m.is_file()).unwrap_or_default())
                    .filter_map(|filepath| filepath.to_str().map(|s| s.to_owned()))
                    .map(LogEvent::from)
                    .collect::<Vec<_>>();
                let count = events.len();

                cx.out.send_batch(events).await.map_err(|error| {
                    emit!(StreamClosedError { error, count });
                })?;

                tokio::select! {
                    _ = tokio::time::sleep(glob_minimum_cooldown) => {},
                    _ = &mut cx.shutdown => break,
                }
            }

            Ok(())
        }))
    }

    fn outputs(&self) -> Vec<Output> {
        vec![Output::default(DataType::Log)]
    }

    fn source_type(&self) -> &'static str {
        "filename"
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

#[derive(Debug)]
pub struct PathGlobbingError<'a> {
    pub path: &'a Path,
    pub error: &'a Error,
}

impl<'a> InternalEvent for PathGlobbingError<'a> {
    fn emit(self) {
        error!(
            message = "Failed to glob path.",
            error = %self.error,
            error_code = "globbing",
            error_type = error_type::READER_FAILED,
            stage = error_stage::RECEIVING,
            path = %self.path.display(),
        );
        counter!(
            "component_errors_total", 1,
            "error_code" => "globbing",
            "error_type" => error_type::READER_FAILED,
            "stage" => error_stage::RECEIVING,
            "path" => self.path.to_string_lossy().into_owned(),
        );
        // deprecated
        counter!(
            "glob_errors_total", 1,
            "path" => self.path.to_string_lossy().into_owned(),
        );
    }
}

#[derive(Clone)]
struct OnlyGlob;

impl FileSourceInternalEvents for OnlyGlob {
    fn emit_file_added(&self, _: &Path) {}

    fn emit_file_resumed(&self, _: &Path, _: u64) {}

    fn emit_file_watch_error(&self, _: &Path, _: Error) {}

    fn emit_file_unwatched(&self, _: &Path) {}

    fn emit_file_deleted(&self, _: &Path) {}

    fn emit_file_delete_error(&self, _: &Path, _: Error) {}

    fn emit_file_fingerprint_read_error(&self, _: &Path, _: Error) {}

    fn emit_file_checkpointed(&self, _: usize, _: Duration) {}

    fn emit_file_checksum_failed(&self, _: &Path) {}

    fn emit_file_checkpoint_write_error(&self, _: Error) {}

    fn emit_files_open(&self, _: usize) {}

    fn emit_path_globbing_failed(&self, path: &Path, error: &Error) {
        emit!(PathGlobbingError { path, error });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        vector::test_util::test_generate_config::<FilenameConfig>();
    }
}
