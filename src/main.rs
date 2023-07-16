#![deny(warnings)]

use std::process::ExitCode;

use vector::app::Application;
use vector_config::component::{SinkDescription, SourceDescription};

// Extensions
#[cfg(feature = "filename")]
inventory::submit! {
    SourceDescription::new::<filename::FilenameConfig>("filename", "", "", "")
}
#[cfg(feature = "aws-s3-upload-file")]
inventory::submit! {
    SinkDescription::new::<aws_s3_upload_file::S3UploadFileConfig>("aws_s3_upload_file", "", "", "")
}
#[cfg(feature = "gcp-cloud-storage-upload-file")]
inventory::submit! {
    SinkDescription::new::<gcp_cloud_storage_upload_file::GcsUploadFileSinkConfig>("gcp_cloud_storage_upload_file", "", "", "")
}
#[cfg(feature = "topsql")]
inventory::submit! {
    SourceDescription::new::<topsql::TopSQLConfig>("topsql", "", "", "")
}
#[cfg(feature = "vm-import")]
inventory::submit! {
    SinkDescription::new::<vm_import::VMImportConfig>("vm_import", "", "", "")
}

#[cfg(unix)]
fn main() -> ExitCode {
    let exit_code = Application::run().code().unwrap_or(exitcode::UNAVAILABLE) as u8;
    ExitCode::from(exit_code)
}

#[cfg(windows)]
pub fn main() -> ExitCode {
    // We need to be able to run vector in User Interactive mode. We first try
    // to run vector as a service. If we fail, we consider that we are in
    // interactive mode and then fallback to console mode.  See
    // https://docs.microsoft.com/en-us/dotnet/api/system.environment.userinteractive?redirectedfrom=MSDN&view=netcore-3.1#System_Environment_UserInteractive
    let exit_code = vector::vector_windows::run()
        .unwrap_or_else(|_| Application::run().code().unwrap_or(exitcode::UNAVAILABLE));
    ExitCode::from(exit_code as u8)
}
