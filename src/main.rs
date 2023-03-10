#![deny(warnings)]
#![allow(unused_imports)]

use vector::app::Application;

// Extensions
#[cfg(feature = "aws-s3-upload-file")]
use aws_s3_upload_file::S3UploadFileConfig as _;
#[cfg(feature = "filename")]
use filename::FilenameConfig as _;
#[cfg(feature = "gcp-cloud-storage-upload-file")]
use gcp_cloud_storage_upload_file::GcsUploadFileSinkConfig as _;
#[cfg(feature = "topsql")]
use topsql::TopSQLConfig as _;
#[cfg(feature = "vm-import")]
use vm_import::VMImportConfig as _;

#[cfg(unix)]
fn main() {
    let app = Application::prepare().unwrap_or_else(|code| {
        std::process::exit(code);
    });

    app.run();
}

#[cfg(windows)]
pub fn main() {
    // We need to be able to run vector in User Interactive mode. We first try
    // to run vector as a service. If we fail, we consider that we are in
    // interactive mode and then fallback to console mode.  See
    // https://docs.microsoft.com/en-us/dotnet/api/system.environment.userinteractive?redirectedfrom=MSDN&view=netcore-3.1#System_Environment_UserInteractive
    vector::vector_windows::run().unwrap_or_else(|_| {
        let app = Application::prepare().unwrap_or_else(|code| {
            std::process::exit(code);
        });

        app.run();
    });
}
