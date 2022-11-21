#![deny(warnings)]

use vector::app::Application;
#[allow(unused_imports)]
use vector::config::{SinkDescription, SourceDescription};

// Extensions
#[cfg(feature = "filename")]
inventory::submit! {
    SourceDescription::new::<filename::FilenameConfig>("filename")
}
#[cfg(feature = "aws-s3-upload-file")]
inventory::submit! {
    SinkDescription::new::<aws_s3_upload_file::S3UploadFileConfig>("aws_s3_upload_file")
}
#[cfg(feature = "topsql")]
inventory::submit! {
    SourceDescription::new::<topsql::TopSQLConfig>("topsql")
}
#[cfg(feature = "vm-import")]
inventory::submit! {
    SinkDescription::new::<vm_import::VMImportConfig>("vm_import")
}

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
