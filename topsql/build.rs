fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    println!("cargo:rerun-if-changed=proto/tidb.proto");
    println!("cargo:rerun-if-changed=proto/tikv.proto");
    println!("cargo:rerun-if-changed=proto/resource_tag.proto");

    let mut prost_build = prost_build::Config::new();
    prost_build.btree_map(&["."]);

    tonic_build::configure()
        .compile_with_config(
            prost_build,
            &[
                "proto/tidb.proto",
                "proto/tikv.proto",
                "proto/resource_tag.proto",
            ],
            &["proto/"],
        )
        .unwrap();
}
