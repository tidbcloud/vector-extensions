# Extensions for Vector

We have some sources and sinks associated with TiDB Cluster, but not sutable to put in official vector repository, e.g. the topsql source, are putted in this repository.

## Development

To add a new component, you can take topsql source as an example.

Steps in general:
1. Initialize a crate as a member of current workspace.
    * create a new crate at `extensions/${YOUR_COMPONENT_NAME}`
    * declare the crate at `workspace.member` in `Cargo.toml`
2. Introduce the component as a dependency.
    * add the component to `dependencies` in `Cargo.toml`
3. Add a feature to control if equipped with the component in `Cargo.toml`.
    * add a feature depends on the dependency introduced in step 2
    * extend the `features.default` to include the feature
4. Declare the component in `src/main.rs`
    * add a line of code `extern crate ${YOUR_COMPONENT_NAME}`
    * add an attribute `#[cfg(feature = "${FEATURE_ADD_IN_STEP_3}")]` above the delcaration

### Clean
```bash
make clean
```

### Check

```bash
# check for all extensions
cargo check
# or make check

# check for topsql only for speed up
cargo check --no-default-features --features topsql

# check for vm-import only for speed up
cargo check --no-default-features --features vm-import
```

### Lint
```bash
# lint for all extensions
cargo clippy
# or make clippy

# lint for topsql only for speed up
cargo clippy --no-default-features --features topsql

# lint for vm-import only for speed up
cargo clippy --no-default-features --features vm-import
```

### Format
```bash
make fmt
```

### Test
```bash
make test
```

## Build

### Build Dev
```bash
# build for all extensions with full features of vector enabled
make build

# build for topsql with the console sink enabled for debug
cargo build --no-default-features --features topsql,vector/sinks-console

# build for all extensions but without any other features enabled
cargo build
```

### Build Release
```bash
make build-release
```

### Cross Build Release
```bash
# Build a release binary for the x86_64-unknown-linux-gnu triple.
make build-x86_64-unknown-linux-gnu

# Build a release binary for the aarch64-unknown-linux-gnu triple.
make build-aarch64-unknown-linux-gnu

# Build a release binary for the x86_64-unknown-linux-musl triple.
make build-x86_64-unknown-linux-musl

# Build a release binary for the aarch64-unknown-linux-musl triple.
make build-aarch64-unknown-linux-musl

# Build a release binary for the armv7-unknown-linux-gnueabihf triple.
make build-armv7-unknown-linux-gnueabihf

# Build a release binary for the armv7-unknown-linux-musleabihf triple.
make build-armv7-unknown-linux-musleabihf
```

### Release Docker Image

```bash
make target/x86_64-unknown-linux-gnu/release/vector
JEMALLOC_SYS_WITH_LG_PAGE=16 make target/aarch64-unknown-linux-gnu/release/vector
make release-docker

# build with given version and repo
REPO=tidbcloud/vector VERSION=0.23.3 make release-docker
```
