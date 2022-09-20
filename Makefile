export AUTOINSTALL ?= false

export RUST_VERSION ?= $(shell cat rust-toolchain)

EMPTY:=

.PHONY: fmt
fmt:
	@echo "Formatting..."
	@cargo fmt
	@echo "Done formatting."

.PHONY: clean
clean:
	@echo "Cleaning..."
	@cargo clean
	@echo "Done cleaning."

.PHONY: check
check:
	@echo "Checking..."
	@cargo check --workspace --all-targets
	@echo "Done checking."

.PHONY: clippy
clippy:
	@echo "Running clippy..."
	@cargo clippy --workspace --all-targets
	@echo "Done running clippy."

.PHONY: test
test:
	@echo "Testing..."
	@cargo test --workspace --lib
	@echo "Done testing."

.PHONY: build
build:
	@echo "Building..."
	@cargo build --no-default-features --features default,vector/default
	@echo "Done building."

.PHONY: build-release
build-release:
	@echo "Building release..."
	@cargo build --release --no-default-features --features default,vector/default
	@echo "Done building release."

.PHONY: build-x86_64-unknown-linux-gnu
build-x86_64-unknown-linux-gnu: target/x86_64-unknown-linux-gnu/release/vector
	@echo "Output to ${<}"

.PHONY: build-aarch64-unknown-linux-gnu
build-aarch64-unknown-linux-gnu: target/aarch64-unknown-linux-gnu/release/vector
	@echo "Output to ${<}"

.PHONY: build-x86_64-unknown-linux-musl
build-x86_64-unknown-linux-musl: target/x86_64-unknown-linux-musl/release/vector
	@echo "Output to ${<}"

.PHONY: build-aarch64-unknown-linux-musl
build-aarch64-unknown-linux-musl: target/aarch64-unknown-linux-musl/release/vector
	@echo "Output to ${<}"

.PHONY: build-armv7-unknown-linux-gnueabihf
build-armv7-unknown-linux-gnueabihf: target/armv7-unknown-linux-gnueabihf/release/vector
	@echo "Output to ${<}"

.PHONY: build-armv7-unknown-linux-musleabihf
build-armv7-unknown-linux-musleabihf: target/armv7-unknown-linux-musleabihf/release/vector
	@echo "Output to ${<}"

.PHONY: CARGO_HANDLES_FRESHNESS
CARGO_HANDLES_FRESHNESS:
	${EMPTY}

.PHONY: cross-image-%
cross-image-%: export TRIPLE =$($(strip @):cross-image-%=%)
cross-image-%:
	docker build \
		--tag vector-cross-env:${TRIPLE} \
		--file scripts/cross/${TRIPLE}.dockerfile \
		scripts/cross

.PHONY: target/%/vector
target/%/vector: export PAIR =$(subst /, ,$(@:target/%/vector=%))
target/%/vector: export TRIPLE ?=$(word 1,${PAIR})
target/%/vector: export PROFILE ?=$(word 2,${PAIR})
target/%/vector: export CFLAGS += -g0 -O3
target/%/vector: cargo-install-cross CARGO_HANDLES_FRESHNESS
	$(MAKE) -k cross-image-${TRIPLE}
	cross build \
		$(if $(findstring release,$(PROFILE)),--release,) \
		--target ${TRIPLE} \
		--no-default-features \
		--features default,vector/target-${TRIPLE}

.PHONY: cargo-install-%
cargo-install-%: override TOOL = $(@:cargo-install-%=%)
cargo-install-%:
	$(if $(findstring true,$(AUTOINSTALL)),cargo install ${TOOL} --quiet; cargo clean,)

.PHONY: build-docker
build-docker: target/x86_64-unknown-linux-musl/release/vector
	@echo "Building docker image..."
	@scripts/build-docker.sh
	@echo "Done building docker image."

.PHONY: test-integration
test-integration:
	@echo "Running integration tests..."
	RUST_VERSION=${RUST_VERSION} CASE=topsql_vm docker-compose -f scripts/integration/docker-compose.yml build
	RUST_VERSION=${RUST_VERSION} CASE=topsql_vm docker-compose -f scripts/integration/docker-compose.yml run --rm runner
	RUST_VERSION=${RUST_VERSION} CASE=topsql_vm docker-compose -f scripts/integration/docker-compose.yml rm --force --stop -v
	@echo "Done running integration tests."
