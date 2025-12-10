export AUTOINSTALL ?= false

export RUST_VERSION ?= $(shell sed -n 's/^channel *= *"\?\([^"]*\)"\?/\1/p' rust-toolchain.toml)

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
	@cargo build --no-default-features --features default
	@echo "Done building."

.PHONY: build-nextgen
build-nextgen:
	@echo "Building nextgen mode..."
	@cargo build --no-default-features --features default,nextgen
	@echo "Done building nextgen mode."

.PHONY: build-release
build-release:
	@echo "Building release..."
	@cargo build --release --no-default-features --features default
	@echo "Done building release."

.PHONY: build-release-nextgen
build-release-nextgen:
	@echo "Building release nextgen mode..."
	@cargo build --release --no-default-features --features default,nextgen
	@echo "Done building release nextgen mode."

.PHONY: build-x86_64-unknown-linux-gnu
build-x86_64-unknown-linux-gnu: target/x86_64-unknown-linux-gnu/release/vector
	@echo "Output to ${<}"

.PHONY: build-x86_64-unknown-linux-gnu-nextgen
build-x86_64-unknown-linux-gnu-nextgen: target/x86_64-unknown-linux-gnu/release/vector-nextgen
	@echo "Output to ${<}"

.PHONY: build-aarch64-unknown-linux-gnu
build-aarch64-unknown-linux-gnu: target/aarch64-unknown-linux-gnu/release/vector
	@echo "Output to ${<}"

.PHONY: build-aarch64-unknown-linux-gnu-nextgen
build-aarch64-unknown-linux-gnu-nextgen: target/aarch64-unknown-linux-gnu/release/vector-nextgen
	@echo "Output to ${<}"

.PHONY: build-armv7-unknown-linux-gnueabihf
build-armv7-unknown-linux-gnueabihf: target/armv7-unknown-linux-gnueabihf/release/vector
	@echo "Output to ${<}"

.PHONY: build-armv7-unknown-linux-gnueabihf-nextgen
build-armv7-unknown-linux-gnueabihf: target/armv7-unknown-linux-gnueabihf/release/vector-nextgen
	@echo "Output to ${<}"

.PHONY: build-x86_64-unknown-linux-musl
build-x86_64-unknown-linux-musl: target/x86_64-unknown-linux-musl/release/vector
	@echo "Output to ${<}"

.PHONY: build-x86_64-unknown-linux-musl-nextgen
build-x86_64-unknown-linux-musl-nextgen: target/x86_64-unknown-linux-musl/release/vector-nextgen
	@echo "Output to ${<}"

.PHONY: build-aarch64-unknown-linux-musl
build-aarch64-unknown-linux-musl: target/aarch64-unknown-linux-musl/release/vector
	@echo "Output to ${<}"

.PHONY: build-aarch64-unknown-linux-musl-nextgen
build-aarch64-unknown-linux-musl-nextgen: target/aarch64-unknown-linux-musl/release/vector-nextgen
	@echo "Output to ${<}"

.PHONY: build-armv7-unknown-linux-musleabihf
build-armv7-unknown-linux-musleabihf: target/armv7-unknown-linux-musleabihf/release/vector
	@echo "Output to ${<}"

.PHONY: build-armv7-unknown-linux-musleabihf-nextgen
build-armv7-unknown-linux-musleabihf: target/armv7-unknown-linux-musleabihf/release/vector-nextgen
	@echo "Output to ${<}"

.PHONY: cross-image-%
cross-image-%: export TRIPLE =$($(strip @):cross-image-%=%)
cross-image-%:
	docker build \
		--tag vector-cross-env:${TRIPLE} \
		--file scripts/cross/${TRIPLE}.dockerfile \
		scripts/cross

target/%/vector: export PAIR =$(subst /, ,$(@:target/%/vector=%))
target/%/vector: export TRIPLE ?=$(word 1,${PAIR})
target/%/vector: export PROFILE ?=$(word 2,${PAIR})
target/%/vector: export FEATURES =default
target/%/vector: export CFLAGS += -g0 -O3
target/%/vector: cargo-install-cross
	$(MAKE) -k cross-image-${TRIPLE}
	cross build \
		$(if $(findstring release,$(PROFILE)),--release,) \
		--target ${TRIPLE} \
		--no-default-features \
		--features ${FEATURES}

target/%/vector-nextgen: export PAIR =$(subst /, ,$(@:target/%/vector-nextgen=%))
target/%/vector-nextgen: export TRIPLE ?=$(word 1,${PAIR})
target/%/vector-nextgen: export PROFILE ?=$(word 2,${PAIR})
target/%/vector-nextgen: export FEATURES =default,nextgen
target/%/vector-nextgen: export CFLAGS += -g0 -O3
target/%/vector-nextgen: cargo-install-cross
	$(MAKE) -k cross-image-${TRIPLE}
	cross build \
		$(if $(findstring release,$(PROFILE)),--release,) \
		--target ${TRIPLE} \
		--no-default-features \
		--features ${FEATURES}

.PHONY: cargo-install-%
cargo-install-%: override TOOL = $(@:cargo-install-%=%)
cargo-install-%:
	$(if $(findstring true,$(AUTOINSTALL)),cargo install ${TOOL} --quiet; cargo clean,)

.PHONY: release-docker
release-docker: target/x86_64-unknown-linux-gnu/release/vector
release-docker: target/aarch64-unknown-linux-gnu/release/vector
# release-docker: target/armv7-unknown-linux-gnueabihf/release/vector
	@echo "Releasing docker image..."
	@scripts/release-docker.sh
	@echo "Done releasing docker image."

.PHONY: release-docker-nextgen
release-docker-nextgen: target/x86_64-unknown-linux-gnu/release/vector-nextgen
release-docker-nextgen: target/aarch64-unknown-linux-gnu/release/vector-nextgen
# release-docker-nextgen: target/armv7-unknown-linux-gnueabihf/release/vector-nextgen
	@echo "Releasing docker image (nextgen mode)..."
	@NEXTGEN=true scripts/release-docker.sh
	@echo "Done releasing docker image (nextgen mode)."

.PHONY: test-integration
test-integration:
	@echo "Running integration tests..."
	RUST_VERSION=${RUST_VERSION} CASE=topsql_vm docker-compose -f scripts/integration/docker-compose.yml build
	RUST_VERSION=${RUST_VERSION} CASE=topsql_vm docker-compose -f scripts/integration/docker-compose.yml run --rm runner
	RUST_VERSION=${RUST_VERSION} CASE=topsql_vm docker-compose -f scripts/integration/docker-compose.yml rm --force --stop -v
	@echo "Done running integration tests."
