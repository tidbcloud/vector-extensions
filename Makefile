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

.PHONY: test-coverage
test-coverage:
	@echo "Running tests with coverage..."
	@cargo install cargo-tarpaulin --quiet 2>/dev/null || true
	@cargo tarpaulin --workspace --lib --out Xml --out Html --output-dir coverage --timeout 120 || true
	@echo "Coverage report generated in coverage/ directory"
	@echo "Done testing with coverage."

.PHONY: test-coverage-summary
test-coverage-summary:
	@echo "Running tests with coverage summary..."
	@cargo install cargo-tarpaulin --quiet 2>/dev/null || true
	@cargo tarpaulin --workspace --lib --out Stdout --out Xml --output-dir coverage --timeout 120 || true
	@echo "Done testing with coverage."

.PHONY: build
build:
	@echo "Building..."
	@cargo build --no-default-features --features default
	@echo "Done building."

# Nextgen is a runtime mode (VECTOR_NEXTGEN=true); same binary as legacy.
.PHONY: build-nextgen
build-nextgen: build
	@echo "Note: nextgen is enabled at runtime via VECTOR_NEXTGEN=true (same binary)."

.PHONY: build-release
build-release:
	@echo "Building release..."
	@cargo build --release --no-default-features --features default
	@echo "Done building release."

.PHONY: build-release-nextgen
build-release-nextgen: build-release
	@echo "Note: nextgen is enabled at runtime via VECTOR_NEXTGEN=true (same binary)."

.PHONY: build-x86_64-unknown-linux-gnu
build-x86_64-unknown-linux-gnu: target/x86_64-unknown-linux-gnu/release/vector
	@echo "Output to ${<}"

.PHONY: build-aarch64-unknown-linux-gnu
build-aarch64-unknown-linux-gnu: target/aarch64-unknown-linux-gnu/release/vector
	@echo "Output to ${<}"

.PHONY: build-armv7-unknown-linux-gnueabihf
build-armv7-unknown-linux-gnueabihf: target/armv7-unknown-linux-gnueabihf/release/vector
	@echo "Output to ${<}"

.PHONY: build-x86_64-unknown-linux-musl
build-x86_64-unknown-linux-musl: target/x86_64-unknown-linux-musl/release/vector
	@echo "Output to ${<}"

.PHONY: build-aarch64-unknown-linux-musl
build-aarch64-unknown-linux-musl: target/aarch64-unknown-linux-musl/release/vector
	@echo "Output to ${<}"

.PHONY: build-armv7-unknown-linux-musleabihf
build-armv7-unknown-linux-musleabihf: target/armv7-unknown-linux-musleabihf/release/vector
	@echo "Output to ${<}"

# Auto-detect Docker platform for cross-compilation
#
# macOS on Apple Silicon (Darwin arm64) requires --platform linux/amd64 because:
# 1. macOS runs Darwin kernel, not Linux - containers need Linux environment
# 2. Rosetta 2 provides efficient x86_64->ARM64 translation for Linux containers
# 3. Building linux/arm64 containers would require QEMU emulation, which is slower
#
# Usage:
#   Default: Auto-detect platform based on host system
#   Override: DOCKER_PLATFORM="--platform linux/amd64" make build-aarch64-unknown-linux-gnu
#   Disable: DOCKER_PLATFORM="" make build-aarch64-unknown-linux-gnu
DOCKER_PLATFORM ?= $(shell if [ "$$(uname)" = "Darwin" ] && [ "$$(uname -m)" = "arm64" ]; then echo "--platform linux/amd64"; else echo ""; fi)

.PHONY: cross-image-%
cross-image-%: export TRIPLE =$($(strip @):cross-image-%=%)
cross-image-%:
	@echo "Building cross image for ${TRIPLE}..."
	@if [ -n "$(DOCKER_PLATFORM)" ]; then \
		echo "Using Docker platform: $(DOCKER_PLATFORM)"; \
	else \
		echo "Using default Docker platform"; \
	fi
	docker build \
		$(DOCKER_PLATFORM) \
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

.PHONY: cargo-install-%
cargo-install-%: override TOOL = $(@:cargo-install-%=%)
cargo-install-%:
	$(if $(findstring true,$(AUTOINSTALL)),cargo install ${TOOL} --quiet; cargo clean,)

.PHONY: release-docker
release-docker: target/x86_64-unknown-linux-gnu/release/vector
release-docker: target/aarch64-unknown-linux-gnu/release/vector
#release-docker: target/armv7-unknown-linux-gnueabihf/release/vector
	@echo "Releasing docker image..."
	@scripts/release-docker.sh
	@echo "Done releasing docker image."

.PHONY: release-docker-alpine
release-docker-alpine: target/x86_64-unknown-linux-musl/release/vector
release-docker-alpine: target/aarch64-unknown-linux-musl/release/vector
	@echo "Releasing Alpine docker image..."
	@DOCKER_BASE=alpine scripts/release-docker.sh
	@echo "Done releasing Alpine docker image."

.PHONY: release-docker-minimal
release-docker-minimal: target/x86_64-unknown-linux-musl/release/vector
release-docker-minimal: target/aarch64-unknown-linux-musl/release/vector
	@echo "Releasing minimal scratch docker image..."
	@DOCKER_BASE=minimal scripts/release-docker.sh
	@echo "Done releasing minimal scratch docker image."

# Same binary as release-docker; image bakes VECTOR_NEXTGEN=true for drop-in -ng tags.
.PHONY: release-docker-nextgen
release-docker-nextgen: target/x86_64-unknown-linux-gnu/release/vector
release-docker-nextgen: target/aarch64-unknown-linux-gnu/release/vector
	@echo "Releasing docker image (nextgen mode via VECTOR_NEXTGEN=true)..."
	@NEXTGEN=true scripts/release-docker.sh
	@echo "Done releasing docker image (nextgen mode)."

.PHONY: release-docker-nextgen-alpine
release-docker-nextgen-alpine: target/x86_64-unknown-linux-musl/release/vector
release-docker-nextgen-alpine: target/aarch64-unknown-linux-musl/release/vector
	@echo "Releasing Alpine docker image (nextgen mode via VECTOR_NEXTGEN=true)..."
	@NEXTGEN=true DOCKER_BASE=alpine scripts/release-docker.sh
	@echo "Done releasing Alpine docker image (nextgen mode)."

.PHONY: test-integration
test-integration:
	@echo "Running integration tests..."
	RUST_VERSION=${RUST_VERSION} CASE=topsql_vm docker-compose -f scripts/integration/docker-compose.yml build
	RUST_VERSION=${RUST_VERSION} CASE=topsql_vm docker-compose -f scripts/integration/docker-compose.yml run --rm runner
	RUST_VERSION=${RUST_VERSION} CASE=topsql_vm docker-compose -f scripts/integration/docker-compose.yml rm --force --stop -v
	@echo "Done running integration tests."
