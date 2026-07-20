#!/bin/sh
set -o errexit

echo 'Acquire::Retries "5";' > /etc/apt/apt.conf.d/80-retries

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get upgrade -y

# LLVM >= 3.9 is required by onig_sys/bindgen. Use distro packages from the
# cross-rs base image (Ubuntu 24.04) instead of the legacy xenial LLVM repo.
apt-get install -y \
  clang \
  curl \
  libclang-dev \
  unzip

rm -rf /var/lib/apt/lists/*
