#!/bin/bash
set -e

export CC="zig cc"

# Build nats.c library
cd libs/nats.c

# Create build directory
mkdir -p build
cd build

# Configure CMake without NATS Streaming support
cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_TESTING=OFF \
    -DNATS_BUILD_STREAMING=OFF \
    -DNATS_BUILD_WITH_TLS=OFF \
    -DCMAKE_INSTALL_PREFIX=../../nats-install

# Build
cmake --build . --config Release

# Install to local directory
cmake --install .

echo "nats.c library built successfully"
echo "Library location: libs/nats-install/lib/libnats_static.a"
echo "Headers location: libs/nats-install/include/"
