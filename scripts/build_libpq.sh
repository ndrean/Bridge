#!/bin/bash
set -e

# Script to build PostgreSQL libpq as a static library
# PostgreSQL 18.1 - matches our Docker PostgreSQL version
VERSION="18.1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LIBS_DIR="$PROJECT_ROOT/libs"
LIBPQ_SOURCE_DIR="$LIBS_DIR/postgresql-${VERSION}"
LIBPQ_BUILD_DIR="$LIBS_DIR/libpq-build"
LIBPQ_INSTALL_DIR="$LIBS_DIR/libpq-install"

echo "=== Building libpq static library ==="
echo "Source: $LIBPQ_SOURCE_DIR"
echo "Build:  $LIBPQ_BUILD_DIR"
echo "Install: $LIBPQ_INSTALL_DIR"
echo ""

# Create directories
mkdir -p "$LIBS_DIR"
cd "$LIBS_DIR"

# Download PostgreSQL source if not already present
if [ ! -d "$LIBPQ_SOURCE_DIR" ]; then
    echo "Downloading PostgreSQL 18.1 source..."
    curl -LO https://ftp.postgresql.org/pub/source/v18.1/postgresql-${VERSION}.tar.gz
    tar -xzf postgresql-${VERSION}.tar.gz
    rm postgresql-${VERSION}.tar.gz
    echo "✓ PostgreSQL source downloaded and extracted"
else
    echo "✓ PostgreSQL source already present"
fi

# Clean previous build
rm -rf "$LIBPQ_BUILD_DIR"
rm -rf "$LIBPQ_INSTALL_DIR"
mkdir -p "$LIBPQ_BUILD_DIR"

cd "$LIBPQ_SOURCE_DIR"

echo ""
echo "Configuring libpq..."
# Configure PostgreSQL with minimal options - we only need libpq
./configure \
    --prefix="$LIBPQ_INSTALL_DIR" \
    --without-readline \
    --without-zlib \
    --without-openssl \
    --without-icu \
    --disable-shared \
    --enable-static \
    CFLAGS="-fPIC"

echo ""
echo "Building libpq..."
# Only build libpq, not the entire PostgreSQL
cd src/interfaces/libpq
make clean
make -j$(nproc || sysctl -n hw.ncpu || echo 4)

echo ""
echo "Building libpgcommon and libpgport (required by libpq)..."
cd "$LIBPQ_SOURCE_DIR"
# Build both static and shared versions of common/port to get all symbols
make -C src/common -j$(nproc || sysctl -n hw.ncpu || echo 4)
make -C src/port -j$(nproc || sysctl -n hw.ncpu || echo 4)
# Build the shared library versions which have public symbols
make -C src/common libpgcommon_shlib.a -j$(nproc || sysctl -n hw.ncpu || echo 4)
make -C src/port libpgport_shlib.a -j$(nproc || sysctl -n hw.ncpu || echo 4)

echo ""
echo "Installing libpq..."
mkdir -p "$LIBPQ_INSTALL_DIR/lib"
mkdir -p "$LIBPQ_INSTALL_DIR/include"

# Copy static libraries
cp src/interfaces/libpq/libpq.a "$LIBPQ_INSTALL_DIR/lib/"
# Use the shlib versions which have public symbols (not _private suffixed)
cp src/common/libpgcommon_shlib.a "$LIBPQ_INSTALL_DIR/lib/libpgcommon.a"
cp src/port/libpgport_shlib.a "$LIBPQ_INSTALL_DIR/lib/libpgport.a"

# Copy headers
cp -r src/include/libpq "$LIBPQ_INSTALL_DIR/include/"
cp src/interfaces/libpq/libpq-fe.h "$LIBPQ_INSTALL_DIR/include/"
cp src/include/postgres_ext.h "$LIBPQ_INSTALL_DIR/include/"
cp src/include/pg_config.h "$LIBPQ_INSTALL_DIR/include/" || echo "Warning: pg_config.h not found"
cp src/include/pg_config_ext.h "$LIBPQ_INSTALL_DIR/include/" || echo "Warning: pg_config_ext.h not found"

echo ""
echo "=== libpq build complete ==="
echo "Static library: $LIBPQ_INSTALL_DIR/lib/libpq.a"
echo "Headers:        $LIBPQ_INSTALL_DIR/include/"
echo ""
echo "Library size:"
ls -lh "$LIBPQ_INSTALL_DIR/lib/"*.a
