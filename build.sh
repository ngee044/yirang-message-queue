#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
BUILD_TYPE="Release"
BUILD_DIR="build"
CLEAN_BUILD=false
BUILD_TESTS=true
RUN_TESTS=false
JOBS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

# vcpkg paths to search
VCPKG_PATHS=(
    "$VCPKG_ROOT"
    "$HOME/vcpkg"
    "/opt/vcpkg"
    "/usr/local/vcpkg"
    "../vcpkg"
    "../../vcpkg"
)

print_usage() {
    echo "==============================================================================="
    echo "  Yi-Rang MQ Build Script"
    echo "==============================================================================="
    echo ""
    echo "DESCRIPTION:"
    echo "  Lightweight message queue daemon for legacy embedded IPC environments."
    echo "  This script configures and builds the project using CMake and vcpkg."
    echo ""
    echo "USAGE:"
    echo "  $0 [options]"
    echo ""
    echo "OPTIONS:"
    echo "  -d, --debug       Build in Debug mode (includes debug symbols, no optimization)"
    echo "  -r, --release     Build in Release mode (optimized, default)"
    echo "  -c, --clean       Clean build (remove build directory before building)"
    echo "  -j, --jobs N      Number of parallel build jobs (default: $JOBS)"
    echo "  -v, --vcpkg PATH  Specify vcpkg root directory path"
    echo "  -t, --no-tests    Disable building tests"
    echo "      --test        Run tests after build"
    echo "  -h, --help        Show this help message and exit"
    echo ""
    echo "EXAMPLES:"
    echo "  $0                    # Release build with auto-detected vcpkg"
    echo "  $0 -d                 # Debug build"
    echo "  $0 -c -d              # Clean debug build"
    echo "  $0 -c -r -j 8         # Clean release build with 8 parallel jobs"
    echo "  $0 -v ~/vcpkg         # Build with custom vcpkg path"
    echo ""
    echo "VCPKG AUTO-DETECTION PATHS:"
    echo "  1. \$VCPKG_ROOT environment variable"
    echo "  2. ~/vcpkg"
    echo "  3. /opt/vcpkg"
    echo "  4. /usr/local/vcpkg"
    echo "  5. ../vcpkg"
    echo "  6. ../../vcpkg"
    echo ""
    echo "DEPENDENCIES (via vcpkg):"
    echo "  - lz4             : Fast compression library"
    echo "  - efsw            : File system watcher"
    echo "  - sqlite3         : SQLite database backend"
    echo "  - nlohmann-json   : JSON parsing library"
    echo ""
    echo "OUTPUT:"
    echo "  - Build directory : ./build"
    echo "  - Executables     : ./build/out"
    echo "  - Libraries       : ./build/lib"
    echo ""
    echo "==============================================================================="
}

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

find_vcpkg() {
    for path in "${VCPKG_PATHS[@]}"; do
        if [[ -n "$path" && -f "$path/scripts/buildsystems/vcpkg.cmake" ]]; then
            echo "$path"
            return 0
        fi
    done
    return 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--debug)
            BUILD_TYPE="Debug"
            shift
            ;;
        -r|--release)
            BUILD_TYPE="Release"
            shift
            ;;
        -c|--clean)
            CLEAN_BUILD=true
            shift
            ;;
        -j|--jobs)
            JOBS="$2"
            shift 2
            ;;
        -t|--no-tests)
            BUILD_TESTS=false
            shift
            ;;
        --test)
            RUN_TESTS=true
            shift
            ;;
        -v|--vcpkg)
            VCPKG_ROOT="$2"
            shift 2
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

# Find vcpkg
if [[ -z "$VCPKG_ROOT" ]]; then
    VCPKG_ROOT=$(find_vcpkg) || true
fi

if [[ -z "$VCPKG_ROOT" || ! -f "$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" ]]; then
    log_error "vcpkg not found. Please specify path with -v option or set VCPKG_ROOT environment variable."
    log_info "Searched paths:"
    for path in "${VCPKG_PATHS[@]}"; do
        [[ -n "$path" ]] && echo "  - $path"
    done
    exit 1
fi

VCPKG_TOOLCHAIN="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
log_info "Using vcpkg: $VCPKG_ROOT"

# Clean build if requested
if [[ "$CLEAN_BUILD" == true && -d "$BUILD_DIR" ]]; then
    log_info "Cleaning build directory..."
    rm -rf "$BUILD_DIR"
fi

# Create build directory
mkdir -p "$BUILD_DIR"

# Configure
log_info "Configuring ($BUILD_TYPE)..."
CMAKE_ARGS=(
    -B "$BUILD_DIR" -S .
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE"
    -DCMAKE_TOOLCHAIN_FILE="$VCPKG_TOOLCHAIN"
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
)

if [[ "$BUILD_TESTS" == false ]]; then
    CMAKE_ARGS+=(-DBUILD_TESTS=OFF)
fi

cmake "${CMAKE_ARGS[@]}"

# Build
log_info "Building with $JOBS jobs..."
cmake --build "$BUILD_DIR" --config "$BUILD_TYPE" -j "$JOBS"

# Done
log_info "Build complete!"
log_info "Output directory: $BUILD_DIR/out"

# Run tests if requested
if [[ "$RUN_TESTS" == true && "$BUILD_TESTS" == true ]]; then
    echo ""
    log_info "Running tests..."
    ctest --test-dir "$BUILD_DIR" --output-on-failure
fi

# List built executables
if [[ -d "$BUILD_DIR/out" ]]; then
    echo ""
    log_info "Built executables:"
    find "$BUILD_DIR/out" -type f -perm +111 2>/dev/null | while read -r exe; do
        echo "  - $exe"
    done
fi
