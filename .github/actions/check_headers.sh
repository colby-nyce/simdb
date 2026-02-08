#!/usr/bin/env bash
set -euo pipefail

# Parameters
COMPILER=${1:-gcc}        # gcc or clang
BUILD_TYPE=${2:-Debug}    # Debug or Release
INCLUDE_DIR=${3:-include} # Path to your headers

echo "Running self-contained-header check: $COMPILER / $BUILD_TYPE"

# Define extra flags
EXTRA_FLAGS=""
if [ "$BUILD_TYPE" = "Release" ]; then
  EXTRA_FLAGS="-DNDEBUG"
fi

# Get all headers tracked by git
HEADER_LIST=$(git ls-files '*.hpp' '*.h')

# Compile each header individually
for hdr in $HEADER_LIST; do
    echo "Checking header: $hdr"
    $COMPILER -std=c++17 -Wall -Werror -I"$INCLUDE_DIR" $EXTRA_FLAGS -c "$hdr" -o /dev/null
done

echo "All headers passed self-contained check!"
