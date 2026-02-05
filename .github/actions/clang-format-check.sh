#!/usr/bin/env bash
set -euo pipefail

FILES=$(git ls-files '*.cpp' '*.cc' '*.cxx' '*.h' '*.hpp')

clang-format -i $FILES

if ! git diff --quiet; then
  echo "clang-format violations detected"
  git diff
  exit 1
fi
