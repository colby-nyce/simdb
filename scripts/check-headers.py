#!/usr/bin/env python3
import subprocess
import os, sys
from pathlib import Path

# ----------------------------
# Configurable parameters
# ----------------------------
COMPILER = "clang++"                   # or "g++"
STD = "c++17"                          # C++ standard
EXTRA_FLAGS = []                       # e.g., ["-DNDEBUG"] for Release
GIT_FILE_EXTENSIONS = ("*.hpp", "*.h")

# ----------------------------
# Helper functions
# ----------------------------
def get_git_headers():
    """Return a list of headers tracked by git."""
    try:
        result = subprocess.run(
            ["git", "ls-files", "*.hpp", "*.h"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        headers = result.stdout.strip().split("\n")
        # Filter out empty lines
        return [hdr for hdr in headers if hdr and 'argos' not in os.path.abspath(hdr).split(os.path.sep)]
    except subprocess.CalledProcessError as e:
        print("Error running git ls-files:", e.stderr, file=sys.stderr)
        sys.exit(1)

def compile_header(header_path):
    """Compile a single header and return (success, output)."""
    cmd = [
        COMPILER,
        f"-std={STD}",
        "-Wall",
        "-Werror",
        "-Iinclude/",
        "-Itest/",
        "-c",
        header_path,
        "-o",
        "/dev/null"
    ] + EXTRA_FLAGS

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        success = result.returncode == 0
        output = result.stderr.strip()
        return success, output
    except FileNotFoundError:
        print(f"Compiler '{COMPILER}' not found!", file=sys.stderr)
        sys.exit(1)

# ----------------------------
# Main
# ----------------------------
def main():
    headers = get_git_headers()
    if not headers:
        print("No headers found in git.")
        return

    found_issues = False

    for hdr in headers:
        print(f"Checking header: {hdr}")
        success, output = compile_header(hdr)
        if not success:
            found_issues = True
            print(f'Found issues in header "{hdr}":')
            for line in output.splitlines():
                print(f"    {line}")

    if found_issues:
        print("\nSome headers failed the self-contained check.")
        sys.exit(1)
    else:
        print("\nAll headers passed the self-contained check!")

if __name__ == "__main__":
    main()
