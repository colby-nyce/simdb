#!/usr/bin/env python3
import subprocess
import os, sys
from pathlib import Path
from multiprocessing import Pool, cpu_count

# ----------------------------
# Configurable parameters
# ----------------------------
COMPILER = "clang++"                   # or "g++"
STD = "c++17"                          # C++ standard
EXTRA_FLAGS = []                       # e.g., ["-DNDEBUG"] for Release
GIT_FILE_EXTENSIONS = ("*.hpp", "*.h")

CONDA_PREFIX = os.environ.get("CONDA_PREFIX")
if CONDA_PREFIX:
    EXTRA_FLAGS.append(f"-I{CONDA_PREFIX}/include")

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
        return [hdr for hdr in headers if hdr and 'argos' not in os.path.abspath(hdr).split(os.path.sep)]
    except subprocess.CalledProcessError as e:
        print("Error running git ls-files:", e.stderr, file=sys.stderr)
        sys.exit(1)

def compile_header(header_path):
    """Compile a single header and return (header_path, success, output)."""
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
        return header_path, success, output
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

    # Use a multiprocessing Pool with streaming results
    with Pool(processes=cpu_count()) as pool:
        for hdr, success, output in pool.imap_unordered(compile_header, headers):
            print(f"Checking header: {hdr}")
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
