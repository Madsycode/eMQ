#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
EMPY_CONFIG="${1:-Debug}"
case "${EMPY_CONFIG}" in
    Debug|Release) ;;
    *) echo "Usage: EmpyLinux.sh Debug|Release" >&2; exit 1 ;;
esac
EMPY_GENERATOR="${CMAKE_GENERATOR:-Unix Makefiles}"
# Each configuration owns Targets/<Config>: Conan files, build tree and bin/.
EMPY_BUILD="Targets/${EMPY_CONFIG}"
EMPY_CONAN="Targets/Conan"

# Install dependencies from the configured Conan profile.
conan install ConanLists.txt --output-folder="${EMPY_CONAN}" \
    --build=missing -s build_type="${EMPY_CONFIG}"

# Configure, build and verify; preserve user presets.
cmake -S . -B "${EMPY_BUILD}" -G "${EMPY_GENERATOR}" \
    -DCMAKE_BUILD_TYPE="${EMPY_CONFIG}" \
    -DCMAKE_TOOLCHAIN_FILE="${PWD}/${EMPY_CONAN}/conan_toolchain.cmake"
cmake --build "${EMPY_BUILD}" --config "${EMPY_CONFIG}" --parallel
ctest --test-dir "${EMPY_BUILD}" -C "${EMPY_CONFIG}" --output-on-failure
