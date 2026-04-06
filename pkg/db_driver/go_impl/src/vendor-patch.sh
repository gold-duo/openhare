#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Running go mod vendor..."
cd "$SCRIPT_DIR"
go mod vendor

echo "Applying vendor patches..."
for patch in "$SCRIPT_DIR"/patches/*.patch; do
    if [ -f "$patch" ]; then
        echo "Applying: $(basename "$patch")"
        patch -d "$SCRIPT_DIR" -p1 < "$patch"
    fi
done

echo "Done."
