#!/usr/bin/env bash
# Refresh the vendored copy of Zamboni's table-config contract (DUCKLKD-94).
#
# Zamboni owns the table-config.json format; this repo owns a pinned copy of
# its published spec, and of its example file. tests/test_table_config_contract.py
# checks target_ducklake/layout.py against that copy: the version it reads,
# and every key, transform and ordering mode it applies or ignores. So the
# tests run with no network and no dependency on Zamboni, and a format change
# arrives as a reviewed diff of a checked-in file.
#
# Re-pin REF below, run this, and the tests either still pass or name what
# changed. The same pattern as target-iceberg's
# bin/refresh-write-completed-contract.sh.
set -euo pipefail

# The Zamboni tag this copy came from. Bump deliberately, never automatically.
REF="v0.7.0"
REPO="paulcaron16k/Zamboni"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
contracts="${root}/tests/contracts"

if ! command -v gh >/dev/null 2>&1; then
    echo "error: gh is required to fetch the contract (${REPO} is a GitHub repo)" >&2
    exit 1
fi

fetch() {
    local remote_path="$1" dest="$2" tmp
    echo "Fetching ${remote_path} from ${REPO}@${REF}..."
    tmp="$(mktemp)"
    gh api "repos/${REPO}/contents/${remote_path}?ref=${REF}" --jq '.content' | base64 -d > "${tmp}"
    # Refuse to write something that is not a table-config document: a 404
    # body or an error page would otherwise land in the repo.
    if ! jq -e 'type == "object"' "${tmp}" >/dev/null 2>&1; then
        rm -f "${tmp}"
        echo "error: ${remote_path} is not a JSON object; not overwriting ${dest}" >&2
        exit 1
    fi
    mv "${tmp}" "${dest}"
}

fetch "src/zamboni/zamboni-table-config_spec.json" "${contracts}/zamboni-table-config_spec.json"
fetch "examples/table-config.json" "${contracts}/zamboni-table-config_example.json"

version="$(jq -r '.properties.version.const // empty' "${contracts}/zamboni-table-config_spec.json")"
if [[ -z "${version}" ]]; then
    echo "error: the spec states no version (properties.version.const)" >&2
    exit 1
fi
echo "Wrote ${contracts} (table-config version ${version}, from ${REF})."
echo
echo "Now run: uv run pytest tests/test_table_config_contract.py"
echo "If it fails, the format moved: read the diff before changing layout.py."
