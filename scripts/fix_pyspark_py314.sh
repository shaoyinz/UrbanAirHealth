#!/usr/bin/env bash
# Patch pyspark 3.5's bundled cloudpickle 2.2.1 for Python 3.14 compat.
#
# Why: pyspark 3.5.x vendors cloudpickle 2.2.1, which recurses to a stack
# overflow when serializing closures on CPython 3.14 (the recursion guard
# in cloudpickle/_reduce_method_descriptor regressed under 3.14's new
# function-introspection internals). Cloudpickle 3.1.2 fixes it.
# Upgrading pyspark itself is not an option — Dataproc Serverless 2.2
# ships Spark 3.5 / Scala 2.13 and we want the local runtime to match.
#
# What this does: install standalone cloudpickle 3.1.2 into the venv, then
# rewrite pyspark/cloudpickle/__init__.py to re-export from the standalone
# package. pyspark only ever touches cloudpickle through the package's
# top-level re-exports, so swapping the __init__ is sufficient — including
# for Python worker subprocesses, which re-import pyspark from disk.
#
# Idempotent: re-running with the patched __init__ in place is a no-op.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENV_PY="${REPO_ROOT}/.venv/bin/python"

if [[ ! -x "${VENV_PY}" ]]; then
  echo "error: ${VENV_PY} not found; create the venv first (uv venv)" >&2
  exit 1
fi

# uv-provisioned venvs don't ship pip; prefer `uv pip` when available and
# fall back to module-pip otherwise. Idempotent — both forms upgrade in
# place if cloudpickle is already at 3.1.2.
if command -v uv >/dev/null 2>&1; then
  VIRTUAL_ENV="${REPO_ROOT}/.venv" uv pip install --quiet 'cloudpickle==3.1.2'
else
  "${VENV_PY}" -m pip install --quiet 'cloudpickle==3.1.2'
fi

CP_INIT=$("${VENV_PY}" -c 'import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), "cloudpickle", "__init__.py"))')

if grep -q "patched-for-py314" "${CP_INIT}" 2>/dev/null; then
  echo "already patched: ${CP_INIT}"
  exit 0
fi

cat >"${CP_INIT}" <<'PY'
# patched-for-py314: pyspark 3.5 bundles cloudpickle 2.2.1, which stack-
# overflows on CPython 3.14. Re-export the standalone cloudpickle 3.x so
# both driver and worker Python processes see the fixed implementation.
# See scripts/fix_pyspark_py314.sh for the rationale.
from cloudpickle import (  # noqa: F401
    CloudPickler,
    __version__,
    dump,
    dumps,
    load,
    loads,
)

Pickler = CloudPickler
PY

echo "patched ${CP_INIT} (cloudpickle 3.1.2 re-export)"
