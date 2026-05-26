"""Sedona / Dataproc Serverless wiring.

This package's ``__init__`` exists primarily to install Python 3.14
shims before anything in the package triggers a pyspark import.
PySpark 3.5 ships with a dozen ``from distutils.version import
LooseVersion`` statements scattered across its modules; CPython 3.14
removed the ``distutils`` package outright (PEP 632), so pyspark fails
to import on a uv-provisioned 3.14 venv without intervention.

The shim is a < 30-line in-process module injection — no venv mutation,
no setuptools fallback. Importing ``airhealth.spark`` from anywhere in
the repo (the smoke script, the silver job, the integration tests)
flips it on, so callers don't have to know it exists.

The cloudpickle patch lives in ``scripts/fix_pyspark_py314.sh`` because
it has to modify a file inside ``site-packages/pyspark/cloudpickle/``
that worker subprocesses re-import from disk — sys.modules injection
won't reach them. The distutils shim, by contrast, only needs to be
present in the driver process, which is the only place ``LooseVersion``
is referenced in the modules our silver job touches.
"""

from __future__ import annotations

import sys
import types


def _install_distutils_shim() -> None:
    """Synthesise ``distutils.version.LooseVersion`` for PEP-632 stacks.

    LooseVersion's original implementation in cpython compared via a
    tuple of int/str segments, which broke under Python 3 because mixed-
    type tuple comparison raises ``TypeError``. The replacement here
    sidesteps that with a leading 0/1 sort key per segment — numeric
    segments compare before string segments at the same depth, which
    matches the dominant pyspark callsite ("library version >= X.Y.Z").
    Faithful enough for the comparisons pyspark actually performs;
    swap for ``packaging.version.Version`` when/if pyspark ever drops
    the import itself.
    """
    if "distutils.version" in sys.modules:
        return

    class LooseVersion:
        def __init__(self, vstring: object) -> None:
            self.vstring = str(vstring)
            self.version = tuple(self._parse(self.vstring))

        @staticmethod
        def _parse(v: str) -> list[tuple[int, object]]:
            parts: list[tuple[int, object]] = []
            for piece in v.split("."):
                try:
                    parts.append((0, int(piece)))
                except ValueError:
                    parts.append((1, piece))
            return parts

        def _key(self) -> tuple:
            return self.version

        def __lt__(self, other: object) -> bool:
            if not isinstance(other, LooseVersion):
                return NotImplemented
            return self._key() < other._key()

        def __le__(self, other: object) -> bool:
            if not isinstance(other, LooseVersion):
                return NotImplemented
            return self._key() <= other._key()

        def __gt__(self, other: object) -> bool:
            if not isinstance(other, LooseVersion):
                return NotImplemented
            return self._key() > other._key()

        def __ge__(self, other: object) -> bool:
            if not isinstance(other, LooseVersion):
                return NotImplemented
            return self._key() >= other._key()

        def __eq__(self, other: object) -> bool:
            if not isinstance(other, LooseVersion):
                return NotImplemented
            return self._key() == other._key()

        def __ne__(self, other: object) -> bool:
            return not (self == other)

        def __hash__(self) -> int:
            return hash(self.version)

        def __repr__(self) -> str:
            return f"LooseVersion ('{self.vstring}')"

        def __str__(self) -> str:
            return self.vstring

    distutils = sys.modules.setdefault("distutils", types.ModuleType("distutils"))
    version = types.ModuleType("distutils.version")
    version.LooseVersion = LooseVersion  # type: ignore[attr-defined]
    distutils.version = version  # type: ignore[attr-defined]
    sys.modules["distutils.version"] = version


_install_distutils_shim()
