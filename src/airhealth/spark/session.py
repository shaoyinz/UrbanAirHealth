"""Sedona-enabled SparkSession factory.

Lifted from ``floodpipe.spark.session`` (UrbanFloodRisk) verbatim except
for the app-name default. Dataproc Serverless 2.2 supplies Spark 3.5 on
Scala 2.13; the Sedona and geotools-wrapper jars are attached at submit
time (``scripts/submit_silver.sh``). When ``local_packages=True``, this
module instead asks Spark to pull the matching Maven artifacts via
``spark.jars.packages`` so a ``uv``-provisioned pyspark install can run
the same job against a fixture without a Dataproc submit.

Pip-installed pyspark 3.5 ships Scala 2.12; Dataproc Serverless 2.2
ships 2.13. ``_detect_pyspark_scala_suffix`` reads the on-disk pyspark
jar inventory at runtime and picks the matching coordinate, so the same
function works on either runtime.
"""

from __future__ import annotations

import glob
import importlib.util
import os
import sys
import types

# Keep in sync with scripts/submit_silver.sh: Dataproc Serverless 2.2
# pins us to Sedona 1.6.1 (latest with a Scala 2.13 shaded artifact).
SEDONA_VERSION = "1.6.1"
GEOTOOLS_WRAPPER_VERSION = f"{SEDONA_VERSION}-28.2"


def _install_rasterio_shim() -> None:
    """Stub sedona.raster so ``import sedona.spark`` doesn't pull rasterio.

    ``sedona.spark.__init__`` eagerly imports ``sedona.raster.sedona_raster``,
    whose first line is ``import rasterio``. The Dataproc Serverless 2.2
    base image doesn't include rasterio and this job touches only the
    JVM-side Sedona SQL functions, so the stub keeps the import path
    cleanly resolvable. Skipped when rasterio actually imports (local
    dev), so the real Sedona raster Python types stay in play.
    """
    if importlib.util.find_spec("rasterio") is not None:
        return
    for name in (
        "sedona.raster",
        "sedona.raster.raster_serde",
        "sedona.raster.sedona_raster",
    ):
        sys.modules.setdefault(name, types.ModuleType(name))
    sys.modules["sedona.raster.raster_serde"].deserialize = lambda *_a, **_k: None

    class _SedonaRasterStub:
        __UDT__ = None

    sys.modules["sedona.raster.sedona_raster"].SedonaRaster = _SedonaRasterStub


_install_rasterio_shim()

from sedona.spark import SedonaContext  # noqa: E402  (must follow the shim)

# Sedona needs Kryo for its geometry serializers; without these settings
# spatial joins fall back to (slow) Java serialization.
_SEDONA_BASE_CONF: dict[str, str] = {
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.registrator": "org.apache.sedona.core.serde.SedonaKryoRegistrator",
    "spark.sql.extensions": (
        "org.apache.sedona.viz.sql.SedonaVizExtensions,"
        "org.apache.sedona.sql.SedonaSqlExtensions"
    ),
}


def _detect_pyspark_scala_suffix() -> str:
    """Return ``"2.12"`` or ``"2.13"`` based on the bundled scala-library jar.

    Pip-installed pyspark ships exactly one ``scala-library-X.Y.Z.jar``
    under ``pyspark/jars/``; the X.Y of that file is the Scala build the
    runtime expects matching Sedona artifacts for. Defaults to 2.12 if
    pyspark isn't import-resolvable.
    """
    try:
        import pyspark
    except ImportError:
        return "2.12"
    jars_dir = os.path.join(os.path.dirname(pyspark.__file__), "jars")
    for path in glob.glob(os.path.join(jars_dir, "scala-library-*.jar")):
        name = os.path.basename(path)
        parts = name.removeprefix("scala-library-").split(".")
        if len(parts) >= 2:
            return f"{parts[0]}.{parts[1]}"
    return "2.12"


def _local_sedona_packages() -> str:
    scala = _detect_pyspark_scala_suffix()
    return (
        f"org.apache.sedona:sedona-spark-shaded-3.5_{scala}:{SEDONA_VERSION},"
        f"org.datasyslab:geotools-wrapper:{GEOTOOLS_WRAPPER_VERSION}"
    )


def sedona_session(
    app_name: str = "airhealth-silver",
    extra_conf: dict[str, str] | None = None,
    *,
    local_packages: bool = False,
):
    """Build (or attach to) a Sedona-enabled SparkSession.

    ``local_packages=True`` adds ``spark.jars.packages`` so a bare
    pip-installed pyspark can fetch Sedona JARs from Maven Central on
    session start — for unit-test runs and notebook smoke tests. On
    Dataproc Serverless leave it False: the submit command wires a
    Scala-2.13 build via ``--properties`` and we must not re-set the
    property to a 2.12 coordinate from code.

    Returns the Sedona-wrapped session; caller is responsible for
    ``.stop()``.
    """
    conf: dict[str, str] = dict(_SEDONA_BASE_CONF)
    if local_packages:
        conf["spark.jars.packages"] = _local_sedona_packages()
        # Pin worker python to the driver venv. Unset PYSPARK_PYTHON falls
        # back to `python3` on PATH, which on macOS resolves to Homebrew's
        # interpreter — a different venv with no cloudpickle, surfacing
        # as ModuleNotFoundError deep in the worker. Leave anything the
        # operator has already exported alone, and never touch this in
        # Dataproc Serverless mode (local_packages=False) where the
        # runtime sets PYSPARK_PYTHON to the batch's own interpreter.
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    conf.update(extra_conf or {})

    builder = SedonaContext.builder().appName(app_name)
    for key, value in conf.items():
        builder = builder.config(key, value)
    return SedonaContext.create(builder.getOrCreate())
