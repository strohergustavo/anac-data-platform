import os
import pytest
from pyspark.sql import SparkSession


def _is_databricks():
    """True when running inside Databricks (Spark Connect), where a
    session already exists and ``master('local')`` is not allowed."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


@pytest.fixture(scope="session")
def spark():
    if _is_databricks():
        # Databricks: reuse the existing Spark Connect session and apply
        # test configs at runtime (builder configs are ignored once a
        # session already exists).
        session = SparkSession.builder.getOrCreate()
        for key, val in [
            ("spark.sql.shuffle.partitions", "1"),
            ("spark.sql.session.timeZone", "UTC"),
            ("spark.sql.ansi.enabled", "false"),
        ]:
            try:
                session.conf.set(key, val)
            except Exception:
                pass  # config may be static in some runtimes — not critical
        yield session
    else:
        # Local / CI: create an isolated local Spark session.
        session = (
            SparkSession.builder.master("local[1]")
            .appName("anac-tests")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", "false")
            .getOrCreate()
        )
        yield session
        session.stop()
