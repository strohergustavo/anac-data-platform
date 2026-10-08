# Databricks notebook source
# DBTITLE 1,Common — Setup
# MAGIC %md
# MAGIC # Common — Setup
# MAGIC
# MAGIC Shared by every notebook through `%run ../common/setup`. Defines the **catalog parameter** (so the same code runs in `dev` and `prod`), structured logging, and helpers that write Unity Catalog comments and tags with properly escaped SQL literals.

# COMMAND ----------

import logging
import time

dbutils.widgets.text("catalog", "airline_operations", "Unity Catalog catalog")
CATALOG = dbutils.widgets.get("catalog").strip()
if not CATALOG.replace("_", "").isalnum():
    raise ValueError(f"Invalid catalog name: {CATALOG!r}")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s | %(message)s",
    datefmt="%H:%M:%S",
    force=True,
)


def get_logger(name):
    """Logger namespaced under `anac`, for example `anac.bronze.vra`."""
    return logging.getLogger(f"anac.{name}")


def sql_str(text):
    """Escape a Python string for use inside a single quoted SQL literal."""
    return str(text).replace("\\", "\\\\").replace("'", "\\'")


def comment_table(table, comment):
    spark.sql(f"COMMENT ON TABLE {table} IS '{sql_str(comment)}'")


def comment_columns(table, comments):
    existing = set(spark.table(table).columns)
    missing = sorted(set(comments) - existing)
    if missing:
        logging.getLogger("anac.setup").warning("%s has no column(s) %s; skipping their comments", table, missing)
    for column, comment in comments.items():
        if column not in existing:
            continue
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN `{column}` COMMENT '{sql_str(comment)}'")


def tag_table(table, tags):
    pairs = ", ".join(f"'{sql_str(k)}' = '{sql_str(v)}'" for k, v in tags.items())
    spark.sql(f"ALTER TABLE {table} SET TAGS ({pairs})")
