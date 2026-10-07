"""Pull the exact SQL that the Databricks notebooks run, so tests exercise production logic.

Fully qualified names such as ``airline_operations.silver.vra`` become local temp views
(``silver_vra``), and ``CREATE OR REPLACE TABLE ... AS`` is reduced to its query.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CATALOG = "airline_operations"


def _statements(path):
    text = (ROOT / path).read_text(encoding="utf-8")
    if path.endswith(".sql"):
        return [s for s in text.split(";") if s.strip()]
    # Python notebooks: SQL lives in spark.sql("""...""") strings or %sql magic cells.
    found = re.findall(r'spark\.sql\(\s*"""(.*?)"""', text, re.S)
    for cell in text.split("# COMMAND ----------"):
        lines = cell.strip().splitlines()
        if lines and lines[0].strip() == "# MAGIC %sql":
            found.append("\n".join(re.sub(r"^# MAGIC ?", "", l) for l in lines[1:]))
    return found


def _localize(sql):
    return re.sub(rf"\b{CATALOG}\.(\w+)\.(\w+)\b", r"\1_\2", sql)


def query_for(path, target):
    """Return the SELECT that builds ``target`` (for example ``gold.fact_flights``)."""
    pattern = re.compile(
        rf"CREATE\s+(?:OR\s+REPLACE\s+TABLE|OR\s+REFRESH\s+MATERIALIZED\s+VIEW|TEMPORARY\s+VIEW|LIVE\s+VIEW)\s+"
        rf"(?:{CATALOG}\.)?{re.escape(target)}\b.*?\bAS\b\s*(.*)",
        re.S | re.I,
    )
    for statement in _statements(path):
        match = pattern.search(statement)
        if match:
            return _localize(match.group(1)).strip().rstrip(";")
    raise LookupError(f"{target} not found in {path}")
