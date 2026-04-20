# Usage in other notebooks:
#   %run ../utils/helpers

"""
============================================================================================
                          -- DataBricks notenook --
    This is a shared utility function used across Bronze, Silver and Gold layers
Instructions: RUN 
============================================================================================
"""

from pyspark.sql import functions
from pyspark.sql import DataFrame

# COMMAND ----------
# TITLE: Catalog & Volume Config (Single Source of Truth)

CATALOG     = "cosmicfire_dev_dbx"
SCHEMA      = "default"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/expenses/raw"

# File paths
FILE_2024   = f"{VOLUME_PATH}/ExpenseReport_2024.csv"
FILE_2025   = f"{VOLUME_PATH}/ExpenseReport_2025.csv"

# Table names — Bronze
BRONZE_2024 = f"{CATALOG}.{SCHEMA}.bronze_expenses_2024"
BRONZE_2025 = f"{CATALOG}.{SCHEMA}.bronze_expenses_2025"

# Table names — Silver
SILVER_2024 = f"{CATALOG}.{SCHEMA}.silver_expenses_2024"
SILVER_2025 = f"{CATALOG}.{SCHEMA}.silver_expenses_2025"

# Table names — Gold
GOLD_TABLE  = f"{CATALOG}.{SCHEMA}.gold_expense_summary"

print("✓ helpers.py loaded")
print(f"  Catalog     : {CATALOG}")
print(f"  Schema      : {SCHEMA}")
print(f"  Volume path : {VOLUME_PATH}")

# COMMAND ----------
# TITLE: read_csv()

def read_csv(path: str, year: int) -> DataFrame:
    """
    Read a CSV from a Databricks Volume into a Spark DataFrame.
    Adds metadata columns:
      - source_file         : original CSV filename
      - report_year         : year the file belongs to
      - ingestion_timestamp : timestamp of when this load ran
    No transformations applied — Bronze stays raw.
    """
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
        .withColumn("source_file", functions.lit(path.split("/")[-1]))
        .withColumn("report_year", functions.lit(year))
        .withColumn("ingestion_timestamp", functions.current_timestamp())
    )

# COMMAND ----------
# TITLE: write_delta()

def write_delta(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    """
    Write a Spark DataFrame to a Delta table.
    Args:
        df         : DataFrame to write
        table_name : fully qualified table name (catalog.schema.table)
        mode       : 'overwrite' (default) or 'append'
    """
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    count = spark.table(table_name).count()
    print(f"✓ Written to {table_name} — {count} rows")

# COMMAND ----------
# TITLE: verify_table()

def verify_table(table_name: str, rows: int = 5) -> None:
    """
    Print row count and display a sample from a Delta table.
    Args:
        table_name : fully qualified table name
        rows       : number of sample rows to display (default 5)
    """
    count = spark.table(table_name).count()
    print(f"── {table_name} — {count} total rows ──")
    display(spark.table(table_name).limit(rows))
