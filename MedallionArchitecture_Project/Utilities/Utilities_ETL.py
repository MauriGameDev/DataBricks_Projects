#|=================================  DataBricks NoteBook =========================================|
#|                                                                                                |
#|          This is a shared utility function used across Bronze, Silver and Gold layers          |
#|                                  Instructions: RUN                                             |
#|%run../Utilities/utilities_ETL -> runs variables and functions from the utilities notebook      |
#|================================================================================================|

#Import Libraries 
from pyspark.sql import functions
from pyspark.sql import DataFrame

# Catalog & Volume Config 
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
BRONZE_2024 = f"{CATALOG}.{BRONZE_SCHEMA}.expenses_2024"
BRONZE_2025 = f"{CATALOG}.{BRONZE_SCHEMA}.expenses_2025"

# Table names — Silver
SILVER_2024 = f"{CATALOG}.{SILVER_SCHEMA}.expenses_2024"
SILVER_2025 = f"{CATALOG}.{SILVER_SCHEMA}.expenses_2025"


# Table Names - GOLD
GOLD_SPEND_BY_CATEGORY     = f"{CATALOG}.{GOLD_SCHEMA}.gold_spend_by_category"
GOLD_SPEND_BY_VENDOR       = f"{CATALOG}.{GOLD_SCHEMA}.gold_spend_by_vendor"
GOLD_SPEND_BY_MONTH        = f"{CATALOG}.{GOLD_SCHEMA}.gold_spend_by_month"
GOLD_TOTAL_SPEND_BY_YEAR   = f"{CATALOG}.{GOLD_SCHEMA}.gold_total_spend_by_year"
GOLD_TOP_SPENDING_CATEGORY = f"{CATALOG}.{GOLD_SCHEMA}.gold_top_spending_category"

# Print to the screen to verify its loaded
print("✓ helpers.py loaded")
print(f"  Catalog     : {CATALOG}")
print(f"  Schema      : {SCHEMA}")
print(f"  Bronze      : {BRONZE_SCHEMA}")
print(f"  Silver      : {SILVER_SCHEMA}")
print(f"  Gold        : {GOLD_SCHEMA}")
print(f"  Volume path : {VOLUME_PATH}")

#FUNCTIONS----------------------------------------------------------------------
# Read CSV file function 
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

# Write to Delta Table function()
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

#  Verify Table Function
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
