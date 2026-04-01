"""
=============================================================================================
|                       -- DataBricks notenook --                                           |
| This is a shared utility function used across Bronze, Silver and Gold layers              |
| Instructions: RUN in DataBricks                                                           |
| %run../Utilities/utilities_ETL -> runs variables and functions from the utilities notebook|
=============================================================================================
"""

#built-in libraries
from pyspark.sql import functions
from pyspark.sql import DataFrame

# Catalog & Volume Config 
CATALOG = "cosmicfire_dev_dbx"
SCHEMA = "default"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/expenses/raw"

# FIle Paths
FILE_2024 = f"{VOLUME_PATH}/ExpenseReport_2024.csv"
FILE_2025 = f"{VOLUME_PATH}/ExpenseReport_2025.csv"

# Table Names -BRONZE
BRONZE_2024 = f"{CATALOG}.{SCHEMA}.bronze_expenses_2024"
BRONZE_2025 = f"{CATALOG}.{SCHEMA}.bronze_expenses_2025"

# Table Names - SILVER
SILVER_2024 = f"{CATALOG}.{SCHEMA}.silver_expenses_2024"
SILVER_2025 = f"{CATALOG}.{SCHEMA}.silver_expenses_2024"

#Table Names -GOLD
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.gold_expense_summary"

# Verify Utilities loaded
print("-- Utilities Loaded ✓ --")
print(f" CATALOG     : {CATALOG}")
print(f" SCHEMA      : {SCHEMA}")
print(f" Volume Path : {VOLUME_PATH}")


# FUNCTIONS ------------------------
# Read CSV Function

"""
    Read a CSV from a Databricks Volume into a Spark DataFrame.
    Adds metadata columns:
      - source_file         : original CSV filename
      - report_year         : year the file belongs to
      - ingestion_timestamp : timestamp of when this load ran
    No transformations applied — Bronze stays raw.
"""
def read_csv(path: str, year: int) -> DataFrame:
    return (
        spark.read
        .option("header", "true")
        .option("inferschema", "true")
        .csv(path)
        .withColumn("source_file", functions.lit(path.split("/")[-1]))
        .withColumn("report_year", functions.lit(year))
        .withColumn("ingestion_time", functions.current_timestamp())
    )



# WRITE DETLA TABLE
"""
Write a Spark Dataframe to a Delta table.
Args:
    df         : DataFrame to write
    table_name : fully qualified table name (catalog.schema.table)
    mode       : 'overwrite' (default) or 'append'
"""

def write_delta(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteschema", "true")
        .saveAsTable(table_name)
    )
    count = spark.table(table_name).count()
    print(f"Written to {table_name} - {count} rows ✓")


# Verify Table
"""
Print row count and display a sample from a Delta Table
Args: 
    table_name  : fully qualified table name
    rows        : number of sample rows to display
"""
def verify_table(table_name: str, rows: int = 5) -> None:
    count = spark.table(table_name).count()
    print(f"---{table_name} -- {count} total rows --")
    display(spark.table(table_name).limit(rows))
