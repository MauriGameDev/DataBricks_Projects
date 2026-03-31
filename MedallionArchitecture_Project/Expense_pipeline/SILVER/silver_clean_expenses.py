# Databricks notebook source
# =============================================================================
# silver/silver_clean_expenses.py
# Reads from Bronze Delta tables, applies cleaning transformations,
# and writes to Silver Delta tables. One table per year.
# =============================================================================

# COMMAND ----------
#run only in databricks
#%run ../utils/pipeline_utils

# COMMAND ----------
# TITLE: 1. Read from Bronze

df_bronze_2024 = spark.table(BRONZE_2024)
df_bronze_2025 = spark.table(BRONZE_2025)

print(f"Bronze 2024 rows : {df_bronze_2024.count()}")
print(f"Bronze 2025 rows : {df_bronze_2025.count()}")

# COMMAND ----------
# TITLE: 2. Define clean_expenses() Transformation

from pyspark.sql import functions
from pyspark.sql import DataFrame

def clean_expenses(df: DataFrame) -> DataFrame:
    """
    Apply Silver layer cleaning transformations to an expense DataFrame.
    Transformations:
      1. Drop empty rows (rows where ExpenseID is null)
      2. Fix column name typo: Quanitity -> Quantity
      3. Trim whitespace from Category
      4. Standardise Category casing (title case)
      5. Cast Date column to DateType
      6. Fill null Remarks with 'N/A'
      7. Add derived column TotalCost = Cost x Quantity
    """

    # 1. Drop empty rows — Excel padding rows have no ExpenseID
    df = df.filter(functions.col("ExpenseID").isNotNull())

    # 2. Fix typo: Quanitity -> Quantity
    df = df.withColumnRenamed("Quanitity", "Quantity")

    # 3. Trim whitespace from Category (e.g. "Technology " -> "Technology")
    df = df.withColumn("Category", functions.trim(functions.col("Category")))

    # 4. Standardise Category casing (e.g. "technology" -> "Technology")
    df = df.withColumn("Category", functions.initcap(functions.col("Category")))

    # 5. Cast Date column from string to DateType
    df = df.withColumn("Date", functions.to_date(functions.col("Date")))

    # 6. Fill null Remarks with "N/A"
    df = df.withColumn(
        "Remarks",
        functions.when(
            functions.col("Remarks").isNull(), functions.lit("N/A")
        ).otherwise(functions.col("Remarks"))
    )

    # 7. Add TotalCost = Cost x Quantity
    df = df.withColumn(
        "TotalCost",
        functions.round(functions.col("Cost") * functions.col("Quantity"), 2)
    )

    # Final column order
    df = df.select(
        "ExpenseID",
        "Items",
        "Category",
        "Cost",
        "Quantity",
        "TotalCost",
        "Vendor",
        "Date",
        "Remarks",
        "report_year",
        "source_file",
        "ingestion_timestamp"
    )

    return df

# COMMAND ----------
# TITLE: 3. Apply Transformations

df_silver_2024 = clean_expenses(df_bronze_2024)
df_silver_2025 = clean_expenses(df_bronze_2025)

print(f"Silver 2024 rows : {df_silver_2024.count()}")
print(f"Silver 2025 rows : {df_silver_2025.count()}")

# COMMAND ----------
# TITLE: 4. Preview — 2024

display(df_silver_2024.limit(10))

# COMMAND ----------
# TITLE: 5. Preview — 2025

display(df_silver_2025.limit(10))

# COMMAND ----------
# TITLE: 6. Write to Silver Delta Tables

write_delta(df_silver_2024, SILVER_2024)
write_delta(df_silver_2025, SILVER_2025)

# COMMAND ----------
# TITLE: 7. Verify

verify_table(SILVER_2024)
verify_table(SILVER_2025)
