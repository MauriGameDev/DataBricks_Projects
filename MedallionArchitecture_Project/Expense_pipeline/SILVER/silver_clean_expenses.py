# Databricks notebook source
# =============================================================================
# silver/silver_clean_expenses.py
# Reads from Bronze Delta tables, applies cleaning transformations,
# and writes to Silver Delta tables. One table per year.
# =============================================================================

#Define clean_expenses() Transformation

#run only in databricks
#%run ../utils/pipeline_utils


# import functions and dataframe
from pyspark.sql import functions
from pyspark.sql import DataFrame


# Read from Bronze table and assign it to df_bronze
df_bronze_2024 = spark.table(BRONZE_2024)
df_bronze_2025 = spark.table(BRONZE_2025)


#Display tables and print rows
#verify steps needed to clean and transform after displayed table
print(f"Bronze 2024 rows : {df_bronze_2024.count()}")
display(df_bronze_2024)
print(f"Bronze 2025 rows : {df_bronze_2025.count()}")
display(df_bronze_2025)

#Functions to clean based off needs
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
    
    #standardize lower case for column header
    df = df.withColumnRenamed("ExpenseID", functions.lower("ExpenseID"))
    df = df.withColumnRenamed("Category", functions.lower("Category"))
    df = df.withColumnRenamed("Items", functions.lower("Items"))
    df = df.withColumnRenamed("Cost", functions.lower("Cost"))
    df = df.withColumnRenamed("Quantity", functions.lower("Quantity"))
    df = df.withColumnRenamed("Date", functions.lower("Date"))
    df = df.withColumnRenamed("Remarks", functions.lower("Remarks"))
    df = df.withColumnRenamed("Vendor", functions.lower("Vendor"))

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


# TITLE: 3. Apply Transformations

df_silver_2024 = clean_expenses(df_bronze_2024)
df_silver_2025 = clean_expenses(df_bronze_2025)

print(f"Silver 2024 rows : {df_silver_2024.count()}")
print(f"Silver 2025 rows : {df_silver_2025.count()}")


# TITLE: 4. Preview — 2024

display(df_silver_2024.limit(10))


# TITLE: 5. Preview — 2025

display(df_silver_2025.limit(10))


# TITLE: 6. Write to Silver Delta Tables

write_delta(df_silver_2024, SILVER_2024)
write_delta(df_silver_2025, SILVER_2025)


# TITLE: 7. Verify

verify_table(SILVER_2024)
verify_table(SILVER_2025)
