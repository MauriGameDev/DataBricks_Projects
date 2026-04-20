# +=========================   Databricks Notebook  ===========================+
# |                              Silver Layer                                  |
# |       Reads from Bronze Delta tables, applies cleaning transformations,    |
# |          and writes to Silver Delta tables. One table per year.            |
# |                # Instructions: RUN utilities_ETF first!!                   |
# +============================================================================+

#run only in databricks -> %run ../utils/pipeline_utils

#import libraries
from pyspark.sql import DataFrame
from pyspark.sql import functions

#referencelbronze tables
df_bronze_2024 = spark.table(BRONZE_2024)
df_bronze_2025 = spark.table(BRONZE_2025)

#Display tables and print rows
#verify steps needed to clean and transform after displayed table
print(f"Bronze 2024 rows : {df_bronze_2024.count()}")
display(df_bronze_2024.limit(10))
print(f"Bronze 2025 rows : {df_bronze_2025.count()}")
display(df_bronze_2025.limit(10))

#Functions to clean based off needs
"""
Apply Silver layer Cleaning transformations to an expense dataframe transformations:
  - Fix column name typo: Quanitity -> Quantity
  - Trim whitespace from Category
  - Standardise casing: lower case
  - cast date column to date type
  - cast cost column to double type
  - cast quantity column to integer type
  - fill null remarks with 'N/A'
"""
def clean_expenses(df: DataFrame) -> DataFrame:
    # Fix typo: Quanitity -> Quantity
    df = df.withColumnRenamed("Quanitity", "Quantity")

    # Trim whitespace from catergory
    df = df.withColumn("Category", functions.trim(functions.col("Category")))

    # remove $ and optional commas
    df = df.withColumn(
      "Cost", functions.regexp_replace(functions.col("Cost"), "[$,]", "").cast("double"))
    
    #standardize lower case for column header
    df = df.withColumnRenamed("ExpenseID", "expenseid")
    df = df.withColumnRenamed("Category", "category")
    df = df.withColumnRenamed("Items", "items")
    df = df.withColumnRenamed("Cost", "cost")
    df = df.withColumnRenamed("Quantity", "quantity")
    df = df.withColumnRenamed("Date", "date")
    df = df.withColumnRenamed("Remarks", "remarks")
    df = df.withColumnRenamed("Vendor", "vendor")
   
    # Cast date column to date type")
    df = df.withColumn("date", functions.to_date(functions.col("date")))

    # First letter for all data is uppercase in category column
    df = df.withColumn("category", functions.initcap(functions.col("category")))

    # Fill null Remarks with "N/A"
    df = df.withColumn("remarks", functions.when(functions.col("remarks").isNull(), functions.lit("N/A")).otherwise(functions.col("remarks"))
    )

    # Add totalCost coulumn cost x quantity
    df = df.withColumn("total_cost", functions.round(functions.col("cost") * functions.col("quantity"), 2)
    )

    # Final column order
    df = df.select(
      "expenseid",
      "items",
      "category",
      "cost",
      "quantity",
      "total_cost",
      "date",
      "remarks",
      "vendor",
      "source_file",
      "report_year",
      "ingestion_time",
    )

    return df
  

#Apply Transformations
df_silver_2024 = clean_expenses(df_bronze_2024)
df_silver_2025 = clean_expenses(df_bronze_2025)
#print rows
print(f"Silver 2024 rows :{df_silver_2024.count()}")
print(f"Silver 2025 rows: {df_silver_2025.count()}")

# preview table
display(df_silver_2024.limit(10))
display(df_silver_2025.limit(10))

#write to delta table
write_delta(df_silver_2024, SILVER_2024)
write_delta(df_silver_2025, SILVER_2025)

#verify tables
verify_table(SILVER_2024)
verify_table(SILVER_2025)