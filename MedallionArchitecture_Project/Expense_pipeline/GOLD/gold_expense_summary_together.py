# Databricks notebook source
# =============================================================================
# gold/gold_expense_summary.py
# Reads from both Silver Delta tables, unions 2024 + 2025,
# applies aggregations, and writes to one combined Gold table.
# =============================================================================

# COMMAND ----------

%run ../utils/pipeline_utils

# COMMAND ----------
# TITLE: 1. Read from Silver

df_silver_2024 = spark.table(SILVER_2024)
df_silver_2025 = spark.table(SILVER_2025)

print(f"Silver 2024 rows : {df_silver_2024.count()}")
print(f"Silver 2025 rows : {df_silver_2025.count()}")

# COMMAND ----------
# TITLE: 2. Union 2024 + 2025

df_combined = df_silver_2024.unionByName(df_silver_2025)

print(f"Combined rows    : {df_combined.count()}")

# COMMAND ----------
# TITLE: 3. Preview Combined

display(df_combined.limit(10))

# COMMAND ----------
# TITLE: 4. Define build_gold() Aggregation

from pyspark.sql import functions
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

def build_gold(df: DataFrame) -> DataFrame:
    """
    Apply Gold layer aggregations to the combined Silver DataFrame.
    Aggregations:
      1. Total spend by Category + report_year
      2. Total spend by Vendor + report_year
      3. Total spend by Month
      4. Transaction count by Category + report_year
      5. Average cost per item by Category + report_year
      6. Top spending Category per year
    """

    # 1. Total spend + transaction count + average cost by Category per year
    df_by_category = (
        df.groupBy("Category", "report_year")
        .agg(
            functions.round(functions.sum("TotalCost"), 2).alias("TotalSpend"),
            functions.count("ExpenseID").alias("TransactionCount"),
            functions.round(functions.avg("Cost"), 2).alias("AvgCostPerItem")
        )
    )

    # 2. Total spend by Vendor per year
    df_by_vendor = (
        df.groupBy("Vendor", "report_year")
        .agg(
            functions.round(functions.sum("TotalCost"), 2).alias("TotalSpendByVendor")
        )
    )

    # 3. Total spend by Month (across both years)
    df_by_month = (
        df.withColumn("Month", functions.date_format(functions.col("Date"), "yyyy-MM"))
        .groupBy("Month")
        .agg(
            functions.round(functions.sum("TotalCost"), 2).alias("TotalSpendByMonth")
        )
        .orderBy("Month")
    )

    # 4. Top spending Category per year using a window function
    window_spec = Window.partitionBy("report_year").orderBy(
        functions.col("TotalSpend").desc()
    )

    df_top_category = (
        df_by_category
        .withColumn("SpendRank", functions.rank().over(window_spec))
        .filter(functions.col("SpendRank") == 1)
        .select(
            "report_year",
            functions.col("Category").alias("TopCategory"),
            functions.col("TotalSpend").alias("TopCategorySpend")
        )
    )

    # 5. Join category aggregations with top category per year
    df_gold = (
        df_by_category
        .join(df_top_category, on="report_year", how="left")
    )

    return df_gold

# COMMAND ----------
# TITLE: 5. Apply Aggregations

df_gold = build_gold(df_combined)

# COMMAND ----------
# TITLE: 6. Preview Gold

display(df_gold.orderBy("report_year", "Category"))

# COMMAND ----------
# TITLE: 7. Write to Gold Delta Table

write_delta(df_gold, GOLD_TABLE)

# COMMAND ----------
# TITLE: 8. Verify

verify_table(GOLD_TABLE)
