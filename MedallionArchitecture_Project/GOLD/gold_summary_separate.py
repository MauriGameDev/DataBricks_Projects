# Databricks notebook source
# =============================================================================
# gold/gold_expense_summary.py
# Reads from Silver Delta tables, applies aggregations,
# and writes to Gold Delta tables. One table per year.
# =============================================================================



#%run ../utils/pipeline_utils

from pyspark.sql import functions
from pyspark.sql import DataFrame
from pyspark.sql.window import Window



# Read from Silver
df_silver_2024 = spark.table(SILVER_2024)
df_silver_2025 = spark.table(SILVER_2025)

print(f"Silver 2024 rows : {df_silver_2024.count()}")
print(f"Silver 2025 rows : {df_silver_2025.count()}")


#  Define build_gold() Aggregation
def build_gold(df: DataFrame) -> DataFrame:
    """
    Apply Gold layer aggregations to a cleaned Silver DataFrame.
    Aggregations:
       Total spend by Category
       Total spend by Vendor
       Total spend by Month
       Transaction count by Category
       Average cost per item by Category
       Top spending Category (derived from total spend)
    Returns a single aggregated summary DataFrame.
    """

    # 1. Total spend + transaction count + average cost by Category
    df_by_category = (
        df.groupBy("Category")
        .agg(
            functions.round(functions.sum("TotalCost"), 2).alias("TotalSpend"),
            functions.count("ExpenseID").alias("TransactionCount"),
            functions.round(functions.avg("Cost"), 2).alias("AvgCostPerItem")
        )
    )

    # 2. Total spend by Vendor
    df_by_vendor = (
        df.groupBy("Vendor")
        .agg(
            functions.round(functions.sum("TotalCost"), 2).alias("TotalSpendByVendor")
        )
    )

    # 3. Total spend by Month
    df_by_month = (
        df.withColumn("Month", functions.date_format(functions.col("Date"), "yyyy-MM"))
        .groupBy("Month")
        .agg(
            functions.round(functions.sum("TotalCost"), 2).alias("TotalSpendByMonth")
        )
        .orderBy("Month")
    )

    # 4. Top spending Category using a window function
    window_spec = Window.orderBy(functions.col("TotalSpend").desc())

    df_top_category = (
        df_by_category
        .withColumn("SpendRank", functions.rank().over(window_spec))
        .filter(functions.col("SpendRank") == 1)
        .select(
            functions.col("Category").alias("TopCategory"),
            functions.col("TotalSpend").alias("TopCategorySpend")
        )
    )

    # 5. Combine all aggregations into one gold summary DataFrame
    df_gold = (
        df_by_category
        .join(df_by_vendor, how="left",
              on=df_by_category["Category"] == df_by_vendor["Vendor"])
        .drop("Vendor")
        .crossJoin(df_top_category)
    )

    # Join monthly spend as a JSON-style summary column
    df_gold = df_gold.withColumn(
        "report_year", functions.lit(df.select("report_year").first()[0])
    )

    return df_gold


#  Apply Aggregations
df_gold_2024 = build_gold(df_silver_2024)
df_gold_2025 = build_gold(df_silver_2025)

# Preview — 2024
display(df_gold_2024)

#Preview — 2025
display(df_gold_2025)


# Write to Gold Delta Tables
write_delta(df_gold_2024, GOLD_2024)
write_delta(df_gold_2025, GOLD_2025)

# Verify
verify_table(GOLD_2024)
verify_table(GOLD_2025)
