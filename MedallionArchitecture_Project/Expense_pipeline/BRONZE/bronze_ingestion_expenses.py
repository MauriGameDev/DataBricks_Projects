# Databricks notebook source
# =============================================================================
# bronze/bronze_raw_expenses.py
# Ingests raw CSV files from the Volume into Bronze Delta tables.
# One table per year — no transformations applied.
# =============================================================================

# COMMAND ----------
# TITLE: 1. Load Pipeline Utils

#uncomment this line, only use in databricks
#%run ../utils/pipeline_utils

# COMMAND ----------
# TITLE: 2. Read CSVs from Volume

df_2024 = read_csv(FILE_2024, 2024)
df_2025 = read_csv(FILE_2025, 2025)

print(f"2024 rows: {df_2024.count()}")
print(f"2025 rows: {df_2025.count()}")

# COMMAND ----------
# TITLE: 3. Preview — 2024

display(df_2024.limit(10))

# COMMAND ----------
# TITLE: 4. Preview — 2025

display(df_2025.limit(10))

# COMMAND ----------
# TITLE: 5. Write to Bronze Delta Tables

write_delta(df_2024, BRONZE_2024)
write_delta(df_2025, BRONZE_2025)

# COMMAND ----------
# TITLE: 6. Verify

verify_table(BRONZE_2024)
verify_table(BRONZE_2025)
