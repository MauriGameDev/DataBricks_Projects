#+==========================   DataBricks  ====================================+
#|                            Bronze Layer                                     |
#|       Ingest raw CSV files from the Volume into Bronze Delta tables.        |
#|                  One table per year MetaData Added                          |
#|            (Ingestion timestamp and source file location).                  |
#+=============================================================================+

#Run in DataBricks ONLY! -> %run ../Utilities/utilities_ETL

# Read CSVs from Volume
df_2024 = read_csv(FILE_2024, 2024)
df_2025 = read_csv(FILE_2025, 2025)
print(f"2024 rows: {df_2024.count()}")
print(f"2025 rows: {df_2025.count()}")

# Preview dataframes
display(df_2024.limit(10))
display(df_2025.limit(10))

# Write to bronze table
write_delta(df_2024, BRONZE_2024)
write_delta(df_2025, BRONZE_2025)

# Verify table
verify_table(BRONZE_2024)
verify_table(BRONZE_2025)





