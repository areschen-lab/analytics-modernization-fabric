# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "04adec83-db21-43c0-ab18-1e78c9ebe9ff",
# META       "default_lakehouse_name": "analytics_lakehouse",
# META       "default_lakehouse_workspace_id": "8b9b41fe-bfe7-4bd2-a935-1307e6a0dc4f",
# META       "known_lakehouses": [
# META         {
# META           "id": "04adec83-db21-43c0-ab18-1e78c9ebe9ff"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

## notebookutils.runtime.context

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 01 Imports

from pyspark.sql.functions import current_timestamp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 02 Configuration

standard_sources = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv"
}

raw_path = "Files/olist_adls"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 03 Standard CSV ingestion

for table_name, file_name in standard_sources.items():
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{raw_path}/{file_name}")
        .withColumn("ingested_at", current_timestamp())
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"bronze_{table_name}")
    )

    print(f"Loaded bronze_{table_name}: {df.count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 04 Review special ingestion

df_reviews = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .csv(f"{raw_path}/olist_order_reviews_dataset.csv")
    .withColumn("ingested_at", current_timestamp())
)

(
    df_reviews.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("bronze_order_reviews")
)

print(f"Loaded bronze_order_reviews: {df_reviews.count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 05 Validation

bronze_tables = [
    "bronze_customers",
    "bronze_geolocation",
    "bronze_order_items",
    "bronze_order_payments",
    "bronze_order_reviews",
    "bronze_orders",
    "bronze_products",
    "bronze_sellers",
    "bronze_category_translation"
]

for table in bronze_tables:
    df = spark.table(table)
    print(f"{table}: {df.count():,} rows | {len(df.columns)} columns")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
