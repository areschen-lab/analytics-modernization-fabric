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

from pyspark.sql.functions import current_timestamp, col
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 02 Configuration

# Run mode: full, backfill, incremental
run_mode = "full"
batch_id = ""

raw_path = "Files/olist_adls"
landing_path = "Files/replay/landing"

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

incremental_sources = {
    "order_events": "order_events_v1",
    "order_items": "order_items_v1",
    "order_payments": "payments_v1",
    "order_reviews": "reviews_v1"
}

assert run_mode in ("full", "backfill", "incremental")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 03 Standard CSV ingestion

if run_mode == "full":

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

if run_mode == "full":

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

## 05 Incremental Ingestion


if run_mode in ("backfill", "incremental"):

    if run_mode == "backfill":
        batch_id = "backfill"

    for table_name, folder in incremental_sources.items():

        df = (
            spark.read
            .format("delta")
            .load(f"{landing_path}/{folder}")
            .filter(col("batch_id") == batch_id)
            .withColumn("ingested_at", current_timestamp())
        )

        target = f"bronze_{table_name}"

        if df.count() == 0:
            print(f"{target}: no records")
            continue

        # Check event ID before ingestion
        assert df.filter(col("event_id").isNull()).count() == 0
        assert df.count() == df.select("event_id").distinct().count()

        if run_mode == "backfill":
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(target)
            )

        else:
            assert spark.catalog.tableExists(target)
            assert "event_id" in spark.table(target).columns

            (
                DeltaTable.forName(spark, target)
                .alias("t")
                .merge(
                    df.alias("s"),
                    "t.event_id = s.event_id"
                )
                .whenNotMatchedInsertAll()
                .execute()
            )

        print(f"Loaded {target}: {df.count():,} records")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 06 Validation

if run_mode == "full":

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

else:

    for table_name, folder in incremental_sources.items():

        table = f"bronze_{table_name}"
        df = spark.table(table)

        total = df.count()
        unique_events = df.select("event_id").distinct().count()

        batch_rows = df.filter(
            col("batch_id") == batch_id
        ).count()

        expected = (
            spark.read.format("delta")
            .load(f"{landing_path}/{folder}")
            .filter(col("batch_id") == batch_id)
            .count()
        )

        assert total == unique_events, f"Duplicate events in {table}"
        assert batch_rows == expected, f"Batch mismatch in {table}"

        print(
            f"{table}: {total:,} rows | "
            f"batch={batch_rows:,} | "
            f"duplicates={total - unique_events}"
        )

    print("PASS: Bronze ingestion validation")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
