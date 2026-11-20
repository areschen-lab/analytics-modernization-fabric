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
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

notebookutils.runtime.context

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 01 Imports

from pyspark.sql.functions import (
    col,
    when,
    lit,
    coalesce,
    datediff,
    count,
    countDistinct,
    avg,
    min,
    max,
    row_number
)

from pyspark.sql.window import Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

## 02 Configuration

# Run mode: full, backfill, incremental
run_mode = "full"
batch_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 03 Orders Transformation

assert run_mode in ("full", "backfill", "incremental")
assert run_mode != "incremental" or batch_id

if run_mode == "full":
    silver_orders = (
        spark.table("bronze_orders")
        .withColumn(
            "delivery_days",
            datediff(
                col("order_delivered_customer_date"),
                col("order_purchase_timestamp")
            )
        )
        .withColumn(
            "delay_days",
            datediff(
                col("order_delivered_customer_date"),
                col("order_estimated_delivery_date")
            )
        )
        .withColumn(
            "is_late",
            when(
                col("order_delivered_customer_date").isNull(),
                lit(None).cast("int")
            )
            .when(col("delay_days") > 0, 1)
            .otherwise(0)
        )
        .drop("ingested_at")
    )

    (
        silver_orders.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("silver_orders")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 04 Products Transformation

if run_mode == "full":

    products = spark.table("bronze_products").alias("p")
    categories = spark.table("bronze_category_translation").alias("c")

    silver_products = (
        products
        .join(
            categories,
            col("p.product_category_name") == col("c.product_category_name"),
            how="left"
        )
        .withColumn(
            "product_category",
            coalesce(
                col("c.product_category_name_english"),
                col("p.product_category_name"),
                lit("unknown")
            )
        )
        .select(
            "p.*",
            "product_category"
        )
        .drop("ingested_at")
    )

    (
        silver_products.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("silver_products")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 05 Geolocation Transformation

if run_mode == "full":

    geo = spark.table("bronze_geolocation")

    geo_location_counts = (
        geo
        .groupBy(
            "geolocation_zip_code_prefix",
            "geolocation_city",
            "geolocation_state"
        )
        .agg(
            count("*").alias("location_count")
        )
    )

    geo_window = (
        Window
        .partitionBy("geolocation_zip_code_prefix")
        .orderBy(
            col("location_count").desc(),
            col("geolocation_state").asc(),
            col("geolocation_city").asc()
        )
    )

    geo_mode = (
        geo_location_counts
        .withColumn(
            "rn",
            row_number().over(geo_window)
        )
        .filter(col("rn") == 1)
        .select(
            "geolocation_zip_code_prefix",
            col("geolocation_city").alias("city"),
            col("geolocation_state").alias("state")
        )
    )

    geo_selected = (
        geo.alias("g")
        .join(
            geo_mode.alias("m"),
            (
                col("g.geolocation_zip_code_prefix")
                == col("m.geolocation_zip_code_prefix")
            )
            & (col("g.geolocation_city") == col("m.city"))
            & (col("g.geolocation_state") == col("m.state")),
            how="inner"
        )
    )

    silver_geolocation = (
        geo_selected
        .groupBy(
            col("m.geolocation_zip_code_prefix").alias(
                "geolocation_zip_code_prefix"
            ),
            col("m.city").alias("city"),
            col("m.state").alias("state")
        )
        .agg(
            avg("g.geolocation_lat").alias("latitude"),
            avg("g.geolocation_lng").alias("longitude")
        )
    )

    (
        silver_geolocation.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("silver_geolocation")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 06 Customers Transformation

if run_mode == "full":

    customers = spark.table("bronze_customers").alias("c")
    geo = spark.table("silver_geolocation").alias("g")

    silver_customers = (
        customers
        .join(
            geo,
            col("c.customer_zip_code_prefix")
            == col("g.geolocation_zip_code_prefix"),
            how="left"
        )
        .select(
            "c.*",
            col("g.city").alias("geo_city"),
            col("g.state").alias("geo_state"),
            col("g.latitude"),
            col("g.longitude")
        )
        .drop("ingested_at")
    )

    (
        silver_customers.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("silver_customers")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 07 Sellers Transformation

if run_mode == "full":

    sellers = spark.table("bronze_sellers").alias("s")
    geo = spark.table("silver_geolocation").alias("g")

    silver_sellers = (
        sellers
        .join(
            geo,
            col("s.seller_zip_code_prefix")
            == col("g.geolocation_zip_code_prefix"),
            how="left"
        )
        .select(
            "s.*",
            col("g.city").alias("geo_city"),
            col("g.state").alias("geo_state"),
            col("g.latitude"),
            col("g.longitude")
        )
        .drop("ingested_at")
    )

    (
        silver_sellers.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("silver_sellers")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 08 Order Items Transformation

if run_mode == "full":

    silver_order_items = (
        spark.table("bronze_order_items")
        .withColumn(
            "item_total",
            col("price") + col("freight_value")
        )
        .drop("ingested_at")
    )

    (
        silver_order_items.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("silver_order_items")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 09 Payments and Reviews Transformation

if run_mode == "full":

    silver_order_payments = (
        spark.table("bronze_order_payments")
        .drop("ingested_at")
    )

    (
        silver_order_payments.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("silver_order_payments")
    )

    silver_order_reviews = (
        spark.table("bronze_order_reviews")
        .drop("ingested_at")
    )

    (
        silver_order_reviews.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("silver_order_reviews")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


## 10 Incremental Items, Payments and Reviews

if run_mode in ("backfill", "incremental"):

    current_batch = (
        "backfill" if run_mode == "backfill" else batch_id
    )

    tables = {
        "order_items": ["order_id", "order_item_id"],
        "order_payments": ["order_id", "payment_sequential"],
        "order_reviews": ["review_id", "order_id"]
    }

    for name, keys in tables.items():

        df = (
            spark.table(f"bronze_{name}")
            .filter(F.col("batch_id") == current_batch)
        )

        if name == "order_items":
            df = (
                df
                .withColumn("order_item_id", F.col("order_item_id").cast("int"))
                .withColumn("price", F.col("price").cast("decimal(18,2)"))
                .withColumn("freight_value", F.col("freight_value").cast("decimal(18,2)"))
                .withColumn("item_total", F.col("price") + F.col("freight_value"))
                .withColumn("shipping_limit_date", F.col("shipping_limit_date").cast("timestamp"))
            )

        elif name == "order_payments":
            df = (
                df
                .withColumn("payment_sequential", F.col("payment_sequential").cast("int"))
                .withColumn("payment_installments", F.col("payment_installments").cast("int"))
                .withColumn("payment_value", F.col("payment_value").cast("decimal(18,2)"))
            )

        elif name == "order_reviews":
            df = (
                df
                .withColumn("review_score", F.col("review_score").cast("int"))
                .withColumn("review_creation_date", F.col("review_creation_date").cast("timestamp"))
                .withColumn("review_answer_timestamp", F.col("review_answer_timestamp").cast("timestamp"))
            )

        df = df.drop(
            "event_id",
            "arrival_rule",
            "simulated_delay"
        )

        target = f"silver_{name}"

        assert df.count() == df.select(*keys).distinct().count()

        if run_mode == "backfill":
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(target)
            )

        else:
            condition = " AND ".join(
                f"t.{key} = s.{key}" for key in keys
            )

            (
                DeltaTable.forName(spark, target)
                .alias("t")
                .merge(df.alias("s"), condition)
                .whenNotMatchedInsertAll()
                .execute()
            )

        print(f"{target}: {spark.table(target).count():,} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


## 11 Incremental Orders Transformation

if run_mode in ("backfill", "incremental"):

    current_batch = (
        "backfill" if run_mode == "backfill" else batch_id
    )

    events = spark.table("bronze_order_events")

    # Orders affected by this batch
    changed_orders = (
        events
        .filter(F.col("batch_id") == current_batch)
        .select("order_id")
        .distinct()
    )

    # Rebuild state using events already received
    history = (
        events
        .join(changed_orders, "order_id")
        .filter(
            (F.col("batch_id") == "backfill") |
            (F.col("batch_id") <= current_batch)
        )
    )

    silver_orders = (
        history.groupBy("order_id")
        .agg(
            F.max(F.when(F.col("event_type") == "CREATED",
                         F.col("customer_id"))).alias("customer_id"),

            F.max(F.when(F.col("event_type") == "CREATED",
                         F.col("event_time"))).alias("order_purchase_timestamp"),

            F.max(F.when(F.col("event_type") == "CREATED",
                         F.col("estimated_delivery_date"))).alias("order_estimated_delivery_date"),

            F.max(F.when(F.col("event_type") == "APPROVED",
                         F.col("event_time"))).alias("order_approved_at"),

            F.max(F.when(F.col("event_type") == "SHIPPED",
                         F.col("event_time"))).alias("order_delivered_carrier_date"),

            F.max(F.when(F.col("event_type") == "DELIVERED",
                         F.col("event_time"))).alias("order_delivered_customer_date")
        )
        .withColumn(
            "order_status",
            F.when(F.col("order_delivered_customer_date").isNotNull(), "delivered")
             .when(F.col("order_delivered_carrier_date").isNotNull(), "shipped")
             .when(F.col("order_approved_at").isNotNull(), "approved")
             .otherwise("created")
        )
        .withColumn(
            "delivery_days",
            F.datediff("order_delivered_customer_date", "order_purchase_timestamp")
        )
        .withColumn(
            "delay_days",
            F.datediff("order_delivered_customer_date", "order_estimated_delivery_date")
        )
        .withColumn(
            "is_late",
            F.when(F.col("order_delivered_customer_date").isNull(), F.lit(None).cast("int"))
             .when(F.col("delay_days") > 0, 1)
             .otherwise(0)
        )
        .withColumn(
            "as_of_date",
            F.lit("2018-06-09" if run_mode == "backfill" else batch_id).cast("date")
        )
    )

    assert silver_orders.filter(
        F.col("order_purchase_timestamp").isNull()
    ).count() == 0

    if run_mode == "backfill":
        (
            silver_orders.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable("silver_orders")
        )

    else:
        (
            DeltaTable.forName(spark, "silver_orders")
            .alias("t")
            .merge(
                silver_orders.alias("s"),
                "t.order_id = s.order_id"
            )
            .whenMatchedUpdateAll(
                condition="s.as_of_date >= t.as_of_date"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    print(
        "Silver Orders:",
        spark.table("silver_orders").count()
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 12 Validation

silver_tables = [
    "silver_orders",
    "silver_products",
    "silver_geolocation",
    "silver_customers",
    "silver_sellers",
    "silver_order_items",
    "silver_order_payments",
    "silver_order_reviews"
]

for table in silver_tables:
    df = spark.table(table)

    print(
        f"{table}: "
        f"{df.count():,} rows | "
        f"{len(df.columns)} columns"
    )

key_checks = {
    "silver_orders": ["order_id"],
    "silver_products": ["product_id"],
    "silver_geolocation": ["geolocation_zip_code_prefix"],
    "silver_customers": ["customer_id"],
    "silver_sellers": ["seller_id"],
    "silver_order_items": ["order_id", "order_item_id"],
    "silver_order_payments": ["order_id", "payment_sequential"],
    "silver_order_reviews": ["review_id", "order_id"]
}

for table, keys in key_checks.items():
    df = spark.table(table)

    total = df.count()
    distinct_keys = df.select(*keys).distinct().count()

    print(
        f"{table}: "
        f"rows={total:,}, "
        f"distinct_key={distinct_keys:,}, "
        f"unique={total == distinct_keys}"
    )

    print(
    "Products:",
    spark.table("bronze_products").count(),
    "→",
    spark.table("silver_products").count()
)

print(
    "Customers:",
    spark.table("bronze_customers").count(),
    "→",
    spark.table("silver_customers").count()
)

print(
    "Sellers:",
    spark.table("bronze_sellers").count(),
    "→",
    spark.table("silver_sellers").count()
)


## Referential Integrity

if run_mode in ("backfill", "incremental"):

    orders = spark.table("silver_orders").select("order_id")

    for table in [
        "silver_order_items",
        "silver_order_payments",
        "silver_order_reviews"
    ]:

        orphans = (
            spark.table(table)
            .join(orders, "order_id", "left_anti")
            .count()
        )

        print(f"{table}: {orphans} orphans")

        assert orphans == 0

    print("PASS: Silver validation")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


## 13 Batch Context

if run_mode == "backfill":
    snapshot_date = "2018-06-09"
elif run_mode == "incremental":
    snapshot_date = batch_id
else:
    snapshot_date = None

batch_context = spark.createDataFrame(
    [(run_mode, snapshot_date)],
    ["run_mode", "snapshot_date"]
).withColumn(
    "snapshot_date",
    F.to_date("snapshot_date")
)

(
    batch_context.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_batch_context")
)

batch_context.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
