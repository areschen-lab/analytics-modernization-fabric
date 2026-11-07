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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 02 Orders Transformation

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

## 03 Products Transformation

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

## 04 Geolocation Transformation

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

## 05 Customers Transformation

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

## 06 Sellers Transformation

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

## 07 Order Items Transformation

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

## 08 Payments and Reviews Transformation

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

## 09 Validation

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
