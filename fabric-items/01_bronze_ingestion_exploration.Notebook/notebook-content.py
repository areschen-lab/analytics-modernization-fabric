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

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import current_timestamp

df_orders = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("Files/raw/olist/olist_orders_dataset.csv")
)

df_orders = df_orders.withColumn("ingested_at", current_timestamp())

display(df_orders.limit(10))
df_orders.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_orders.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("bronze_orders")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SHOW TABLES").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Row count:", spark.table("bronze_orders").count())

spark.table("bronze_orders").printSchema()

spark.sql("""
SELECT 
    order_status,
    COUNT(*) AS order_count
FROM bronze_orders
GROUP BY order_status
ORDER BY order_status DESC
""").show()

spark.sql("""
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS distinct_order_ids
FROM bronze_orders
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import current_timestamp

tables = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv"
}

for table_name, file_name in tables.items():
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"Files/raw/olist/{file_name}")
        .withColumn("ingested_at", current_timestamp())
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"bronze_{table_name}")
    )

print(f"Loaded bronze_{table_name}:{df.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw = spark.read.text("Files/raw/olist/olist_order_reviews_dataset.csv")
display(raw.limit(100))

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("Files/raw/olist/olist_order_reviews_dataset.csv")
)

print("Rows:", df.count())

df.printSchema()

from pyspark.sql.functions import col, sum as spark_sum

df.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
]).show(truncate=False)

display(
    df.filter(
        col("review_id").isNull()
        | col("order_id").isNull()
        | col("review_score").isNull()
    ).limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

from pyspark.sql.functions import current_timestamp

df_reviews = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .csv("Files/raw/olist/olist_order_reviews_dataset.csv")
    .withColumn("ingested_at", current_timestamp())
)

print("Rows: ", df_reviews.count())
df_reviews.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    df_reviews.filter(
        col("review_id").isNull()
        | col("order_id").isNull()
        | col("review_score").isNull()
    ).limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_reviews.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("bronze_order_reviews")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_reviews.printSchema()
spark.table("bronze_order_reviews").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_reviews.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("bronze_order_reviews")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.table("bronze_order_reviews").limit(50))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

# CELL ********************

checks = {
    "bronze_customers": ["customer_id"],
    "bronze_orders": ["order_id"],
    "bronze_order_items": ["order_id", "order_item_id"],
    "bronze_order_payments": ["order_id", "payment_sequential"],
    "bronze_order_reviews": ["review_id"],
    "bronze_products": ["product_id"],
    "bronze_sellers": ["seller_id"]
}

for table, keys in checks.items():
    df = spark.table(table)

    total = df.count()
    distinct_keys = df.select(*keys).distinct().count()

    print(
        f"{table}: total={total:,},"
        f"distinct_keys={distinct_keys:,}, "
        f"unique={total == distinct_keys}"
    )



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM analytics_lakehouse.dbo.bronze_order_reviews LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, count

duplicate_reviews = (
    spark.table("bronze_order_reviews")
    .groupBy("review_id")
    .agg(count("*").alias("row_count"))
    .filter(col("row_count") > 1)
    .orderBy(col("row_count").desc())
)

display(duplicate_reviews)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.table("bronze_order_reviews")
    .filter(col("review_id") == "0c76e7a547a531e7bf9f0b99cba071c1")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.table("bronze_order_reviews")

total = df.count()

distinct_keys = df.select("review_id", "order_id").distinct().count()

print(f"total={total:,}, composite key={distinct_keys:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
