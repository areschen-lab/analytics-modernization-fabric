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

from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    min,
    max,
    to_date,
    trunc,
    sequence,
    explode,
    year,
    month,
    quarter,
    dayofmonth,
    dayofweek,
    date_format,
    row_number
)

from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 02 Dimensions

dim_customer = (
    spark.table("silver_customers")
    .select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "geo_city",
        "geo_state",
        "latitude",
        "longitude"
    )
)

(
    dim_customer.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dim_customer")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_product = (
    spark.table("silver_products")
    .select(
        "product_id",
        "product_category",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    )
)

(
    dim_product.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dim_product")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_seller = (
    spark.table("silver_sellers")
    .select(
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
        "geo_city",
        "geo_state",
        "latitude",
        "longitude"
    )
)

(
    dim_seller.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dim_seller")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 03 Date Dimension

orders = spark.table("silver_orders")

date_range = (
    orders
    .select(
        min(to_date("order_purchase_timestamp")).alias("min_date"),
        max(to_date("order_purchase_timestamp")).alias("max_date")
    )
)

dates = (
    date_range
    .select(
        explode(
            sequence(
                col("min_date"),
                col("max_date")
            )
        ).alias("date")
    )
)

dim_date = (
    dates
    .withColumn("year", year("date"))
    .withColumn("quarter", quarter("date"))
    .withColumn("month", month("date"))
    .withColumn("month_start", trunc(col("date"), "month"))
    .withColumn("month_name", date_format("date", "MMMM"))
    .withColumn("day", dayofmonth("date"))
    .withColumn("day_of_week", dayofweek("date"))
    .withColumn("day_name", date_format("date", "EEEE"))
)

(
    dim_date.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dim_date")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 04 Payment Aggregation

gold_order_payments = (
    spark.table("silver_order_payments")
    .groupBy("order_id")
    .agg(
        spark_sum("payment_value").alias("payment_total"),
        count("*").alias("payment_count")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 05 Review Canonicalization

review_window = (
    Window
    .partitionBy("order_id")
    .orderBy(
        col("review_answer_timestamp").asc_nulls_last(),
        col("review_creation_date").asc_nulls_last(),
        col("review_id").asc()
    )
)

gold_order_reviews = (
    spark.table("silver_order_reviews")
    .withColumn(
        "rn",
        row_number().over(review_window)
    )
    .filter(col("rn") == 1)
    .drop("rn")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 06 Fact Orders

orders = (
    spark.table("silver_orders")
    .withColumn(
        "order_purchase_date",
        to_date("order_purchase_timestamp")
    )
    .alias("o")
)

payments = gold_order_payments.alias("p")
reviews = gold_order_reviews.alias("r")
customers = dim_customer.alias("c")

fact_orders = (
    orders
    .join(
        payments,
        on="order_id",
        how="left"
    )
    .join(
        reviews,
        on="order_id",
        how="left"
    )
    .join(
        customers,
        col("o.customer_id") == col("c.customer_id"),
        how="left"
    )
    .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("c.customer_unique_id"),

        col("o.order_status"),
        col("o.order_purchase_timestamp"),
        col("o.order_purchase_date"),
        col("o.order_delivered_customer_date"),
        col("o.order_estimated_delivery_date"),
        col("o.delivery_days"),
        col("o.delay_days"),
        col("o.is_late"),

        col("payment_total"),
        col("payment_count"),

        col("review_id"),
        col("review_score"),
        col("review_comment_title"),
        col("review_comment_message"),
        col("review_creation_date"),
        col("review_answer_timestamp")
    )
)

(
    fact_orders.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("fact_orders")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 07 Fact Order Items

fact_order_items = (
    spark.table("silver_order_items")
    .select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "price",
        "freight_value",
        "item_total"
    )
)

(
    fact_order_items.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("fact_order_items")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## 08 Validation

key_checks = {
    "fact_orders": ["order_id"],
    "fact_order_items": ["order_id", "order_item_id"],
    "dim_customer": ["customer_id"],
    "dim_product": ["product_id"],
    "dim_seller": ["seller_id"],
    "dim_date": ["date"]
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orphan_customers = (
    spark.table("fact_orders").alias("f")
    .join(
        spark.table("dim_customer").alias("d"),
        col("f.customer_id") == col("d.customer_id"),
        how="left_anti"
    )
)

orphan_products = (
    spark.table("fact_order_items").alias("f")
    .join(
        spark.table("dim_product").alias("d"),
        col("f.product_id") == col("d.product_id"),
        how="left_anti"
    )
)

orphan_sellers = (
    spark.table("fact_order_items").alias("f")
    .join(
        spark.table("dim_seller").alias("d"),
        col("f.seller_id") == col("d.seller_id"),
        how="left_anti"
    )
)

print("orphan customers:", orphan_customers.count())
print("orphan products:", orphan_products.count())
print("orphan sellers:", orphan_sellers.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_orders_check = spark.table("fact_orders")
dim_date_check = spark.table("dim_date")

unmatched_dates = (
    fact_orders_check
    .filter(col("order_purchase_date").isNotNull())
    .join(
        dim_date_check,
        fact_orders_check["order_purchase_date"] == dim_date_check["date"],
        how="left_anti"
    )
)

print(
    "null purchase dates:",
    fact_orders_check
    .filter(col("order_purchase_date").isNull())
    .count()
)

print("unmatched dates:", unmatched_dates.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

item_totals = (
    spark.table("fact_order_items")
    .agg(
        spark_sum("price").alias("total_product_value"),
        spark_sum("freight_value").alias("total_freight"),
        spark_sum("item_total").alias("total_item_value")
    )
)

payment_totals = (
    spark.table("fact_orders")
    .agg(
        spark_sum("payment_total").alias("total_payment_value")
    )
)

display(item_totals)
display(payment_totals)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
