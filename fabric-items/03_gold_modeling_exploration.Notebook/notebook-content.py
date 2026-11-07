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

customers = spark.table("silver_customers")

dim_customer = (
    customers
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
    .saveAsTable("dim_customer")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

products = spark.table("silver_products")

dim_product = (
    products
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
    .saveAsTable("dim_product")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sellers = spark.table("silver_sellers")

dim_seller = (
    sellers
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
    .saveAsTable("dim_seller")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

items = spark.table("silver_order_items").alias("i")
orders = spark.table("silver_orders").alias("o")

fact_order_items = (
    items
    .join(
        orders,
        col("i.order_id") == col("o.order_id"),
        how="left"
    )
    .select(
        col("i.order_id"),
        col("i.order_item_id"),
        col("i.product_id"),
        col("i.seller_id"),

        col("o.customer_id"),
        col("o.order_status"),
        col("o.order_purchase_timestamp"),
        col("o.order_delivered_customer_date"),
        col("o.order_estimated_delivery_date"),
        col("o.delivery_days"),
        col("o.delay_days"),
        col("o.is_late"),

        col("i.price"),
        col("i.freight_value"),
        col("i.item_total")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("items:", items.count())
print("fact:", fact_order_items.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    fact_order_items.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("fact_order_items")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (
    min,
    max,
    to_date,
    sequence,
    explode,
    year,
    month,
    quarter,
    dayofmonth,
    dayofweek,
    date_format,
    col
)

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
    .withColumn("month_name", date_format("date", "MMMM"))
    .withColumn("day", dayofmonth("date"))
    .withColumn("day_of_week", dayofweek("date"))
    .withColumn("day_name", date_format("date", "EEEE"))
)

display(
    dim_date
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    dim_date.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("dim_date")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.table("silver_order_payments").filter(col("order_id") == "487c1451b8fd7347d0e80e5aca887e91"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import sum as spark_sum, count

payments = spark.table("silver_order_payments")

gold_order_payments = (
    payments
    .groupBy("order_id")
    .agg(
        spark_sum("payment_value").alias("payment_total"),
        count("*").alias("payment_count")
    )
)

display(gold_order_payments.filter(col("order_id") == "487c1451b8fd7347d0e80e5aca887e91"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM analytics_lakehouse.dbo.fact_order_items LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM analytics_lakehouse.dbo.silver_orders LIMIT 1000")
display(df)

print(f"count distinct order id: ", spark.table("silver_orders").select("order_id").distinct().count())
print(f"total rows: ", spark.table("silver_orders").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import to_date

orders = spark.table("silver_orders")

orders = (
    orders
    .withColumn(
        "order_purchase_date",
        to_date("order_purchase_timestamp")
    )
    .alias("o")
)

payments = gold_order_payments.alias("p")

fact_orders = (
    orders
    .join(
        payments,
        on = "order_id",
        how = "left"
    )
    .select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_purchase_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "delivery_days",
        "delay_days",
        "is_late",
        "payment_total",
        "payment_count"
    )
)

print("silver orders:", spark.table("silver_orders").count())
print("fact orders:", fact_orders.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

df = spark.sql("SELECT * FROM analytics_lakehouse.dbo.silver_order_reviews LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

review_order_count = spark.table("silver_order_reviews").select("order_id").distinct().count()
review_review_count = spark.table("silver_order_reviews").select("review_id").distinct().count()
review_rows_count = spark.table("silver_order_reviews").count()

print(f"order count:", review_order_count)
print(f"review count:", review_review_count)
print(f"rows count:", review_rows_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

reviews = spark.table("silver_order_reviews")

review_counts = (
    reviews
    .groupBy("order_id")
    .agg(
        count("*").alias("review_count")
    )
)

display(
    review_counts
    .orderBy(col("review_count").desc())
    .limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.table("silver_order_reviews").filter(col("order_id") == "df56136b8031ecd28e200bb18e6ddb2e"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

review_counts = (
    reviews
    .groupBy("review_id")
    .agg(
        count("*").alias("order_count")
    )
)

display(
    review_counts
    .orderBy(col("order_count").desc())
    .limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.table("silver_order_reviews").filter(col("review_id") == "4d0e6dd087008d1f992d25ef6e1f619f"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

review_window = (
    Window
    .partitionBy("order_id")
    .orderBy(col("review_answer_timestamp").asc())
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

display(gold_order_review.filter(col("order_id") == "df56136b8031ecd28e200bb18e6ddb2e"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

review_order_count = gold_order_reviews.select("order_id").distinct().count()
review_review_count = gold_order_reviews.select("review_id").distinct().count()
review_rows_count = gold_order_reviews.count()

print(f"order count:", review_order_count)
print(f"review count:", review_review_count)
print(f"rows count:", review_rows_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "total rows:",
    gold_order_reviews.count()
)

print(
    "distinct order ids:",
    gold_order_reviews
        .select("order_id")
        .distinct()
        .count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "null order_id:",
    gold_order_reviews
        .filter(col("order_id").isNull())
        .count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_orders_current = spark.table("fact_orders").alias("f")
reviews = gold_order_reviews.alias("r")

fact_orders_enriched = (
    fact_orders_current
    .join(
        reviews,
        on="order_id",
        how="left"
    )
    .select(
        "f.*",
        col("r.review_id"),
        col("r.review_score"),
        col("r.review_comment_title"),
        col("r.review_comment_message"),
        col("r.review_creation_date"),
        col("r.review_answer_timestamp")
    )
)

print("before:", fact_orders_current.count())
print("after:", fact_orders_enriched.count())

print(
    "orders without review:",
    fact_orders_enriched
        .filter(col("review_id").isNull())
        .count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    fact_orders_enriched.write
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

# fact_orders key
print(
    "fact_orders:",
    spark.table("fact_orders").count(),
    spark.table("fact_orders").select("order_id").distinct().count()
)

# fact_order_items composite key
items = spark.table("fact_order_items")

print(
    "fact_order_items:",
    items.count(),
    items.select("order_id", "order_item_id").distinct().count()
)

# dimension keys
for table, key in [
    ("dim_customer", "customer_id"),
    ("dim_product", "product_id"),
    ("dim_seller", "seller_id")
]:
    df = spark.table(table)
    print(
        table,
        df.count(),
        df.select(key).distinct().count()
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

checks = [
    ("fact_orders", ["order_id"]),
    ("fact_order_items", ["order_id", "order_item_id"]),
    ("dim_customer", ["customer_id"]),
    ("dim_product", ["product_id"]),
    ("dim_seller", ["seller_id"]),
    ("dim_date", ["date"])
]

for table, keys in checks:
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

print("orphan customer keys:", orphan_customers.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

print("orphan products:", orphan_products.count())
print("orphan sellers:", orphan_sellers.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import sum as spark_sum

item_totals = (
    spark.table("fact_order_items")
    .agg(
        spark_sum("price").alias("total_price"),
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

# CELL ********************

spark.sql("""
SELECT
    COUNT(*) AS orders,
    COUNT(DISTINCT customer_id) AS customers,
    AVG(delivery_days) AS avg_delivery_days,
    AVG(CASE WHEN is_late = 1 THEN 1.0 ELSE 0.0 END) AS late_rate,
    AVG(review_score) AS avg_review_score
FROM fact_orders
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

fact_orders = spark.table("fact_orders").alias("f")
customers = spark.table("dim_customer").alias("c")

fact_orders_updated = (
    fact_orders
    .join(
        customers,
        col("f.customer_id") == col("c.customer_id"),
        how="left"
    )
    .select(
        "f.*",
        col("c.customer_unique_id")
    )
)

print("before:", fact_orders.count())
print("after:", fact_orders_updated.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    fact_orders_updated.write
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

# Gold validation - date relationship checks

from pyspark.sql.functions import col

dim_date = spark.table("dim_date")
fact_orders = spark.table("fact_orders")

print(
    "null dim dates:",
    dim_date.filter(col("date").isNull()).count()
)

print(
    "null purchase dates:",
    fact_orders.filter(col("order_purchase_date").isNull()).count()
)

unmatched_dates = (
    fact_orders
    .filter(col("order_purchase_date").isNotNull())
    .join(
        dim_date,
        fact_orders["order_purchase_date"] == dim_date["date"],
        how="left_anti"
    )
)

print("unmatched fact rows:", unmatched_dates.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
