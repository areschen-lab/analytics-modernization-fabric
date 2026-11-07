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

df = spark.sql("SELECT * FROM analytics_lakehouse.dbo.bronze_orders LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM analytics_lakehouse.dbo.bronze_order_items LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

df = spark.sql("SELECT * FROM analytics_lakehouse.dbo.bronze_order_payments LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM analytics_lakehouse.dbo.bronze_customers LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM analytics_lakehouse.dbo.bronze_products LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM analytics_lakehouse.dbo.bronze_sellers LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM analytics_lakehouse.dbo.bronze_category_translation LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM analytics_lakehouse.dbo.bronze_geolocation LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# category_translation
df = spark.table("bronze_category_translation")

print(
    "category name:", 
    df.count(),
    "distinct categories:",
    df.select("product_category_name").distinct().count()
    )

# geolocation
df = spark.table("bronze_geolocation")

print(
    "category name:", 
    df.count(),
    "distinct zip prefixes:",
    df.select("geolocation_zip_code_prefix").distinct().count()
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import datediff, col, when

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
        when(col("delay_days") > 0, 1).otherwise(0)
    )

)

display(
    silver_orders.select(
        "order_id",
        "order_status",
        "delivery_days",
        "delay_days",
        "is_late"
    ).limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import count, min, max

silver_orders.select(
    min("delivery_days").alias("min_delivery_days"),
    max("delivery_days").alias("max_delivery_days"),
    min("delay_days").alias("min_delay_days"),
    max("delay_days").alias("max_delay_days")
).show()

(
    
    silver_orders
        .groupBy("order_status")
        .agg(count("*").alias("order_count"))
        .orderBy("order_count", ascending=False)
        .show()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import sum as spark_sum, col

silver_orders.select(
    spark_sum(col("order_purchase_timestamp").isNull().cast("int")).alias("purchase_ts_nulls"),
    spark_sum(col("order_delivered_customer_date").isNull().cast("int")).alias("delivered_ts_nulls"),
    spark_sum(col("order_estimated_delivery_date").isNull().cast("int")).alias("estimated_ts_nulls")
).show()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    silver_orders.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_orders")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

products = spark.table("bronze_products").alias("p")
categories = spark.table("bronze_category_translation").alias("c")

silver_products = (
    products
    .join(
        categories,
#        on = "product_category_name",
#        products["product_category_name"] == categories["product_category_name"],
        col("p.product_category_name") == col("c.product_category_name"),
        how = "left"
    )
)

print(f"product counts = {products.count()}, categories counts = {categories.count()}, joined counts = {silver_products.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

unmatched = (
    silver_products
    .filter(col("c.product_category_name_english").isNull())
)

print("Unmatched products:", unmatched.count())

display(
    unmatched
    .select("p.product_category_name")
    .distinct()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import coalesce, lit

silver_products = (
    silver_products
    .withColumn(
        "product_category",
        coalesce(
            col("c.product_category_name_english"),
            col("c.product_category_name"),
            lit("unknown")
        )
    )
)

silver_products_final = (
    silver_products
    .drop(
        col("c.product_category_name_english"),
        col("c.product_category_name"),
        col("p.ingested_at")
    )
)

display(silver_products_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    silver_products_final.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("silver_products")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.table("silver_products"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

geo = spark.table("bronze_geolocation")

from pyspark.sql.functions import count, avg, min, max

geo_profile = (
    geo
    .groupBy("geolocation_zip_code_prefix")
    .agg(
        count("*").alias("point_count"),
        avg("geolocation_lat").alias("avg_lat"),
        avg("geolocation_lng").alias("avg_lng"),
        min("geolocation_lat").alias("min_lat"),
        max("geolocation_lat").alias("max_lat"),
        min("geolocation_lng").alias("min_lng"),
        max("geolocation_lng").alias("max_lng")
    )
)

display(geo_profile.orderBy("point_count", ascending = False).limit(50))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import countDistinct, col

geo_consistency = (
    geo
    .groupBy("geolocation_zip_code_prefix")
    .agg(
        countDistinct("geolocation_city").alias("city_count"),
        countDistinct("geolocation_state").alias("state_count")
    )
)

display(
    geo_consistency
    .filter(
        (col("city_count") > 1) |
        (col("state_count") > 1)
    )
    .orderBy("state_count", "city_count", ascending = False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc, count

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

# display(geo_location_counts.orderBy( "geolocation_zip_code_prefix", "location_count", ascending = [True, False]).limit(50))

display(
    geo_location_counts
    .orderBy(
        col("geolocation_zip_code_prefix").asc(), 
        col("location_count").desc()
    )
    .limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_spec = (
    Window
    .partitionBy("geolocation_zip_code_prefix")
    .orderBy(desc("location_count"))
)

geo_mode =  (
    geo_location_counts
    .withColumn(
        "rn",
        row_number().over(window_spec)
    )
    .filter(col("rn") == 1)
    .select(
        "geolocation_zip_code_prefix",
        col("geolocation_city").alias("city"),
        col("geolocation_state").alias("state"),
    )
)

display(
    geo_mode
    .orderBy(
        col("geolocation_zip_code_prefix").asc()
    )
)

print(f"bronze count: ", spark.table("bronze_geolocation").select("geolocation_zip_code_prefix").distinct().count())
print(f"silver count: ", geo_mode.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

geo_selected = (
    geo.alias("g")
    .join(
        geo_mode.alias("m"),
        (col("g.geolocation_zip_code_prefix") == col("m.geolocation_zip_code_prefix")) &
        (col("g.geolocation_state") == col("m.state")) &
        (col("g.geolocation_city") == col("m.city")),
        how = "inner"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import avg, min, max, count

geo_coords = (
    geo_selected
    .groupBy(
        "m.geolocation_zip_code_prefix",
        "m.city",
        "m.state"
    )
    .agg(
        count("*").alias("point_count"),
        avg("g.geolocation_lat").alias("latitude"),
        avg("g.geolocation_lng").alias("longitude"),
        min("g.geolocation_lat").alias("min_lat"),
        max("g.geolocation_lat").alias("max_lat"),
        min("g.geolocation_lng").alias("min_lng"),
        max("g.geolocation_lng").alias("max_lng")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_geolocation = (
    geo_coords
    .select(
        col("geolocation_zip_code_prefix"),
        col("city"),
        col("state"),
        col("latitude"),
        col("longitude")
    )
)

print(
    "bronze distinct zip:",
    geo.select("geolocation_zip_code_prefix").distinct().count()
)

print(
    "silver rows:",
    silver_geolocation.count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    silver_geolocation.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("silver_geolocation")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM analytics_lakehouse.dbo.silver_geolocation LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customers = spark.table("bronze_customers").alias("c")
geo = spark.table("silver_geolocation").alias("g")

silver_customers = (
    customers
    .join(
        geo,
        col("c.customer_zip_code_prefix") ==  col("g.geolocation_zip_code_prefix"),
        how = "left"     
    )
    .select(
        "c.*",
        col("g.city").alias("geo_city"),
        col("g.state").alias("geo_state"),
        col("g.latitude"),
        col("g.longitude")
    )
)

print("before: ", customers.count())
print("after: ", silver_customers.count())

display(silver_customers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

unmatched_customers = (
    silver_customers
    .filter(col("g.geolocation_zip_code_prefix").isNull())
)

print("unmatched customers: ", unmatched_customers.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mismatched = (
    silver_customers
    .filter(
        col("customer_state") != col("geo_state") 
    )
)

print("state mismatched: ", mismatched.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    silver_customers
    .filter(col("geo_state").isNull())
    .select(
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state"
    )
    .distinct()
    .orderBy("customer_zip_code_prefix")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

city_mismatch = (
    silver_customers
    .filter(
        col("geo_city").isNotNull() &
        (col("customer_city") != col("geo_city"))
    )
)

print("city mismatches:", city_mismatch.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    city_mismatch
    .select(
        "customer_zip_code_prefix",
        "customer_city",
        "geo_city",
        "customer_state",
        "geo_state"
    )
    .distinct()
    .limit(50)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    silver_customers.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_customers")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sellers = spark.table("bronze_sellers").alias("s")
geo = spark.table("silver_geolocation").alias("g")

silver_sellers = (
    sellers
    .join(
        geo,
        col("s.seller_zip_code_prefix") == col("g.geolocation_zip_code_prefix"),
        how="left"
    )
    .select(
        "s.*",
        col("g.city").alias("geo_city"),
        col("g.state").alias("geo_state"),
        col("g.latitude"),
        col("g.longitude")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    silver_sellers.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_sellers")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM analytics_lakehouse.dbo.bronze_order_items LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

order_items = spark.table("bronze_order_items")

silver_order_items = (
    order_items
    .withColumn(
        "item_total",
        col("price") + col("freight_value")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    silver_order_items.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_order_items")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

payments = spark.table("bronze_order_payments")

silver_order_payments = payments

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    silver_order_payments.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_order_payments")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

reviews = spark.table("bronze_order_reviews")

silver_order_reviews = reviews.drop("ingested_at")

(
    silver_order_reviews.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_order_reviews")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
