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

# MARKDOWN ********************


# CELL ********************

from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Configuration
spark.conf.set("spark.sql.session.timeZone", "UTC")

START = "2018-06-10"
END_EXCLUSIVE = "2018-06-18"

SOURCE_PATH = "Files/olist_adls/olist_orders_dataset.csv"
LANDING_PATH = "Files/replay/landing/order_events_v1"
EXCEPTION_PATH = "Files/replay/audit/order_event_exceptions_v1"

start_ts = F.to_timestamp(F.lit(START))
end_ts = F.to_timestamp(F.lit(END_EXCLUSIVE))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# 2. Read original CSV, not existing Bronze
schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", StringType()),
    StructField("order_approved_at", StringType()),
    StructField("order_delivered_carrier_date", StringType()),
    StructField("order_delivered_customer_date", StringType()),
    StructField("order_estimated_delivery_date", StringType())
])

raw = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv(SOURCE_PATH)
)

ts_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

orders = raw

for name in ts_columns:
    orders = orders.withColumn(
        name,
        F.to_timestamp(F.col(name), "yyyy-MM-dd HH:mm:ss")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Validate source grain
source_count = orders.count()
unique_count = orders.select("order_id").distinct().count()

assert source_count == unique_count, "Duplicate source order_id"
assert orders.filter(F.col("order_id").isNull()).count() == 0

print("Source orders:", source_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. Generate inferred lifecycle events
event_mapping = [
    ("CREATED", "order_purchase_timestamp", 1),
    ("APPROVED", "order_approved_at", 2),
    ("SHIPPED", "order_delivered_carrier_date", 3),
    ("DELIVERED", "order_delivered_customer_date", 4)
]

parts = []

for event_type, timestamp_col, rank in event_mapping:
    part = orders.select(
        "order_id",
        "customer_id",
        "order_purchase_timestamp",
        F.col(timestamp_col).alias("event_time"),
        F.when(
            F.lit(event_type) == "CREATED",
            F.col("order_estimated_delivery_date")
        ).alias("estimated_delivery_date"),
        F.lit(event_type).alias("event_type"),
        F.lit(rank).alias("event_rank")
    )
    parts.append(part)

candidates = reduce(
    lambda a, b: a.unionByName(b),
    parts
).filter(
    F.col("event_time").isNotNull()
    & (F.col("event_time") < end_ts)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. Detect inconsistent historical records
classified = candidates.withColumn(
    "exception_reason",
    F.when(
        F.col("order_purchase_timestamp").isNull()
        | (F.col("order_purchase_timestamp") >= end_ts),
        "NO_CREATED_EVENT_BY_CUTOFF"
    ).when(
        F.col("event_time") < F.col("order_purchase_timestamp"),
        "EVENT_BEFORE_ORDER_CREATED"
    )
)

exceptions = classified.filter(
    F.col("exception_reason").isNotNull()
)

valid = classified.filter(
    F.col("exception_reason").isNull()
).drop("exception_reason")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 6. Assign replay batches and simulated arrival time
events = (
    valid
    .withColumn(
        "batch_id",
        F.when(
            F.col("event_time") < start_ts,
            "backfill"
        ).otherwise(
            F.date_format("event_time", "yyyy-MM-dd")
        )
    )
    .withColumn(
        "arrival_time",
        F.when(
            F.col("batch_id") == "backfill",
            F.to_timestamp(F.lit("2018-06-09 23:59:59"))
        ).otherwise(F.col("event_time"))
    )
    .withColumn(
        "event_id",
        F.sha2(
            F.concat_ws("|", "order_id", "event_type"),
            256
        )
    )
    .withColumn("event_source", F.lit("olist_inferred"))
    .withColumn(
        "arrival_rule",
        F.when(
            F.col("batch_id") == "backfill",
            "historical_backfill"
        ).otherwise("simulated_same_day")
    )
    .drop("order_purchase_timestamp")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 7. Validate events before writing
duplicate_ids = (
    events.groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

assert duplicate_ids == 0, "Duplicate event_id"

assert events.filter(
    F.col("event_type") == "CREATED"
).select("order_id").distinct().count() == (
    events.select("order_id").distinct().count()
), "Some orders have no CREATED event"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 8. Replace only isolated replay datasets
(
    events.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("batch_id")
    .save(LANDING_PATH)
)

(
    exceptions.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(EXCEPTION_PATH)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 9. Validate persisted results
saved = spark.read.format("delta").load(LANDING_PATH)
audit = spark.read.format("delta").load(EXCEPTION_PATH)

print("Total events:", saved.count())
print(
    "Unique orders:",
    saved.select("order_id").distinct().count()
)
print("Excluded events:", audit.count())

saved.groupBy("batch_id", "event_type") \
    .count() \
    .orderBy("batch_id", "event_type") \
    .show(100, truncate=False)

audit.groupBy("exception_reason").count().show()

print("Landing:", LANDING_PATH)
print("Exceptions:", EXCEPTION_PATH)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
