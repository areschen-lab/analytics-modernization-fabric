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

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

LANDING_PATH = "Files/replay/landing/order_events_v1"
TARGET_TABLE = "replay_bronze_order_events"

def ingest_batch(batch_id):
    # Read only the specified arrival batch
    incoming = (
        spark.read.format("delta")
        .load(LANDING_PATH)
        .filter(F.col("batch_id") == batch_id)
        .withColumn("ingested_at", F.current_timestamp())
    )

    incoming_count = incoming.count()
    assert incoming_count > 0, f"Empty batch: {batch_id}"
    assert incoming.filter(F.col("event_id").isNull()).count() == 0
    assert incoming.select("event_id").distinct().count() == incoming_count

    if not spark.catalog.tableExists(TARGET_TABLE):
        incoming.write.format("delta").saveAsTable(TARGET_TABLE)
    else:
        (
            DeltaTable.forName(spark, TARGET_TABLE)
            .alias("t")
            .merge(incoming.alias("s"), "t.event_id = s.event_id")
            .whenNotMatchedInsertAll()
            .execute()
        )

    bronze = spark.table(TARGET_TABLE)
    total = bronze.count()

    assert bronze.filter(
        F.col("batch_id") == batch_id
    ).count() == incoming_count

    print(f"{batch_id}: incoming={incoming_count:,}, bronze_total={total:,}")
    return total

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Test 1: Historical Backfill
first = ingest_batch("backfill")

# Test 2: Repeat Backfill
second = ingest_batch("backfill")
assert first == second, "Backfill idempotency failed"

# Test 3: June 10 Replay
third = ingest_batch("2018-06-10")

# Validate persisted batch counts
bronze = spark.table(TARGET_TABLE)

expected_batches = {
    "backfill": 321841,
    "2018-06-10": 355
}

for batch_id, expected in expected_batches.items():
    actual = bronze.filter(
        F.col("batch_id") == batch_id
    ).count()

    assert actual == expected, (
        f"{batch_id}: expected {expected}, got {actual}"
    )

# Validate uniqueness
total = bronze.count()
unique = bronze.select("event_id").distinct().count()

assert total == unique, "Duplicate event IDs"

# Validate cumulative total
assert total == 322196, f"Unexpected total: {total}"

# Re-run same batch and ensure nothing changes
before = bronze.count()
ingest_batch("2018-06-10")
after = spark.table(TARGET_TABLE).count()

assert before == after, "Idempotency failed"

print("PASS: Backfill, daily ingestion, uniqueness, idempotency")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SOURCE = "Files/olist_adls/olist_order_items_dataset.csv"
OUTPUT = "Files/replay/landing/order_items_v1"

# Read original CSV
items = (
    spark.read
    .option("header", True)
    .csv(SOURCE)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Use valid CREATED events to determine item arrival
created = (
    spark.read.format("delta")
    .load("Files/replay/landing/order_events_v1")
    .filter(F.col("event_type") == "CREATED")
    .select(
        "order_id",
        "batch_id",
        F.col("event_time").alias("order_created_at"),
        "arrival_time"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Each item arrives with its parent order
replay_items = (
    items.join(created, "order_id", "inner")
    .withColumn(
        "event_id",
        F.sha2(
            F.concat_ws(
                "|",
                F.lit("ITEM"),
                F.col("order_id"),
                F.col("order_item_id")
            ),
            256
        )
    )
    .withColumn(
        "arrival_rule",
        F.lit("simulated_at_order_creation")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate grain before writing
total = replay_items.count()

unique_items = (
    replay_items
    .select("order_id", "order_item_id")
    .distinct()
    .count()
)

assert total == unique_items, "Duplicate order items"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write isolated Landing
(
    replay_items.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("batch_id")
    .save(OUTPUT)
)

print("Replay Order Items:", total)

replay_items.groupBy("batch_id").count() \
    .orderBy("batch_id") \
    .show(20, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

START = "2018-06-10"
END = "2018-06-18"
RAW = "Files/olist_adls"
LANDING = "Files/replay/landing"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Use only orders with a valid CREATED event
events = spark.read.format("delta").load(
    f"{LANDING}/order_events_v1"
)

created = (
    events.filter(F.col("event_type") == "CREATED")
    .select(
        "order_id",
        F.col("event_time").alias("created_at")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Historical approval timestamp
order_times = (
    spark.read.option("header", True)
    .csv(f"{RAW}/olist_orders_dataset.csv")
    .select(
        "order_id",
        F.to_timestamp("order_approved_at").alias("approved_at")
    )
)

timeline = created.join(order_times, "order_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --------------------------------
# PAYMENT LANDING
# --------------------------------
payments = (
    spark.read.option("header", True)
    .csv(f"{RAW}/olist_order_payments_dataset.csv")
    .join(timeline, "order_id")
    .withColumn(
        "payment_value",
        F.col("payment_value").cast("decimal(18,2)")
    )
    .withColumn(
        "payment_sequential",
        F.col("payment_sequential").cast("int")
    )
    .withColumn(
        "payment_installments",
        F.col("payment_installments").cast("int")
    )
    .withColumn(
        "base_arrival",
        F.when(
            F.col("approved_at").isNotNull(),
            F.greatest("approved_at", "created_at")
        ).otherwise(F.expr("created_at + INTERVAL 1 DAY"))
    )
)

payments = (
    payments
    .withColumn(
        "simulated_delay",
        F.pmod(
            F.crc32(F.concat_ws(
                "|", "order_id", "payment_sequential"
            )),
            F.lit(10)
        ) == 0
    )
    .withColumn(
        "arrival_time",
        F.when(
            F.col("simulated_delay"),
            F.expr("base_arrival + INTERVAL 2 DAYS")
        ).otherwise(F.col("base_arrival"))
    )
    .filter(F.col("arrival_time") < F.to_timestamp(F.lit(END)))
    .withColumn(
        "batch_id",
        F.when(
            F.col("arrival_time") < F.to_timestamp(F.lit(START)),
            "backfill"
        ).otherwise(F.date_format("arrival_time", "yyyy-MM-dd"))
    )
    .withColumn(
        "event_id",
        F.sha2(F.concat_ws(
            "|", F.lit("PAYMENT"),
            "order_id", "payment_sequential"
        ), 256)
    )
    .withColumn(
        "arrival_rule",
        F.when(
            F.col("approved_at").isNull(),
            "simulated_created_plus_1d"
        ).otherwise("simulated_at_approval")
    )
    .drop("base_arrival", "created_at", "approved_at")
)

assert payments.count() == payments.select(
    "event_id"
).distinct().count()

payments.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("batch_id") \
    .save(f"{LANDING}/payments_v1")

print("Payments:", payments.count())
payments.groupBy("batch_id").count() \
    .orderBy("batch_id").show(20, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --------------------------------
# REVIEW LANDING
# --------------------------------
reviews = (
    spark.read.option("header", True)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .csv(f"{RAW}/olist_order_reviews_dataset.csv")
    .join(created, "order_id")
    .withColumn(
        "event_time",
        F.to_timestamp("review_answer_timestamp")
    )
    .filter(
        F.col("event_time").isNotNull()
        & (F.col("event_time") >= F.col("created_at"))
    )
    .withColumn(
        "simulated_delay",
        F.pmod(
            F.crc32(F.concat_ws(
                "|", "review_id", "order_id"
            )),
            F.lit(10)
        ) == 0
    )
    .withColumn(
        "arrival_time",
        F.when(
            F.col("simulated_delay"),
            F.expr("event_time + INTERVAL 2 DAYS")
        ).otherwise(F.col("event_time"))
    )
    .filter(F.col("arrival_time") < F.to_timestamp(F.lit(END)))
    .withColumn(
        "batch_id",
        F.when(
            F.col("arrival_time") < F.to_timestamp(F.lit(START)),
            "backfill"
        ).otherwise(F.date_format("arrival_time", "yyyy-MM-dd"))
    )
    .withColumn(
        "event_id",
        F.sha2(F.concat_ws(
            "|", F.lit("REVIEW"),
            "review_id", "order_id"
        ), 256)
    )
    .withColumn(
        "arrival_rule",
        F.lit("answer_timestamp_with_simulated_delay")
    )
    .drop("created_at")
)

assert reviews.count() == reviews.select(
    "event_id"
).distinct().count()

reviews.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("batch_id") \
    .save(f"{LANDING}/reviews_v1")

print("Reviews:", reviews.count())
reviews.groupBy("batch_id").count() \
    .orderBy("batch_id").show(20, False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Three additional replay sources
SOURCES = {
    "items": (
        "Files/replay/landing/order_items_v1",
        "replay_bronze_order_items"
    ),
    "payments": (
        "Files/replay/landing/payments_v1",
        "replay_bronze_order_payments"
    ),
    "reviews": (
        "Files/replay/landing/reviews_v1",
        "replay_bronze_order_reviews"
    )
}

def ingest_source(name, batch_id):
    path, target = SOURCES[name]

    incoming = (
        spark.read.format("delta")
        .load(path)
        .filter(F.col("batch_id") == batch_id)
    )

    count = incoming.count()
    assert count > 0, f"Empty batch: {name}/{batch_id}"

    assert incoming.filter(
        F.col("event_id").isNull()
    ).count() == 0

    assert incoming.select(
        "event_id"
    ).distinct().count() == count

    incoming = incoming.withColumn(
        "ingested_at", F.current_timestamp()
    )

    if not spark.catalog.tableExists(target):
        (
            incoming.write
            .format("delta")
            .mode("errorifexists")
            .saveAsTable(target)
        )
    else:
        (
            DeltaTable.forName(spark, target)
            .alias("t")
            .merge(
                incoming.alias("s"),
                "t.event_id = s.event_id"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    bronze = spark.table(target)

    actual_batch = bronze.filter(
        F.col("batch_id") == batch_id
    ).count()

    assert actual_batch == count, (
        f"{name}/{batch_id}: expected {count}, "
        f"got {actual_batch}"
    )

    assert bronze.count() == bronze.select(
        "event_id"
    ).distinct().count(), f"Duplicate events: {name}"

    return bronze.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test all three sources
for name in SOURCES:

    backfill = ingest_source(name, "backfill")

    day1 = ingest_source(name, "2018-06-10")

    # Re-run June 10 to verify idempotency
    repeated = ingest_source(name, "2018-06-10")

    assert day1 == repeated, (
        f"Idempotency failed: {name}"
    )

    print(
        f"{name}: total={repeated:,}, "
        f"idempotency=PASS"
    )

print("PASS: All replay Bronze sources")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
