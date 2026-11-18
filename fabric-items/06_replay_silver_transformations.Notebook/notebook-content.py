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

BRONZE_TABLE = "replay_bronze_order_events"
SILVER_TABLE = "replay_silver_orders"

def transform_batch(batch_id):

    as_of = "2018-06-09" if batch_id == "backfill" else batch_id

    if not spark.catalog.tableExists(SILVER_TABLE):
        assert batch_id == "backfill", "Initialize Backfill first"

    bronze = spark.table(BRONZE_TABLE)

    # 1. Identify orders affected by this batch
    changed_keys = (
        bronze.filter(F.col("batch_id") == batch_id)
        .select("order_id").distinct()
    )

    # 2. Read all arrived events for those orders,
    # excluding events from future batches
    history = (
        bronze.join(changed_keys, "order_id")
        .filter(
            (F.col("batch_id") == "backfill") |
            (F.to_date("batch_id") <= F.lit(as_of).cast("date"))
        )
    )

    # 3. Reconstruct the order state
    state = history.groupBy("order_id").agg(
        F.max(F.when(F.col("event_type") == "CREATED",
                     F.col("customer_id"))).alias("customer_id"),

        F.max(F.when(F.col("event_type") == "CREATED",
                     F.col("event_time"))).alias("order_purchase_timestamp"),

        F.max(F.when(F.col("event_type") == "CREATED",
                     F.col("estimated_delivery_date"))).alias(
                         "order_estimated_delivery_date"),

        F.max(F.when(F.col("event_type") == "APPROVED",
                     F.col("event_time"))).alias("order_approved_at"),

        F.max(F.when(F.col("event_type") == "SHIPPED",
                     F.col("event_time"))).alias("order_delivered_carrier_date"),

        F.max(F.when(F.col("event_type") == "DELIVERED",
                     F.col("event_time"))).alias("order_delivered_customer_date")
    )

    # No order should exist without a CREATED event
    assert state.filter(
        F.col("order_purchase_timestamp").isNull()
    ).count() == 0

    state = (
        state
        .withColumn(
            "order_status",
            F.when(F.col("order_delivered_customer_date").isNotNull(),
                   "delivered")
             .when(F.col("order_delivered_carrier_date").isNotNull(),
                   "shipped")
             .when(F.col("order_approved_at").isNotNull(),
                   "approved")
             .otherwise("created")
        )
        .withColumn(
            "delivery_days",
            F.datediff("order_delivered_customer_date",
                       "order_purchase_timestamp")
        )
        .withColumn(
            "delay_days",
            F.datediff("order_delivered_customer_date",
                       "order_estimated_delivery_date")
        )
        .withColumn(
            "is_late",
            F.when(F.col("order_delivered_customer_date").isNull(),
                   F.lit(None).cast("int"))
             .when(F.col("delay_days") > 0, F.lit(1))
             .otherwise(F.lit(0))
        )
        .withColumn("as_of_date", F.lit(as_of).cast("date"))
    )

    # 4. Initialize or MERGE into Silver
    if not spark.catalog.tableExists(SILVER_TABLE):
        state.write.format("delta").saveAsTable(SILVER_TABLE)
    else:
        (
            DeltaTable.forName(spark, SILVER_TABLE)
            .alias("t")
            .merge(state.alias("s"), "t.order_id = s.order_id")
            .whenMatchedUpdateAll(
                condition="s.as_of_date >= t.as_of_date"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    silver = spark.table(SILVER_TABLE)
    total = silver.count()

    assert silver.select("order_id").distinct().count() == total

    print(f"{batch_id}: Silver Orders = {total:,}")
    silver.groupBy("order_status").count().orderBy(
        "order_status"
    ).show()

    return total

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test 1: Historical Backfill
first = transform_batch("backfill")

# Test 2: Repeat Backfill
second = transform_batch("backfill")
assert first == second, "Backfill idempotency failed"

# Test 3: June 10 Replay
third = transform_batch("2018-06-10")

print("New orders:", third - second)

assert third - second == 193, "Unexpected new order count"

print("PASS: Silver Backfill, MERGE, idempotency, daily replay")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import functions as F

SOURCES = {
    "items": (
        "replay_bronze_order_items",
        "replay_silver_order_items",
        ["order_id", "order_item_id"]
    ),
    "payments": (
        "replay_bronze_order_payments",
        "replay_silver_order_payments",
        ["order_id", "payment_sequential"]
    ),
    "reviews": (
        "replay_bronze_order_reviews",
        "replay_silver_order_reviews",
        ["review_id", "order_id"]
    )
}

def prepare_silver(name, df):
    metadata = [
        F.col("event_id").alias("source_event_id"),
        F.col("batch_id"),
        F.col("arrival_time"),
        F.col("ingested_at")
    ]

    if name == "items":
        return (
            df.select(
                "order_id",
                F.col("order_item_id").cast("int").alias("order_item_id"),
                "product_id",
                "seller_id",
                F.col("shipping_limit_date").cast("timestamp").alias("shipping_limit_date"),
                F.col("price").cast("decimal(18,2)").alias("price"),
                F.col("freight_value").cast("decimal(18,2)").alias("freight_value"),
                *metadata
            )
            .withColumn("item_total", F.col("price") + F.col("freight_value"))
        )

    if name == "payments":
        return df.select(
            "order_id",
            F.col("payment_sequential").cast("int").alias("payment_sequential"),
            "payment_type",
            F.col("payment_installments").cast("int").alias("payment_installments"),
            F.col("payment_value").cast("decimal(18,2)").alias("payment_value"),
            *metadata
        )

    return df.select(
        "review_id",
        "order_id",
        F.col("review_score").cast("int").alias("review_score"),
        "review_comment_title",
        "review_comment_message",
        F.col("review_creation_date").cast("timestamp").alias("review_creation_date"),
        F.col("review_answer_timestamp").cast("timestamp").alias("review_answer_timestamp"),
        *metadata
    )


def ingest_silver(name, batch_id):
    bronze_table, target, keys = SOURCES[name]

    incoming = (
        spark.table(bronze_table)
        .filter(F.col("batch_id") == batch_id)
    )

    source_count = incoming.count()
    assert source_count > 0, f"Empty source: {name}/{batch_id}"

    incoming = prepare_silver(name, incoming)

    for key in keys:
        assert incoming.filter(F.col(key).isNull()).count() == 0

    assert incoming.select(*keys).distinct().count() == source_count

    if not spark.catalog.tableExists(target):
        (
            incoming.write
            .format("delta")
            .mode("errorifexists")
            .saveAsTable(target)
        )
    else:
        condition = " AND ".join(
            f"t.{key} = s.{key}" for key in keys
        )

        (
            DeltaTable.forName(spark, target)
            .alias("t")
            .merge(incoming.alias("s"), condition)
            .whenNotMatchedInsertAll()
            .execute()
        )

    silver = spark.table(target)

    assert silver.select(*keys).distinct().count() == silver.count()

    actual_batch = silver.filter(
        F.col("batch_id") == batch_id
    ).count()

    assert actual_batch == source_count, (
        f"{name}/{batch_id}: expected {source_count}, got {actual_batch}"
    )

    return silver.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run Backfill + June 10 + Idempotency
for name in SOURCES:
    ingest_silver(name, "backfill")
    first = ingest_silver(name, "2018-06-10")
    second = ingest_silver(name, "2018-06-10")

    assert first == second, f"Idempotency failed: {name}"

    print(f"{name}: Silver total={second:,}, idempotency=PASS")

print("PASS: All replay Silver sources")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql import functions as F

orders = (
    spark.table("replay_silver_orders")
    .select("order_id")
    .distinct()
)

children = {
    "Items": "replay_silver_order_items",
    "Payments": "replay_silver_order_payments",
    "Reviews": "replay_silver_order_reviews"
}

for name, table in children.items():
    df = spark.table(table)

    orphans = (
        df.join(orders, "order_id", "left_anti")
        .count()
    )

    print(f"{name}: {df.count():,} rows, {orphans} orphans")

    assert orphans == 0, f"{name} has orphan orders"

print("PASS: All Silver referential integrity checks")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
