from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, row_number,sqrt, pow,expr,
    sum as _sum, avg as _avg, to_json, struct, lit, count, collect_list
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, IntegerType, MapType, TimestampType
)
from pyspark.sql.window import Window

from confluent_kafka import Producer
import json

import time

from pymongo import MongoClient
import pymongo




BASE_PATH_TO_CHECKPOINTING = "./checkpointing"

spark = SparkSession \
    .builder \
    .appName("Streaming from Kafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1"
        ])
    ) \
    .config("spark.mongodb.read.connection.uri",  "mongodb://localhost:27017/transaction_analysis") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/transaction_analysis") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()



schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("customer_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_category", StringType()),
    StructField("payment_method", StringType()),
    StructField("amount", LongType()),
    StructField("status", StringType()),
    StructField("commission_type", StringType()),
    StructField("commission_amount", LongType()),
    StructField("vat_amount", LongType()),
    StructField("total_amount", LongType()),
    StructField("customer_type", StringType()),
    StructField("risk_level", IntegerType()),
    StructField("failure_reason", StringType()),
    StructField("location", MapType(StringType(), DoubleType())),
    StructField("device_info", MapType(StringType(), StringType()))
])


# print("hiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "darooghe.transactions") \
    .option("failOnDataLoss", "false")\
    .option("startingOffsets", "earliest") \
    .load()
# print("byeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
json_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
)



flattened = json_df.select(
    col("data.transaction_id").alias("tx_id"),
    to_timestamp(col("data.timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("event_time"),
    col("data.customer_id"),
    col("data.merchant_id"),
    col("data.merchant_category"),
    col("data.payment_method"),
    col("data.amount"),
    col("data.commission_amount"),
    col("data.vat_amount"),
    col("data.total_amount"),
    col("data.commission_type"),
    col("data.customer_type"),
    col("data.risk_level"),
    col("data.failure_reason"),
    col("data.status"),
    col("data.location").getItem("lat").alias("lat"),
    col("data.location").getItem("lng").alias("lng"),
    col("data.device_info").getItem("os").alias("device_os"),
    col("data.device_info").getItem("app_version").alias("device_app_version"),
    col("data.device_info").getItem("device_model").alias("device_model")
    
)


flattened.printSchema()


commission_by_type = flattened.withWatermark("event_time", "10 seconds").groupBy(
    window(col("event_time"), "1 minute"),
    col("commission_type")
).agg(
    _sum("commission_amount").alias("total_commission")
)

def write_commission_by_type(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    pdf = batch_df.toPandas()
    p = Producer({"bootstrap.servers": "localhost:9092"})

    for _, row in pdf.iterrows():
        payload = {
            "commission_type": row["commission_type"],
            "total_commission": row["total_commission"],
            "window": str(row["window"]),
        }
        p.produce(
            topic="darooghe.commission_by_type",
            key=str(row["commission_type"]),
            value=json.dumps(payload)
        )

    p.flush()

commission_by_type.writeStream \
    .foreachBatch(write_commission_by_type) \
    .trigger(processingTime="20 seconds") \
    .start()


query = commission_by_type.writeStream.outputMode("append").option("checkpointLocation", BASE_PATH_TO_CHECKPOINTING + "/commission_by_type").format("console").trigger(processingTime="20 seconds").start()













commission_ratio = flattened.withWatermark("event_time", "10 seconds").groupBy(
    window(col("event_time"), "1 minute"),
    col("merchant_category")
).agg(
    (_sum("commission_amount") / _sum("total_amount")).alias("commission_ratio")
)

def write_commission_ratio(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    pdf = batch_df.toPandas()
    p = Producer({"bootstrap.servers": "localhost:9092"})

    for _, row in pdf.iterrows():
        payload = {
            "commission_ratio": row["commission_ratio"],
            "merchant_category": row["merchant_category"],
            "window": str(row["window"]),
        }
        p.produce(
            topic="darooghe.commission_ratio",
            key=str(row["merchant_category"]),
            value=json.dumps(payload)
        )

    p.flush()

commission_ratio.writeStream \
    .foreachBatch(write_commission_ratio) \
    .trigger(processingTime="20 seconds") \
    .start()

query = commission_ratio.writeStream.outputMode("append").option("checkpointLocation", BASE_PATH_TO_CHECKPOINTING + "/commission_ratio").format("console").trigger(processingTime="20 seconds").start()









merchant_commission = flattened.withWatermark("event_time", "20 seconds").groupBy(
    window(col("event_time"), "1 minute"),
    col("merchant_id")
).agg(
    _sum("commission_amount").alias("total_commission")
).orderBy( col("window.end").desc(), col("total_commission").desc()).limit(10)


query = merchant_commission.writeStream.outputMode("complete").option("checkpointLocation", BASE_PATH_TO_CHECKPOINTING + "/merchant_commission").format("console").trigger(processingTime="20 second").start()












customer_summary_df = spark.read \
    .format("mongodb") \
    .option("database",   "transaction_analysis") \
    .option("collection", "customer_summary") \
    .load() \
    .select(col("_id").alias("customer_id"), col("avg_trx_value"))


joined_df = flattened.join(
    customer_summary_df,
    on="customer_id",
    how="left"
)


fraud_df = joined_df.filter(
    (col("avg_trx_value").isNotNull()) & 
    (col("total_amount") > col("avg_trx_value") * 10)
).select(
    col("tx_id"),
    lit("Amount anomaly").alias("fraud_type"),
    col("avg_trx_value").alias("customer_avg_trx_value"),
    col("total_amount").alias("transaction_amount")
)
# query = fraud_df.writeStream.outputMode("append").format("console").trigger(processingTime="20 second").start()

def write_fraud_to_kafka(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    pdf = batch_df.toPandas()

    p = Producer({
        "bootstrap.servers": "localhost:9092",
    })


    for _, row in pdf.iterrows():
        key = row["tx_id"]
        payload = {
            "tx_id":                 row["tx_id"],
            "fraud_type":            row["fraud_type"],
            "customer_avg_trx_value": row["customer_avg_trx_value"],
            "transaction_amount":    row["transaction_amount"]
        }
        p.produce(
            topic="darooghe.fraud_alerts",
            key=str(key),
            value=json.dumps(payload),
        )

    p.flush()


fraud_df.writeStream \
    .foreachBatch(write_fraud_to_kafka) \
    .option("checkpointLocation", "/tmp/checkpoints/fraud_amount_anomaly") \
    .trigger(processingTime="1 minute") \
    .start() 








velocity_df = flattened.withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "2 minutes", "1 minute"),
        col("customer_id")
    ) \
    .agg(
        count("*").alias("transaction_count"),
        collect_list("tx_id").alias("transaction_ids")
    ) \
    .filter(col("transaction_count") > 5) \
    .select(
        col("customer_id"),
        col("transaction_ids"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("transaction_count")
    )


def write_velocity_alerts(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    pdf = batch_df.toPandas()
    p = Producer({"bootstrap.servers": "localhost:9092"})

    for _, row in pdf.iterrows():
        payload = {
            "fraud_type": "Velocity Fraud",
            "customer_id": row["customer_id"],
            "transaction_ids": row["transaction_ids"],
            "transaction_count": row["transaction_count"],
            "window_start": str(row["window_start"]),
            "window_end": str(row["window_end"])
        }
        p.produce(
            topic="darooghe.fraud_alerts",
            key=str(row["customer_id"]),
            value=json.dumps(payload)
        )

    p.flush()


velocity_df.writeStream \
    .foreachBatch(write_velocity_alerts) \
    .option("checkpointLocation", "/tmp/checkpoints/fraud_velocity_check") \
    .trigger(processingTime="1 minute") \
    .start()


# velocity_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("checkpointLocation", BASE_PATH_TO_CHECKPOINTING + "/velocity_check") \
#     .trigger(processingTime="1 minute") \
#     .start()









geo_df = flattened.select("tx_id", "customer_id", "event_time", "lat", "lng") \
    .withWatermark("event_time", "10 minutes")

geo_joined = geo_df.alias("t1").join(
    geo_df.alias("t2"),
    (F.col("t1.customer_id") == F.col("t2.customer_id")) &
    (F.col("t1.event_time") < F.col("t2.event_time")) &
    (F.col("t2.event_time") <= F.col("t1.event_time") + expr("INTERVAL 5 MINUTES")),
    how="inner"
)

geo_fraud  = geo_joined.withColumn(
    "euclidean_distance",
    sqrt(
        pow(col("t1.lat") - col("t2.lat"), 2) +
        pow(col("t1.lng") - col("t2.lng"), 2)
    )
).filter(col("euclidean_distance") > 0.5)



geo_alerts = geo_fraud.select(
    col("t1.customer_id").alias("customer_id"),
    col("t1.tx_id").alias("tx_id_1"),
    col("t2.tx_id").alias("tx_id_2"),
    col("t1.event_time").alias("event_time_1"),
    col("t2.event_time").alias("event_time_2"),
    col("euclidean_distance")
)

def write_geo_alerts(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    pdf = batch_df.toPandas()
    p = Producer({"bootstrap.servers": "localhost:9092"})

    for _, row in pdf.iterrows():
        payload = {
            "fraud_type": "Geographical impossibility",
            "customer_id": row["customer_id"],
            "tx_id_1": row["tx_id_1"],
            "tx_id_2": row["tx_id_2"],
            "event_time_1": str(row["event_time_1"]),
            "event_time_2": str(row["event_time_2"]),
            "euclidean_distance": row["euclidean_distance"]
        }
        p.produce(
            topic="darooghe.fraud_alerts",
            key=str(row["customer_id"]),
            value=json.dumps(payload)
        )

    p.flush()

# geo_alerts.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("checkpointLocation", BASE_PATH_TO_CHECKPOINTING + "/geo_alerts") \
#     .trigger(processingTime="1 minute") \
#     .start()

geo_alerts.writeStream \
    .foreachBatch(write_geo_alerts) \
    .option("checkpointLocation", "/tmp/checkpoints/geo_fraud") \
    .trigger(processingTime="1 minute") \
    .start()























mongo_client = MongoClient("mongodb://localhost:27017")
db = mongo_client["transaction_analysis"]  



def update_customer_category_summary(df, epoch_id):
    if df.isEmpty():
        return

    pdf = df.toPandas()
    if "customer_type" in pdf.columns:
        pdf = pdf.rename(columns={"customer_type": "_id"})
    
    collection = db["customer_category_summary"]

    for _, row in pdf.iterrows():
        row_dict = row.to_dict()
        _id = row_dict.pop("_id")

        new_count = row_dict.pop("total_count", 0)
        approved_count = row_dict.pop("approved_count", 0)
        declined_count = row_dict.pop("declined_count", 0)

        existing = collection.find_one({"_id": _id})

        if existing:
            updated = {}

            # Incremental sum updates
            for key in [
                "total_commissions", "total_successful_txns", "total_failed_txns",
                "total_transaction_value", "total_value_approved", "total_value_declined",
                "total_commission_approved", "total_commission_declined"
            ]:  
                # print("updating", key)
                updated[key] = existing.get(key, 0) + row_dict.get(key, 0)

            # Average commission per transaction
            prev_avg_commission = existing.get("avg_commission_per_transaction", 0)
            prev_count = existing.get("total_count", 0)
            new_avg_commission = row_dict.get("avg_commission_per_transaction", 0)
            updated["avg_commission_per_transaction"] = (
                (prev_avg_commission * prev_count + new_avg_commission * new_count)
                / (prev_count + new_count)
            ) if (prev_count + new_count) else 0

            # Average risk level
            prev_avg_risk = existing.get("avg_risk_level", 0)
            updated["avg_risk_level"] = (
                (prev_avg_risk * prev_count + row_dict.get("avg_risk_level", 0) * new_count)
                / (prev_count + new_count)
            ) if (prev_count + new_count) else 0

            # Commission to transaction ratio
            updated["commission_to_transaction_ratio"] = (
                updated["total_commissions"] / updated["total_transaction_value"]
                if updated["total_transaction_value"] else 0
            )

            updated["total_count"] = prev_count + new_count
            updated["approved_count"] = existing.get("approved_count", 0) + approved_count
            updated["declined_count"] = existing.get("declined_count", 0) + declined_count

            collection.replace_one({"_id": _id}, {"_id": _id, **updated}, upsert=True)
        else:
            row_dict["total_count"] = new_count
            row_dict["approved_count"] = approved_count
            row_dict["declined_count"] = declined_count
            collection.insert_one({"_id": _id, **row_dict})




customer_type_stream_df = flattened.withWatermark("event_time", "20 seconds")\
    .groupBy("customer_type",  window("event_time", "1 minute")).agg(
    F.sum("commission_amount").alias("total_commissions"),
    F.avg("commission_amount").alias("avg_commission_per_transaction"),
    (F.sum("commission_amount") / F.sum("total_amount")).alias("commission_to_transaction_ratio"),

    F.sum(F.when(F.col("status") == "approved", 1).otherwise(0)).alias("total_successful_txns"),
    F.sum(F.when(F.col("status") == "declined", 1).otherwise(0)).alias("total_failed_txns"),

    F.sum("total_amount").alias("total_transaction_value"),
    F.sum(F.when(F.col("status") == "approved", F.col("total_amount")).otherwise(0)).alias("total_value_approved"),
    F.sum(F.when(F.col("status") == "declined", F.col("total_amount")).otherwise(0)).alias("total_value_declined"),

    F.sum(F.when(F.col("status") == "approved", F.col("commission_amount")).otherwise(0)).alias("total_commission_approved"),
    F.sum(F.when(F.col("status") == "declined", F.col("commission_amount")).otherwise(0)).alias("total_commission_declined"),

    F.avg("risk_level").alias("avg_risk_level"),

    F.count("*").alias("total_count"),
    F.count(F.when(F.col("status") == "approved", True)).alias("approved_count"),
    F.count(F.when(F.col("status") == "declined", True)).alias("declined_count")
)

customer_type_query = customer_type_stream_df.writeStream \
    .foreachBatch(update_customer_category_summary) \
    .outputMode("append") \
    .option("checkpointLocation", BASE_PATH_TO_CHECKPOINTING + "/customer_type_query")\
    .trigger(processingTime="1 minute") \
    .start()










merchant_categories = ["retail", "food_service", "entertainment", "transportation", "government"]

# Basic aggregations
customer_stream_df = flattened.withWatermark("event_time", "20 seconds") \
    .groupBy("customer_id", window("event_time", "1 minute")).agg(
        F.count("*").alias("total_trxs"),
        F.sum("total_amount").alias("total_trx_value"),
        F.sum("commission_amount").alias("total_commissions"),
        F.avg("commission_amount").alias("avg_commission"),
        F.avg("total_amount").alias("avg_trx_value"),
        F.sum(F.when(F.col("status") == "approved", 1).otherwise(0)).alias("approved_trxs"),
        F.sum(F.when(F.col("status") == "declined", 1).otherwise(0)).alias("declined_trxs")
    )



def update_customer_summary(df, epoch_id):
    # print("hi\n\n\n\n\n\n\n\n\n\n\n\n")
    if df.isEmpty():
        return

    pdf = df.toPandas()
    if "customer_id" in pdf.columns:
        pdf = pdf.rename(columns={"customer_id": "_id"})

    collection = db["customer_summary"]

    for _, row in pdf.iterrows():
        row_dict = row.to_dict()
        _id = row_dict.pop("_id")
        new_count = row_dict.pop("total_trxs", 0)
        existing = collection.find_one({"_id": _id})

        if existing:
            updated = {}

            for key in ["total_trx_value", "total_commissions", "approved_trxs", "declined_trxs"]:
                updated[key] = existing.get(key, 0) + row_dict.get(key, 0)

            prev_count = existing.get("total_trxs", 0)
            for key in ["avg_commission", "avg_trx_value"]:
                prev_avg = existing.get(key, 0)
                new_avg = row_dict.get(key, 0)
                updated[key] = (
                    (prev_avg * prev_count + new_avg * new_count) / (prev_count + new_count)
                ) if (prev_count + new_count) else 0

  
            updated["total_trxs"] = prev_count + new_count

            collection.replace_one({"_id": _id}, {"_id": _id, **updated}, upsert=True)
        else:
            row_dict["total_trxs"] = new_count
            collection.insert_one({"_id": _id, **row_dict})



customer_summary_query = customer_stream_df.writeStream \
    .foreachBatch(update_customer_summary) \
    .outputMode("append") \
    .option("checkpointLocation", BASE_PATH_TO_CHECKPOINTING + "/customer_summary_query")\
    .trigger(processingTime="1 minute") \
    .start()



















merchant_category_stream_df = flattened.withWatermark("event_time", "20 seconds") \
    .groupBy("merchant_category", window("event_time", "1 minute")).agg(
        F.sum("commission_amount").alias("total_commissions"),
        F.avg("commission_amount").alias("avg_commission_per_transaction"),
        (F.sum("commission_amount") / F.sum("total_amount")).alias("commission_to_transaction_ratio"),

        F.sum(F.when(F.col("status") == "approved", 1).otherwise(0)).alias("total_successful_txns"),
        F.sum(F.when(F.col("status") == "declined", 1).otherwise(0)).alias("total_failed_txns"),

        F.sum("total_amount").alias("total_transaction_value"),
        F.sum(F.when(F.col("status") == "approved", F.col("total_amount")).otherwise(0)).alias("total_value_approved"),
        F.sum(F.when(F.col("status") == "declined", F.col("total_amount")).otherwise(0)).alias("total_value_declined"),

        F.sum(F.when(F.col("status") == "approved", F.col("commission_amount")).otherwise(0)).alias("total_commission_approved"),
        F.sum(F.when(F.col("status") == "declined", F.col("commission_amount")).otherwise(0)).alias("total_commission_declined"),

        F.avg("risk_level").alias("avg_risk_level"),

        F.count("*").alias("total_count")
    )


def update_merchant_category_summary(df, epoch_id):
    if df.isEmpty():
        return

    pdf = df.toPandas()
    if "merchant_category" in pdf.columns:
        pdf = pdf.rename(columns={"merchant_category": "_id"})

    collection = db["merchant_category_summary"]

    for _, row in pdf.iterrows():
        row_dict = row.to_dict()
        _id = row_dict.pop("_id")

        new_count = row_dict.pop("total_count", 0)
        existing = collection.find_one({"_id": _id})

        if existing:
            updated = {}

            for key in [
                "total_commissions", "total_successful_txns", "total_failed_txns",
                "total_transaction_value", "total_value_approved", "total_value_declined",
                "total_commission_approved", "total_commission_declined"
            ]:
                updated[key] = existing.get(key, 0) + row_dict.get(key, 0)

            prev_count = existing.get("total_count", 0)

            prev_avg_commission = existing.get("avg_commission_per_transaction", 0)
            new_avg_commission = row_dict.get("avg_commission_per_transaction", 0)
            updated["avg_commission_per_transaction"] = (
                (prev_avg_commission * prev_count + new_avg_commission * new_count) / (prev_count + new_count)
            ) if (prev_count + new_count) else 0

            prev_avg_risk = existing.get("avg_risk_level", 0)
            updated["avg_risk_level"] = (
                (prev_avg_risk * prev_count + row_dict.get("avg_risk_level", 0) * new_count) / (prev_count + new_count)
            ) if (prev_count + new_count) else 0

            updated["commission_to_transaction_ratio"] = (
                updated["total_commissions"] / updated["total_transaction_value"]
                if updated["total_transaction_value"] else 0
            )

            updated["total_count"] = prev_count + new_count

            collection.replace_one({"_id": _id}, {"_id": _id, **updated}, upsert=True)
        else:
            row_dict["total_count"] = new_count
            collection.insert_one({"_id": _id, **row_dict})


merchant_category_query = merchant_category_stream_df.writeStream \
    .foreachBatch(update_merchant_category_summary) \
    .outputMode("append") \
    .option("checkpointLocation", BASE_PATH_TO_CHECKPOINTING + "/merchant_category_query")\
    .trigger(processingTime="1 minute") \
    .start()















merchant_summary_stream_df = flattened.withWatermark("event_time", "20 seconds") \
    .groupBy("merchant_id", window("event_time", "1 minute")).agg(
        F.count("*").alias("total_trxs"),
        F.sum("total_amount").alias("total_trx_value"),
        F.sum("commission_amount").alias("total_commissions"),
        F.avg("commission_amount").alias("avg_commission"),
        F.avg("total_amount").alias("avg_trx_value"),

        F.sum(F.when(F.col("status") == "approved", 1).otherwise(0)).alias("approved_trxs"),
        F.sum(F.when(F.col("status") == "declined", 1).otherwise(0)).alias("declined_trxs")
    )


def update_merchant_summary(df, epoch_id):
    if df.isEmpty():
        return

    pdf = df.toPandas()
    if "merchant_id" in pdf.columns:
        pdf = pdf.rename(columns={"merchant_id": "_id"})

    collection = db["merchant_summary"]

    for _, row in pdf.iterrows():
        row_dict = row.to_dict()
        _id = row_dict.pop("_id")

        new_count = row_dict.pop("total_trxs", 0)
        existing = collection.find_one({"_id": _id})

        if existing:
            updated = {}

            for key in [
                "total_trxs", "total_trx_value", "total_commissions", 
                "approved_trxs", "declined_trxs"
            ]:
                updated[key] = existing.get(key, 0) + row_dict.get(key, 0)

            prev_avg_commission = existing.get("avg_commission", 0)
            prev_count = existing.get("total_trxs", 0)
            new_avg_commission = row_dict.get("avg_commission", 0)
            updated["avg_commission"] = (
                (prev_avg_commission * prev_count + new_avg_commission * new_count)
                / (prev_count + new_count)
            ) if (prev_count + new_count) else 0

            prev_avg_trx_value = existing.get("avg_trx_value", 0)
            updated["avg_trx_value"] = (
                (prev_avg_trx_value * prev_count + row_dict.get("avg_trx_value", 0) * new_count)
                / (prev_count + new_count)
            ) if (prev_count + new_count) else 0

            updated["total_trxs"] = prev_count + new_count

            collection.replace_one({"_id": _id}, {"_id": _id, **updated}, upsert=True)
        else:
            row_dict["total_trxs"] = new_count
            collection.insert_one({"_id": _id, **row_dict})

merchant_summary_query = merchant_summary_stream_df.writeStream \
    .foreachBatch(update_merchant_summary) \
    .outputMode("append") \
    .option("checkpointLocation", BASE_PATH_TO_CHECKPOINTING + "/merchant_summary_query")\
    .trigger(processingTime="1 minute") \
    .start()





spark_df_tmp = flattened.withColumn("date", F.to_date("event_time"))

daily_summary_stream_df = spark_df_tmp.withWatermark("event_time", "20 seconds") \
    .groupBy("date", window("event_time", "1 day")).agg(
        F.count("*").alias("total_transactions"),
        F.sum(F.when(F.col("status") == "approved", 1).otherwise(0)).alias("approved_transactions"),
        F.sum(F.when(F.col("status") == "declined", 1).otherwise(0)).alias("declined_transactions"),
        F.avg("risk_level").alias("avg_risk_level"),
        F.sum("total_amount").alias("total_transaction_value"),
        F.sum("commission_amount").alias("total_commissions")
    )

def update_daily_summary(df, epoch_id):
    if df.isEmpty():
        return

    pdf = df.toPandas()
    if "date" in pdf.columns:
        pdf = pdf.rename(columns={"date": "_id"})

    collection = db["daily_summary"]

    for _, row in pdf.iterrows():
        row_dict = row.to_dict()
        _id = row_dict.pop("_id")

        new_count = row_dict.pop("total_transactions", 0)
        approved_count = row_dict.pop("approved_transactions", 0)
        declined_count = row_dict.pop("declined_transactions", 0)

        existing = collection.find_one({"_id": _id})
        # print("chekcing the daily \n\n\n\n\n\n\n\n\n\n\n\n")
        if existing:
            updated = {}

            for key in [
                "total_transactions", "total_transaction_value", "total_commissions",
                "approved_transactions", "declined_transactions"
            ]:
                updated[key] = existing.get(key, 0) + row_dict.get(key, 0)

            prev_avg_risk = existing.get("avg_risk_level", 0)
            updated["avg_risk_level"] = (
                (prev_avg_risk * existing.get("total_transactions", 0) + row_dict.get("avg_risk_level", 0) * new_count)
                / (existing.get("total_transactions", 0) + new_count)
            ) if (existing.get("total_transactions", 0) + new_count) else 0

            updated["total_transactions"] = existing.get("total_transactions", 0) + new_count
            updated["approved_transactions"] = existing.get("approved_transactions", 0) + approved_count
            updated["declined_transactions"] = existing.get("declined_transactions", 0) + declined_count

            collection.replace_one({"_id": _id}, {"_id": _id, **updated}, upsert=True)
        else:
            row_dict["total_transactions"] = new_count
            row_dict["approved_transactions"] = approved_count
            row_dict["declined_transactions"] = declined_count
            # print("inserting================================\n======================================")
            collection.insert_one({"_id": _id, **row_dict})


daily_summary_query = daily_summary_stream_df.writeStream \
    .foreachBatch(update_daily_summary) \
    .outputMode("append") \
    .option("checkpointLocation", BASE_PATH_TO_CHECKPOINTING + "/daily_summary_query")\
    .trigger(processingTime="1 minute") \
    .start()











def insert_transaction_summary(df, epoch_id):
    # print("hi adding transactions")
    if df.isEmpty():
        return

    pdf = df.toPandas()

    records = pdf.to_dict(orient="records")

    if records:
        for rec in records:
            rec["_id"] = rec.pop("tx_id")
        try:
            db["transaction_summary"].insert_many(records, ordered=False)
        except pymongo.errors.BulkWriteError as bwe:
            # print("Duplicate entries skipped:", bwe.details)
            pass

    # print("+======================================\n doneeeeeeeeeeeee")


transaction_summary_query = flattened.writeStream \
    .foreachBatch(insert_transaction_summary) \
    .outputMode("append") \
    .option("checkpointLocation", BASE_PATH_TO_CHECKPOINTING + "/transaction_summary_query")\
    .trigger(processingTime="20 seconds") \
    .start()


spark.streams.awaitAnyTermination()


