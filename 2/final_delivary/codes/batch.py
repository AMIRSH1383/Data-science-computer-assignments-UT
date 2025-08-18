import os
import sys
import pandas as pd
import uuid
import random
import datetime
from pymongo import MongoClient


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import hour, dayofweek, month


client = MongoClient("mongodb://localhost:27017/")
db = client["transaction_analysis"]

collections_to_reset = ["transaction_summary","daily_summary", "merchant_summary", "customer_summary", "customer_category_summary" ,"merchant_category_summary"]
for name in collections_to_reset:
    db[name].drop()

def insert_dataframe(df, collection_name, id_column=None):
    pdf = df.toPandas()
    
    if id_column and id_column in pdf.columns:
        pdf = pdf.rename(columns={id_column: "_id"})

    records = pdf.to_dict("records")
    if records:
        db[collection_name].insert_many(records)
    else:
        print(f"No records to insert for {collection_name}.")



os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


OUTPUT_DIR = "output/batch/"



dummy_df = pd.read_csv("tmp_out.csv")

spark = SparkSession.builder \
    .appName("Darooghe Batch Processing") \
    .getOrCreate()

spark_df = spark.createDataFrame(dummy_df)


insert_dataframe(spark_df, "transaction_summary", id_column="transaction_id")




merchant_category_df = spark_df.groupBy("merchant_category").agg(
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

    F.avg("risk_level").alias("avg_risk_level")
)

merchant_category_df.show()
merchant_category_df.write.csv(OUTPUT_DIR + "merchant_category_df", header=True, mode="overwrite")
insert_dataframe(merchant_category_df, "merchant_category_summary", id_column="merchant_category")




customer_type_df = spark_df.groupBy("customer_type").agg(
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
    F.count("*").alias("total_count"),

    F.avg("risk_level").alias("avg_risk_level")
)

customer_type_df.show()
customer_type_df.write.csv(OUTPUT_DIR + "customer_type_df", header=True, mode="overwrite")
insert_dataframe(customer_type_df, "customer_category_summary", id_column="customer_type")








spark_df_tmp = spark_df.withColumn("date", F.to_date("timestamp"))

daily_summary_df = spark_df_tmp.groupBy("date").agg(
    F.count("*").alias("total_transactions"),
    F.sum(F.when(F.col("status") == "approved", 1).otherwise(0)).alias("approved_transactions"),
    F.sum(F.when(F.col("status") == "declined", 1).otherwise(0)).alias("declined_transactions"),
    F.avg("risk_level").alias("avg_risk_level"),
    F.sum("total_amount").alias("total_transaction_value"),
    F.sum("commission_amount").alias("total_commissions")
).orderBy(F.col("date").desc())

daily_summary_df.show()
daily_summary_df.write.csv(OUTPUT_DIR + "daily_summary_df", header=True, mode="overwrite")
# insert_dataframe(daily_summary_df, "daily_summary", id_column="date")
daily_summary_pdf = daily_summary_df.toPandas()
daily_summary_pdf["date"] = pd.to_datetime(daily_summary_pdf["date"])  

insert_dataframe(spark.createDataFrame(daily_summary_pdf), "daily_summary", "date")




# merchant_categories = ["retail", "food_service", "entertainment", "transportation", "government"]

customer_summary_df = spark_df.groupBy("customer_id").agg(
    F.count("*").alias("total_trxs"),
    F.sum("total_amount").alias("total_trx_value"),
    F.sum("commission_amount").alias("total_commissions"),
    F.avg("commission_amount").alias("avg_commission"),
    F.avg("total_amount").alias("avg_trx_value"),
    F.sum(F.when(F.col("status") == "approved", 1).otherwise(0)).alias("approved_trxs"),
    F.sum(F.when(F.col("status") == "declined", 1).otherwise(0)).alias("declined_trxs"),
)


customer_summary_df = customer_summary_df.orderBy(F.col("total_commissions").desc())

customer_summary_df.show()
customer_summary_df.write.csv(OUTPUT_DIR + "customer_summary_df", header=True, mode="overwrite")
insert_dataframe(customer_summary_df, "customer_summary", id_column="customer_id")






merchant_summary_df = spark_df.groupBy("merchant_id").agg(
    F.count("*").alias("total_trxs"),
    F.sum("total_amount").alias("total_trx_value"),
    F.sum("commission_amount").alias("total_commissions"),
    F.avg("commission_amount").alias("avg_commission"),
    F.avg("total_amount").alias("avg_trx_value"),
    F.sum(F.when(F.col("status") == "approved", 1).otherwise(0)).alias("approved_trxs"),
    F.sum(F.when(F.col("status") == "declined", 1).otherwise(0)).alias("declined_trxs"),
)

merchant_summary_df = merchant_summary_df.orderBy(F.col("total_commissions").desc())
merchant_summary_df.show()
merchant_summary_df.write.csv(OUTPUT_DIR + "merchant_summary_df", header=True, mode="overwrite")
insert_dataframe(merchant_summary_df, "merchant_summary", id_column="merchant_id")


















commission_performance_df = spark_df.groupBy( "commission_type").agg(
    F.sum("commission_amount").alias("total_commissions"),
    F.avg("commission_amount").alias("avg_commission_per_trx"),
    F.sum("total_amount").alias("total_trx_amount"),
    (F.sum("commission_amount") / F.sum("total_amount")).alias("commission_to_trx_ratio")
)


commission_performance_df.show()












spark_df = spark_df.withColumn("timestamp", F.col("timestamp").cast("timestamp"))
spark_df = spark_df.withColumn("hour_of_day", hour("timestamp"))
spark_df = spark_df.withColumn("day_of_week", dayofweek("timestamp"))
spark_df = spark_df.withColumn("month", month("timestamp"))


hourly_transactions_df = spark_df.groupBy("hour_of_day").agg(
    F.count("transaction_id").alias("num_transactions"),
    F.sum("total_amount").alias("total_transaction_amount")
)


daily_transactions_df = spark_df.groupBy("day_of_week").agg(
    F.count("transaction_id").alias("num_transactions"),
    F.sum("total_amount").alias("total_transaction_amount")
)


monthly_transactions_df = spark_df.groupBy("month").agg(
    F.count("transaction_id").alias("num_transactions"),
    F.sum("total_amount").alias("total_transaction_amount")
)


hourly_transactions_df.show()
daily_transactions_df.show()
monthly_transactions_df.show()



























spark_df = spark_df.withColumn("timestamp", F.col("timestamp").cast("timestamp"))

# Calculate the number of transactions and total spending per customer
customer_spending_df = spark_df.groupBy("customer_id").agg(
    F.count("transaction_id").alias("num_transactions"),
    F.sum("total_amount").alias("total_spending"),
    F.max("timestamp").alias("last_transaction_time")
)

# Get the current date
current_date = datetime.datetime.utcnow()

customer_spending_df = customer_spending_df.withColumn(
    "recency_days", 
    F.datediff(F.lit(current_date), "last_transaction_time")
)


spending_frequency_threshold = 10  #  10 or more transactions
spending_amount_threshold = 100000  # spending more than 100,000 units
recency_threshold = 30  # recent transaction within 30 days

# Classify customers based on their spending behavior
customer_spending_df = customer_spending_df.withColumn(
    "segment",
    F.when(F.col("num_transactions") >= spending_frequency_threshold, "High Frequency")
     .otherwise("Low Frequency")
)

customer_spending_df = customer_spending_df.withColumn(
    "spending_segment",
    F.when(F.col("total_spending") >= spending_amount_threshold, "High Spending")
     .otherwise("Low Spending")
)

customer_spending_df = customer_spending_df.withColumn(
    "recency_segment",
    F.when(F.col("recency_days") <= recency_threshold, "Recent")
     .otherwise("Inactive")
)

customer_spending_df = customer_spending_df.withColumn(
    "final_segment",
    F.concat_ws("_", "segment", "spending_segment", "recency_segment")
)

customer_spending_df.select("customer_id", "final_segment").show()

# customer_spending_df.write.csv("/path/to/save/customer_segments.csv", header=True)

# spark_df.printSchema()
# spark_df.show(5)






















merchant_comparison_df = spark_df.groupBy("merchant_category").agg(
    F.sum("total_amount").alias("total_transaction_amount"),
    F.avg("total_amount").alias("avg_transaction_amount"),
    F.count("transaction_id").alias("num_transactions"),
    F.sum("commission_amount").alias("total_commission"),
    F.avg("commission_amount").alias("avg_commission_per_transaction"),
    (F.sum("commission_amount") / F.sum("total_amount")).alias("commission_to_transaction_ratio"),
    (F.sum(F.when(F.col("status") == "approved", 1).otherwise(0)) / F.count("transaction_id")).alias("success_rate"),
    F.avg("risk_level").alias("avg_risk_level")
)

merchant_comparison_df.show()

merchant_comparison_df.orderBy(F.col("total_transaction_amount").desc()).show()

merchant_comparison_df.orderBy(F.col("success_rate").desc()).show()

merchant_comparison_df.orderBy(F.col("commission_to_transaction_ratio").desc()).show()

















# 1. Extract the Hour of the Day from the Timestamp
spark_df = spark_df.withColumn("hour_of_day", F.hour("timestamp"))

# 2. Define Time Blocks (Morning, Afternoon, Evening, Night)
def time_of_day(hour):
    if 6 <= hour < 12:
        return "Morning"
    elif 12 <= hour < 18:
        return "Afternoon"
    elif 18 <= hour < 24:
        return "Evening"
    else:
        return "Night"

# Register the UDF (User Defined Function) for time categorization
time_of_day_udf = F.udf(time_of_day, StringType())

# Apply the UDF to create a new column 'time_of_day'
spark_df = spark_df.withColumn("time_of_day", time_of_day_udf(F.col("hour_of_day")))

# 3. Aggregate Transactions by Time of Day
time_of_day_summary_df = spark_df.groupBy("time_of_day").agg(
    F.count("transaction_id").alias("num_transactions"),
    F.sum("total_amount").alias("total_transaction_amount")
)

# 4. Show the Results
time_of_day_summary_df.show()

time_of_day_summary_df.orderBy(F.col("num_transactions").desc()).show()




















from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Extract year and week of the year
spark_df = spark_df.withColumn("year", F.year("timestamp")) \
                   .withColumn("week_of_year", F.weekofyear("timestamp"))

# 2. Aggregate total spending per week (group by year and week)
weekly_spending_df = spark_df.groupBy("year", "week_of_year").agg(
    F.sum("total_amount").alias("total_spending")
)

# 3. Define the window: partition by year, order by week number
weekly_window = Window.partitionBy("year").orderBy("week_of_year")

# 4. Calculate percentage change week over week
weekly_spending_df = weekly_spending_df.withColumn(
    "percentage_change",
    (F.col("total_spending") - F.lag("total_spending", 1).over(weekly_window)) / F.lag("total_spending", 1).over(weekly_window) * 100
)

# 5. Sort the results for better visualization
weekly_spending_df = weekly_spending_df.orderBy("year", "week_of_year")

# 6. Show final result
weekly_spending_df.show()



















# First extract the hour early
spark_df = spark_df.withColumn("hour", F.hour("timestamp"))

# --- Risk Level by Hour ---
risk_by_hour_df = spark_df.groupBy("hour").agg(
    F.avg("risk_level").alias("avg_risk_level")
).orderBy("hour")

risk_by_hour_df.show()
risk_by_hour_df.write.csv(OUTPUT_DIR + "risk_by_hour_df", header=True, mode="overwrite")

# Only failed transactions
failed_df = spark_df.filter(F.col("failure_reason").isNotNull())

# Group by hour and failure reason
failure_reason_by_hour_df = failed_df.groupBy("hour", "failure_reason").agg(
    F.count("*").alias("failures")
).orderBy("hour", "failure_reason")

failure_reason_by_hour_df.show()
failure_reason_by_hour_df.write.csv(OUTPUT_DIR + "failure_reason_by_hour_df", header=True, mode="overwrite")

# --- Payment Method Usage by Hour ---
payment_by_hour_df = spark_df.groupBy("hour", "payment_method").agg(
    F.count("*").alias("transaction_count")
).orderBy("hour", "payment_method")

payment_by_hour_df.show()
payment_by_hour_df.write.csv(OUTPUT_DIR + "payment_by_hour_df", header=True, mode="overwrite")

# Group by hour and status
status_by_hour_df = spark_df.groupBy("hour", "status").agg(
    F.count("*").alias("count")
).groupBy("hour").pivot("status").sum("count").fillna(0).orderBy("hour")

# Calculate approval rate
status_by_hour_df = status_by_hour_df.withColumn(
    "approval_rate", 
    (F.col("approved") / (F.col("approved") + F.col("declined"))) * 100
).orderBy("hour")

status_by_hour_df.show()
status_by_hour_df.write.csv(OUTPUT_DIR + "status_by_hour_df", header=True, mode="overwrite")

