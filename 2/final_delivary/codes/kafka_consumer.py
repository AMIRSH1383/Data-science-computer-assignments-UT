from confluent_kafka import Consumer
from confluent_kafka import Producer
import json
import datetime
from utils import *
import pandas as pd

FLATTENED_TRANSACTION_COLUMNS = [
    "transaction_id",
    "timestamp",
    "customer_id",
    "merchant_id",
    "merchant_category",
    "payment_method",
    "amount",
    "status",
    "commission_type",
    "commission_amount",
    "vat_amount",
    "total_amount",
    "customer_type",
    "risk_level",
    "failure_reason",
    "location_lat",
    "location_lng",
    "device_os",
    "device_app_version",
    "device_model",
]

df = pd.DataFrame(columns = FLATTENED_TRANSACTION_COLUMNS)

# Set up Kafka producer for errors
producer_conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_conf)

# Set up Kafka consumer
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',  
    'group.id': 'darooghe-consumer-group',
    'auto.offset.reset': 'earliest'  
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['darooghe.transactions']) 

try:
    for i in range(20000):
        msg = consumer.poll(1.0)  # Wait for message
        if msg is None:
            continue

        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        raw_json = msg.value().decode('utf-8')
        #print(f"Received: {raw_json}")  # Print raw JSON
        transaction = convert_transaction(msg.value().decode('utf-8'))
        error_type = validate_transaction_data(transaction)
        if error_type != Error.VALID and error_type != Error.ERR_AMOUNT:
            print("error: ", error_type)
            send_error_to_kafka(transaction, error_type, producer)
        else:
            flat_tx = flatten_transaction(transaction)
            # Add the new flattened transaction as a row
            if(i%1000 == 0):
                print("hi: ", error_type, len(df))
            df.loc[len(df)] = flat_tx

except KeyboardInterrupt:
    pass

df.to_csv("tmp_out.csv")
consumer.close()

