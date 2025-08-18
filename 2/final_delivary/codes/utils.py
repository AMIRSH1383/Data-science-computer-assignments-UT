from confluent_kafka import Consumer
from confluent_kafka import Producer
import json
import datetime
from enum import Enum

class Error(Enum):
    ERR_AMOUNT = 1
    ERR_TIME = 2
    ERR_DEVICE = 3
    VALID = 4

def convert_transaction(raw_json):
    tx = json.loads(raw_json)
    
    tx['timestamp'] = datetime.datetime.fromisoformat(tx['timestamp'].replace('Z', ''))
    tx['amount'] = int(tx['amount'])
    tx['commission_amount'] = int(tx['commission_amount'])
    tx['vat_amount'] = int(tx['vat_amount'])
    tx['total_amount'] = int(tx['total_amount'])
    tx['risk_level'] = int(tx['risk_level'])

    if 'location' in tx and isinstance(tx['location'], dict):
        tx['location']['lat'] = float(tx['location']['lat'])
        tx['location']['lng'] = float(tx['location']['lng'])
    
    # All other attributes have correct data types
    # device_info stays as dict
    # failure_reason can stay None or str
    return tx

def validate_transaction_data(transaction):

    # Device mismatch
    if transaction["payment_method"] == "mobile":
        if  transaction["device_info"]["os"] not in ["iOS","Android"]:
            return Error.ERR_DEVICE
        
    # Amount consistency
    if (transaction["amount"] + transaction["vat_amount"] + transaction["commission_amount"]) != transaction["total_amount"]:
        return Error.ERR_AMOUNT
    
    # Time warping
    ingestion_time = datetime.datetime.now()
    transaction["timestamp"]
    # Rule 1: Cannot be in the future 
    # Rule 2: Cannot be older than 24 hours
    if transaction["timestamp"] > ingestion_time or transaction["timestamp"] < (ingestion_time - datetime.timedelta(days=1)):
        return Error.ERR_TIME

    return Error.VALID
    
def send_error_to_kafka(transaction, error, producer):
    error_code = ""
    if error == Error.ERR_AMOUNT:
        error_code = "ERR_AMOUNT"
    elif error == Error.ERR_TIME:
        error_code = "ERR_TIME"
    elif error == Error.ERR_DEVICE:
        error_code = "ERR_DEVICE"

    error_message = {
        "transaction_id": transaction.get("transaction_id", "UNKNOWN"),
        "error_codes": error_code,
        "original_data": transaction
    }
    
    # Send to 'darooghe.error_logs' topic
    producer.produce(
        topic="darooghe.error_logs",
        key=transaction.get("transaction_id", "UNKNOWN"),
        value=json.dumps(error_message,default=default_converter)
    )
    producer.flush()

def default_converter(o):
    if isinstance(o, datetime.datetime):
        return o.isoformat()
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

def flatten_transaction(tx):
    flat = {}

    # Copy simple fields
    for key in [
        "transaction_id", "timestamp", "customer_id", "merchant_id",
        "merchant_category", "payment_method", "amount",
        "status", "commission_type", "commission_amount",
        "vat_amount", "total_amount", "customer_type", "risk_level", "failure_reason"
    ]:
        flat[key] = tx.get(key)

    # Flatten location
    loc = tx.get("location", {})
    flat["location_lat"] = loc.get("lat")
    flat["location_lng"] = loc.get("lng")

    # Flatten device_info
    dev = tx.get("device_info", {})
    flat["device_os"] = dev.get("os")
    flat["device_app_version"] = dev.get("app_version")
    flat["device_model"] = dev.get("device_model")

    return flat
