from confluent_kafka import Consumer
import json

def main():
    conf = {
        'bootstrap.servers': 'localhost:9092',   # adjust if needed
        'group.id': 'error_logs_consumer_group',
        'auto.offset.reset': 'earliest',         # consume from beginning if new
    }

    consumer = Consumer(conf)

    topic = "darooghe.error_logs"
    consumer.subscribe([topic])

    print(f"Listening for error logs on topic: {topic}...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # poll messages

            if msg is None:
                continue  # no message this cycle

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            # Decode message
            try:
                error_log = json.loads(msg.value().decode('utf-8'))
            except Exception as e:
                print(f"Failed to decode message: {e}")
                continue

            # Print the error log nicely
            print("---- Received Error Log ----")
            print(f"Transaction ID: {error_log.get('transaction_id')}")
            print(f"Error Codes: {error_log.get('error_codes')}")
            print(f"Original Data: {error_log.get('original_data')}")
            print("------------------------------")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
