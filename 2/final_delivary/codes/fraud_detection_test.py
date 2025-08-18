from confluent_kafka import Consumer, KafkaError

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'fraud_alerts_printer',    
    'auto.offset.reset': 'earliest'        
}

consumer = Consumer(conf)

# Subscribe to the topic
consumer.subscribe(['darooghe.fraud_alerts'])

print("Listening for messages on 'darooghe.fraud_alerts'...")

try:
    while True:
        msg = consumer.poll(1.0) 
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Consumer error: {msg.error()}")
            continue

        key   = msg.key().decode('utf-8') if msg.key() else None
        value = msg.value().decode('utf-8')
        print(f"Received message: key={key}, value={value}")

except KeyboardInterrupt:
    print("Shutting down consumer...")

finally:
   # Close  consumer 
    consumer.close()
