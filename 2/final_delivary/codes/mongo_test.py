from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")

# Access the database
db = client["transaction_analysis"]  
# Fetch and print a few records from each collection
daily_summary = db["daily_summary"].find().limit(5)
merchant_summary = db["merchant_summary"].find().limit(5)
customer_summary = db["customer_summary"].find().limit(5)
total_transaction_summary = db["transaction_summary"].find().limit(1)

customer_catefory_summary = db["customer_category_summary"].find().limit(3)
merchant_catefory_summary = db["merchant_category_summary"].find().limit(3)


print("=== Daily Summary ===")
for record in daily_summary:
    print(record)

print("\n=== Merchant Summary ===")
for record in merchant_summary:
    print(record)

print("\n=== Customer Summary ===")
for record in customer_summary:
    print(record)

print("\n=== total_transaction Summary ===")
for record in total_transaction_summary:
    print(record)


print("\n=== Customer category Summary ===")
for record in customer_catefory_summary:
    print(record)

print("\n=== merchant category Summary ===")
for record in merchant_catefory_summary:
    print(record)
