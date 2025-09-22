# consumer.py
import json
from kafka import KafkaConsumer
from pymongo import MongoClient, errors
import time

# --- الإعدادات ---
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'currency_rates'
KAFKA_GROUP_ID = 'currency-storage-group'

MONGO_URI = 'mongodb://localhost:27017/'
MONGO_DB = 'currency_db'
MONGO_COLLECTION = 'rates'

# --- الاتصال بـ MongoDB مع محاولة إعادة الاتصال ---
def connect_to_mongo():
    while True:
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            client.server_info() # اختبار الاتصال
            print("MongoDB connected successfully.")
            return client[MONGO_DB][MONGO_COLLECTION]
        except errors.ServerSelectionTimeoutError as e:
            print(f"Could not connect to MongoDB: {e}. Retrying in 5 seconds...")
            time.sleep(5)

# --- تهيئة Kafka Consumer ---
def create_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest', # البدء من أول رسالة
                group_id=KAFKA_GROUP_ID,
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )
            print("Kafka Consumer connected successfully.")
            return consumer
        except Exception as e:
            print(f"Could not connect to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)


def main():
    """
    الدالة الرئيسية التي تستمع للرسائل وتخزنها.
    """
    collection = connect_to_mongo()
    consumer = create_kafka_consumer()
    
    print("Starting currency rate consumer...")
    print(f"Listening for messages on topic '{KAFKA_TOPIC}'...")

    for message in consumer:
        try:
            rate_data = message.value
            
            # إضافة حقل تاريخ مقروء
            rate_data['readable_timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(rate_data['timestamp']))
            
            # إدخال البيانات في MongoDB
            collection.insert_one(rate_data)
            
            print(f"Stored data in MongoDB: {rate_data}")
        
        except json.JSONDecodeError:
            print(f"Error decoding message: {message.value}")
        except Exception as e:
            print(f"An error occurred while processing message: {e}")

if __name__ == "__main__":
    main()
