# هذا التطبيق يعمل كل دقيقة، يجلب البيانات من الـ API ويرسلها إلى Kafka.
import requests
import json
import time
import schedule
from kafka import KafkaProducer

# --- الإعدادات ---
# استبدل "YOUR_API_KEY" بمفتاح الـ API الخاص بك
API_KEY = "b1c2fb571b0316990db061c0" 
API_URL = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/YER"

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'currency_rates'

# --- تهيئة Kafka Producer ---
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v ).encode('utf-8')
    )
    print("Kafka Producer connected successfully.")
except Exception as e:
    print(f"Error connecting to Kafka: {e}")
    exit()

def fetch_and_send_rates():
    """
    تجلب أسعار الصرف من الـ API وترسلها إلى Kafka.
    """
    try:
        response = requests.get(API_URL)
        response.raise_for_status()  # يطلق خطأ إذا كانت الاستجابة غير ناجحة
        
        data = response.json()
        
        if data.get("result") == "success":
            rates = data.get("conversion_rates", {})
            usd_rate = rates.get("USD")
            sar_rate = rates.get("SAR")
            
            if usd_rate and sar_rate:
                # سعر 1 ريال يمني مقابل العملات الأخرى
                # سنقوم بعكسها للحصول على سعر الدولار/السعودي مقابل الريال اليمني
                yer_per_usd = 1 / usd_rate
                yer_per_sar = 1 / sar_rate

                message = {
                    "base_currency": "YER",
                    "timestamp": time.time(),
                    "rates": {
                        "USD": round(yer_per_usd, 2),
                        "SAR": round(yer_per_sar, 2)
                    }
                }
                
                # إرسال الرسالة إلى Kafka
                producer.send(KAFKA_TOPIC, message)
                producer.flush() # التأكد من إرسال الرسالة
                
                print(f"Sent data to Kafka: {message}")
            else:
                print("USD or SAR rates not found in API response.")
        else:
            print(f"API request failed: {data.get('error-type')}")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# --- جدولة المهمة ---
print("Starting currency rate producer...")
# تنفيذ المهمة فورًا عند بدء التشغيل
fetch_and_send_rates() 

# جدولة المهمة لتعمل كل دقيقة
schedule.every(1).minute.do(fetch_and_send_rates)

while True:
    schedule.run_pending()
    time.sleep(1)
