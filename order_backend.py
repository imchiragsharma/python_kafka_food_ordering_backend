import json
import time

from kafka import KafkaProducer, producer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_LIMIT = 15_000

producer = KafkaProducer(bootstrap_servers="localhost:29092")

print("Going to generating order after 10 seconds")
print("Will generate one unique order every 10 seconds")

for i in range(1, ORDER_LIMIT):
    data = {
        "order_id": i,
        "user_id": f"tom_{i}",
        "total_cost" : i * 2,
        "items": "burger, sandwich"
    }
    producer.send(
        ORDER_KAFKA_TOPIC,
        json.dumps(data).encode("utf-8")
    )   
    print(f"Done sending..{i}")
    

