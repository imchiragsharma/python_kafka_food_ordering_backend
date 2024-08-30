import json 

from kafka import KafkaConsumer

ORDER_CONFIRMED_KAFKA_TOPIC = 'order_confirmed'

consumer = KafkaConsumer(ORDER_CONFIRMED_KAFKA_TOPIC, bootstrap_servers="localhost:29092")

emails_send_so_far = set()

print("Gonna Listen..")

while True:
    for message in consumer:
        consumed_message = json.loads(message.value.decode())
        customer_email = consumed_message["customer_email"]
        print(f'Sending email to {customer_email}')
        emails_send_so_far.add(customer_email)
        print(f'We have send email to {len(emails_send_so_far)} unique emails')

