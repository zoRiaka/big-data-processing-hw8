from time import sleep
from kafka import KafkaProducer, KafkaConsumer
import csv
from json import dumps
from datetime import date, timedelta
import sys
import random

producer = KafkaProducer(bootstrap_servers='kafka-server:9092', api_version=(2, 0, 2),
                         value_serializer=lambda x: dumps(x).encode('utf-8'))


def get_random_date():
    start_date = date.today()
    end_date = start_date - timedelta(days=30)

    random_number_of_days = random.randrange(0, 30)
    random_date = start_date - timedelta(days=random_number_of_days)
    return random_date.strftime("%d/%m/%Y")


with open('/opt/app/dataset.csv') as f:
    reader = csv.DictReader(f, delimiter=',')
    try:
        for row in reader:
            row['transaction_date'] = get_random_date()
            result = producer.send('my-topic', str(row))
            sleep(0.1)
        # producer.send('tweets', {'created_at': 'end', 'text': 'end', 'author_id': 'end'})
        producer.flush()
    except KeyboardInterrupt:
        sys.exit(0)
