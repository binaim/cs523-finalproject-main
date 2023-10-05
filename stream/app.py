import random
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import csv
import json
import time

producer = Producer({"bootstrap.servers": "kafka:29092"})
TOPIC = "electronic-store"
OUT_TOPIC = "electronic-analytics"


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.value().decode("utf-8")))


def create_topics():
    admin_client = AdminClient({"bootstrap.servers": "kafka:29092"})
    topic_list = [NewTopic(x) for x in [TOPIC, OUT_TOPIC]]
    admin_client.create_topics(topic_list)
    # print(admin_client.list_topics().topics)


create_topics()
# exit(0)

with open("dataset.csv", newline="") as csvfile:
    reader = csv.DictReader(csvfile)

    producer.produce(OUT_TOPIC, "{}".encode("utf-8"), callback=delivery_report)

    for row in reader:
        producer.poll(0)
        msg = json.dumps(row)
        producer.produce(TOPIC, msg.encode("utf-8"), callback=delivery_report)
        time.sleep(0.1)
