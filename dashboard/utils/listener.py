from threading import Thread
from confluent_kafka import Consumer
import json
from collections import deque


class EventListener:
    def __init__(self, on_new_event) -> None:
        self.on_new_event = on_new_event
        self.queue = deque()

    def add_value(self, value):
        self.queue.append(value)

    def consume(self):
        if len(self.queue) > 0:
            value = self.queue.popleft()
            data = json.loads(value)
            if len(data) > 0:
                self.on_new_event(data)

        # msg = self.c.poll(1.0)
        # while msg is None:
        #     msg = self.c.poll(1.0)
        # if msg is None:
        #     print("None Message")
        #     return
        # if msg.error():
        #     print("Consumer error: {}".format(msg.error()))
        #     return

        # key = msg.key().decode("utf-8") if msg.key() is not None else ""
        # value = msg.value().decode("utf-8")

        # print("Received message: {} ;; {}".format(key, value))

        # if key == "event_type_agg":
        #     data = json.loads(value)
        #     if len(data) > 0:
        #         self.on_new_event(json.loads(value))
