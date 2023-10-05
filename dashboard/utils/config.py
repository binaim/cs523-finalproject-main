import os


HBASE_URL = os.getenv("HBASE_URL", "localhost")
KAFKA_URL = os.getenv("KAFKA_URL", "localhost:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "dashboard-dev")
THREAD_DAEMON = os.getenv("THREAD_DAEMON", "true") == "true"
