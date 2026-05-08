import json
import os
import time
from ensurepip import bootstrap

import pandas as pd
from confluent_kafka import Producer

load_dotenv()
bootstrap_server = os.getenv("CONFLUENT_BOOTSTRAP_SERVER")
conf = {
    "bootstrap.servers": "SERVER_BOOTSTRAP",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "API",
    "sasl.password": "API_SECRET",
}

topic_name = "network_traffic"

p = Producer(conf)


def deliver_report(err, msg):
    if err is not None:
        print(f"Falha no envio {err}")
    else:
        print(f"Enviadno para {msg.topic()} [{msg.partition()}]")
    csv_path = "./data/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv"
