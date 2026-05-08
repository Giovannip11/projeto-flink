import json
import os
import time
from ensurepip import bootstrap

import pandas as pd
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()
conf = {
    "bootstrap_server": os.getenv("CONFLUENT_BOOTSTRAP_SERVER"),
    "bootstrap.servers": "SERVER_BOOTSTRAP",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "API",
    "sasl.password": "API_SECRET",
}

topic_name = "network_traffic"

p = Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"Falha no envio {err}")
    else:
        print(f"Enviadno para {msg.topic()} [{msg.partition()}]")


csv_path = "./data/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv"

if not os.path.exists(csv_path):
    print(f"ERRO: Arquivo {csv_path} não encontrado!")
else:
    print("Carregando dados...")
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip()
    print(f"Iniciando streaming de {len(df)} linhas...")

    for index, row in df.iterrows():
        payload = row.to_dict()
        p.produce(
            topic_name,
            value=json.dumps(payload).encode("utf-8"),
            callback=delivery_report,
        )
        if index % 100 == 0:
            p.poll(0)
            p.flush()
            print(f"Enviadas {index} linhas...")
            time.sleep(0.5)
    p.flush()
