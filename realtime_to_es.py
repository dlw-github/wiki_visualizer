from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import uuid
from elasticsearch import Elasticsearch
import re
import ast

es = Elasticsearch([{'host': 'localhost', 'port': 9200, "scheme": "http"}])
# delete index if exists
if es.indices.exists("wiki_realtime"):
    print("Delete existing index")
    es.indices.delete(index="wiki_realtime")

mappings = {
        "properties": {
            "url": {"type": "keyword"},
            "uuid": {"type": "keyword"},
            "dom": {"type": "keyword"},
            "type": {"type": "keyword"},
            "dt": {"type": "date"},
            "user": {"type": "keyword"},
            "title": {"type": "keyword"}
    }
}

es.indices.create(index="wiki_realtime", mappings=mappings)

var = 1
while var == 1 :
    consumer = KafkaConsumer(bootstrap_servers='localhost:9092', group_id='group1',auto_offset_reset='latest')
    consumer.subscribe(['output_topic'])

    for msg in consumer:
        msg_out = {}
        msg_value = msg.value.decode()
        msg_value = ast.literal_eval(msg_value)
        msg_value = json.loads(msg_value)
        doc = {
           "url": msg_value["url"],
           "uuid": msg_value["uuid"],
           "dom": msg_value["dom"],
           "type": msg_value["type"],
           "dt": msg_value["dt"],
           "user": msg_value["user"],
           "title": msg_value["title"],
        }
        res = es.index(index="wiki_realtime", id=msg_value["uuid"], document=doc)
        print('ElasticSearch Response: ', res['result'])