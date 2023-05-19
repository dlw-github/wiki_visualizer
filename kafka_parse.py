from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import uuid


var = 1
while var == 1 :
    consumer = KafkaConsumer(bootstrap_servers='localhost:9092', group_id='group1',auto_offset_reset='latest')
    consumer.subscribe(['test_topic'])

    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))


    for msg in consumer:
        msg_out = {}
        msg_value = msg.value.decode()
        msg_dict = json.loads(msg_value)
        msg_out['url'] = msg_dict['meta']['uri']
        msg_out['uuid'] = str(uuid.uuid4())
        msg_out['dom'] = msg_dict['meta']['domain']
        msg_out['type'] = msg_dict['type']
        msg_out['dt'] = msg_dict['meta']['dt']
        msg_out['user'] = msg_dict['user']
        msg_out['title'] = msg_dict['title']
        msg_out['date_hour'] = msg_dict['meta']['dt'][0:13]

        msg_out = json.dumps(msg_out)
        print(msg_out)
        print()



        producer.send('output_topic', msg_out)






    # byte_message = str.encode(message)
    # producer.send('test_output' ,value=byte_message, key=byte_message)



if __name__ == '__main__':
