from time import sleep
from json import dumps
from kafka import KafkaProducer
import configparser

cfg = configparser.ConfigParser()
cfg.read('./config.ini')
bs = cfg.get('default','bootstrap_servers').split(',')
dest_topic = cfg.get('default','topic')

producer = KafkaProducer(bootstrap_servers = bs, value_serializer=lambda x:dumps(x).encode('utf-8'), retries=5)

for e in range(10,100):
    data = {'number' : e}
    producer.send(dest_topic, value=data)
    print(data)
    sleep(.01)