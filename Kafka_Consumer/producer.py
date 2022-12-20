import json
from logging import log
from kafka import KafkaProducer
from kafka.errors import KafkaError
import requests
from json import dumps
import os
from yaml import parse
bootstrap_servers = ['localhost:9092']
topicName = 'test2'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda K: dumps(data).encode('utf-8'))
# Considered Routes
data = 'BQE N Atlantic Ave - BKN Bridge Manhattan Side',
data1 = '12th Ave S 57th St - 45th S',
data2 = 'HRP N LAFAYETTE AVENUE - E TREMONT AVENUE',
data3 = 'FDR N 25th - 63rd St',
data4 = '11th ave s ganservoort - west st @ spring st'

# Fetch data from APi
URL = 'http://localhost:3000/data'
key = os.getenv('KEYS')
while (True):
    with requests.Session() as s:
        donwload = s.get(URL)
        data = donwload.content.decode('utf-8')
        data = json.loads(data)
        future = producer.send('test2', data)
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            log.exception()
            pass
