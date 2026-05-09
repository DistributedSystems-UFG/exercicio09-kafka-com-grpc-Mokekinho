from kafka import KafkaProducer
from const import *
import sys

import random
import time

try:
    topic = "temperatura"
        
    producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])


    while True:
        temperatura = random.randint(30, 65)

        msg = f'A temperatura atual é: {temperatura} '  + '; st message for topic ' + topic
        print ('Sending message: ' + msg)
        producer.send(topic, value=msg.encode())

        
        time.sleep(10) # pausa a execução por 10 segundos

except KeyboardInterrupt:
    print("\nInterrupção detectada! Finalizando o producer...")

finally:
    producer.flush()
    print("Dados enviados e conexão encerrada com sucesso.")