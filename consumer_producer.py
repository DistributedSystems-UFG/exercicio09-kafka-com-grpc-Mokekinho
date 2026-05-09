from kafka import KafkaConsumer
from kafka import KafkaProducer
from const import *
import sys

# Create consumer: Option 1 -- only consume new events
#consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT]) # começa lendo apenas as mensagens que chegarem ao vivo

try:
  # Create consumer: Option 2 -- consume old events (uncomment to test -- and comment Option 1 above)
  consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT], auto_offset_reset='earliest') # vai ler todas as mensagens e quando chegar no ao vivo ai ficar esperando 


  # Vou criar um producer pro meu consumidor postar em um topico
  consumer_producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])


  topic_to_hear = "temperatura"
  topic_to_send = "temperaturaServer"

  consumer.subscribe([topic_to_hear]) #liga o consumidor a um topico 
  for msg in consumer:
      print (msg.value)


      print(f"Mensagem enviada para tópico {topic_to_send}")

      new_msg = msg.value + f'. Agora pelo topico {topic_to_send}'.encode()

      consumer_producer.send(topic_to_send, value = new_msg)
      

      #print(msg.value.decode('utf-8'), flush=True)
except KeyboardInterrupt:
    print("\nInterrupção detectada! Finalizando o producer...")

finally:
    consumer_producer.flush()
    print("Dados enviados e conexão encerrada com sucesso.")