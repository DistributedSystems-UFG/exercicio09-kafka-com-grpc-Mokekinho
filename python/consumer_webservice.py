from kafka import KafkaConsumer
from const import *
import sys

import grpc
from concurrent import futures
import time
import threading
from kafka import KafkaConsumer

import SensorService_pb2, SensorService_pb2_grpc

ultima_msg = "Nenhuma mensagem recebida ainda"

def kafka_worker():
    global ultima_msg

    consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT], auto_offset_reset='earliest')


    topic = "temperaturaServer"
    

    consumer.subscribe([topic])
    for msg in consumer:
        ultima_msg = msg.value.decode('utf-8')
        print(f"Kafka recebeu: {ultima_msg}")

class SensorServicer(SensorService_pb2_grpc.SensorServiceServicer):
    def GetUltimaTemperatura(self, request, context):
        return SensorService_pb2.StringResponse(mensagem=ultima_msg)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    SensorService_pb2_grpc.add_SensorServiceServicer_to_server(SensorServicer(), server)
    server.add_insecure_port('[::]:'+PORT)
    print(f"Servidor gRPC rodando na porta {PORT}")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    
    t = threading.Thread(target=kafka_worker, daemon=True)
    t.start()

    # Inicia o gRPC na thread principal
    try:
        serve()
    except KeyboardInterrupt:
        print("Encerrando...")
