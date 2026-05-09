from __future__ import print_function
import logging

import grpc

import SensorService_pb2, SensorService_pb2_grpc
import const

def run():
    with grpc.insecure_channel(const.IP+':'+const.PORT) as channel:
        stub = SensorService_pb2_grpc.SensorServiceStub(channel)

        
        response = stub.GetUltimaTemperatura(SensorService_pb2.Empty())
        print ('Temperatura Resgata do servidor: ' + str(response))


if __name__ == '__main__':
    logging.basicConfig()
    run()