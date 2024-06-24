#!/usr/bin/env python
import pika
import random, os, time, json, sys
import numpy as np
import pandas as pd

class NumpyEncoder(json.JSONEncoder):
    '''
    A Util class for Json#load and Json#dumps funtions
    '''
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def push_data_to_RMQ(sensor_type='DEFAULT', rmqhost="localhost", rmqqueue="hello"):
    '''
    Supported Sensor Types: DISTANCE, SONAR, IRIS, DEFAULT
    
    RabbitMQ Queue Names : DISTANCE, SONAR, IRIS, DEFAULT
    '''
    #credentials = pika.PlainCredentials('guest', 'guest')
    #parameters = pika.ConnectionParameters('rabbit-server1', 5672, '/', credentials)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rmqhost))
    channel = connection.channel()

    channel.queue_declare(queue=rmqqueue, durable=True)

    if(sensor_type == 'DISTANCE'):
        time.sleep(1)
        sensor_data = generate_distance_data()
        
        input = {"data":sensor_data}
        json_input = json.dumps(input)
        channel.basic_publish(exchange='', routing_key='hello', body=json_input)
    elif(sensor_type == 'SONAR'):
        time.sleep(10)
        sensor_data = generate_sonar_data()

        input = {"data":sensor_data}
        json_input = json.dumps(input, cls=NumpyEncoder)
        channel.basic_publish(exchange='', routing_key='hello', body=json_input)
    elif(sensor_type == 'IRIS'):
        time.sleep(0.5)
        sensor_data = generate_iris_data()

        input = {"data":sensor_data}
        json_input = json.dumps(input, cls=NumpyEncoder)
        channel.basic_publish(exchange='', routing_key='hello', body=json_input)
    else:
        pass

    print("Data Sent..")
    connection.close()

def generate_distance_data():
    '''
    90% of the values are between (201, 1000) and 10% of the values are between (0,200)
    Rate : one value every second
    '''
    distribution = random.randint(1,10)
    if(distribution == 1):
        return random.randint(0, 200)
    else:
        return random.randint(201, 1000)
    
def generate_sonar_data():
    '''
    Sonar Data([60*1] vector)
    Rate : Every 10 seconds
    '''
    df = pd.read_csv("./sonar.all-data")
    X = df[df.columns[0:60]].values
    distrinution = random.randint(0, X.shape[0]-1)
    return X[distrinution]

def generate_iris_data():
    '''
    Iris Data([4*1] vector)
    Rate : Every 0.5 second
    '''
    df = pd.read_csv("./iris.data")
    X = df[df.columns[0:4]].values
    distrinution = random.randint(0, X.shape[0]-1)
    return X[distrinution]

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print('Please provide RabbitMQ machine IP Address as first command line argument')
        sys.exit()
    if  len(sys.argv) < 3:
        print('Please provide 2nd argument as dataset type. Example : DISTANCE, SONAR, IRIS')
        sys.exit()

    RMQ_hostname = sys.argv[1]
    dataset_type = sys.argv[2]
    try:
        while(True):
            push_data_to_RMQ(sensor_type='IRIS')
    except KeyboardInterrupt as identifier:
        print("Sensor Stopped by User")