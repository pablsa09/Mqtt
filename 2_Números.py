from paho.mqtt.client import Client
from multiprocessing import Process, Manager
from time import sleep
import random

NUMBERS = 'numbers'
CLIENTS = 'clients'
TIMER_STOP = f'{CLIENTS}/timerstop'
HUMIDITY = 'humidity'
ints = []
floats = []
frecuencias = {}


def int_or_float(x):
    try:
        int(x)
        ints.append(x)
        print(f"The number {int(x)} is an integer")
    except ValueError:
        floats.append(float(x))
        print(f"The number {float(x)} is a float")


def is_prime(n):
    n = int(n)
    i = 2
    while i*i < n and n % i != 0:
        i += 1
    return i*i > n


def frecuencia(x):
    try:
        int(x)
        if int(x) in frecuencias:
            frecuencias[int(x)] += 1
        else:
            frecuencias[int(x)] = 1
        print(f"La frecuencia de {int(x)} es: {frecuencias[int(x)]}")
    except ValueError:
        if float(x) in frecuencias:
            frecuencias[float(x)] += 1
        else:
            frecuencias[float(x)] = 1
        print(f"La frecuencia de {float(x)} es: {frecuencias[float(x)]}")


def timer(time, data):
    mqttc = Client()
    mqttc.connect(data['broker'])
    msg = f'timer working. timeout: {time}'
    print(msg)
    mqttc.publish(TIMER_STOP, msg)
    sleep(time)
    msg = f'timer working. timeout: {time}'
    mqttc.publish(TIMER_STOP, msg)
    print('timer end working')
    mqttc.disconnect()


def on_message(mqttc, data, msg):
    print(f"MESSAGE:data:{data}, msg.topic:{msg.topic}, payload:{msg.payload}")
    try:
        #if is_prime(int(msg.payload)):
        if int(msg.payload) % 2 == 0:
            worker = Process(target=timer,args=(random.random()*20, data))
            worker.start()
    except ValueError as e:
        print(e)
    int_or_float(msg.payload)
    if (msg.payload in ints) and (is_prime(msg.payload)):
        print(f"The number {int(msg.payload)} is prime")
    frecuencia(msg.payload)

    


def on_log(mqttc, userdata, level, string):
    print("LOG", userdata, level, string)


def main(broker):
    data = {'client':None,'broker': broker}
    mqttc = Client(client_id="combine_numbers", userdata=data)
    data['client'] = mqttc
    mqttc.enable_logger()
    mqttc.on_message = on_message
    mqttc.on_log = on_log
    mqttc.connect(broker)
    mqttc.subscribe(NUMBERS)
    mqttc.loop_forever()


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} broker")
        sys.exit(1)
    broker = sys.argv[1]
    main(broker)



















