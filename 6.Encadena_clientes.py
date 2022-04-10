from paho.mqtt.client import Client
from multiprocessing import Process, Manager, Lock
from time import sleep
import paho.mqtt.publish as publish
import time

NUMBERS = 'numbers'
CLIENTS = 'clients'
HUMIDITY = 'humidity'
ints = []
floats = []


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

def temp_media(data):
    temps = []    
    while len(temps) < 5:
        sleep(3)
        print(data['temp'].items())
        for key,temp in data['temp'].items():
            temps += temp
            data[key]=[]
        mean = sum(map(lambda x: int(x), temps))/len(temps)
        print(f'La temperatura media en todos los sensores es: {mean}')
        return mean

def get_list(d):
    #Función auxiliar que se usa para calcular la lista de data['temp'] más adelante
    l = []
    for i in d:
        l = d[i]
    return l



def on_message1(mqttc, data, msg):
    print(f"MESSAGE:data:{data}, msg.topic:{msg.topic}, payload:{msg.payload}")
    """
    El cliente 1 lee un número desde numbers y si es primo pone una alarma
    """
    int_or_float(msg.payload)
    if (msg.payload in ints) and (is_prime(msg.payload)):
        print(f"The number {int(msg.payload)} is prime")
        topic = 'clients/timeout'
        print(f"Contador de {int(msg.payload)} segundos iniciado")
        sleep(int(msg.payload))
        print("Fin del contador")
        topic = 'clients/timeout'
        mqttc.publish(topic, f'Se puso una alarma de {int(msg.payload)} segundos')
        
def on_message2(mqtcc, data, msg):
    print(f"MESSAGE:data:{data}, msg.topic:{msg.topic}, payload:{msg.payload}")
    """
    El cliente 2 calcula la temperatura media de todos los sensores leyendo las 4
    primeras temperaturas. Luego lee un número de numbers y calcula la media entre
    la temperatura y el número. Por último redondea la media y pone una alarma con esa media.
    """
    print ('on_message', msg.topic, msg.payload)
    n = len('temperature/')
    lock = data['lock']
    lock.acquire()
    try:
        n_items = len(get_list(data['temp']))
        if n_items < 4:
            key = msg.topic[n:]
            if key in data:
                data['temp'][key].append(msg.payload)
            else:
                data['temp'][key]=[msg.payload]
    finally:
        lock.release()
    mean = temp_media(data)
    mqtcc.unsubscribe('temperature')
    mqtcc.subscribe('numbers')
    num = msg.payload
    int_or_float(num)
    if num in ints:
        num = int(num)
    else:
        num = float(num)
    
    mean2 = (mean+num)/2
    timer = round(mean2)
    if n_items >= 4:
        print(f"Contador de {timer} segundos iniciado")
        sleep(timer)
        print("Fin del contador")
        mqtcc.publish('clients/timeout', f'Se puso una alarma de {timer} segundos')
        
        
def on_log(mqttc, userdata, level, string):
    print("LOG", userdata, level, string)


def main(broker):
    data1 = {'client':None,'broker': broker}
    data2 = {'lock':Lock(), 'temp':{}}
    mqttc1 = Client(client_id="cliente1", userdata=data1)
    mqttc2 = Client(client_id="cliente2", userdata=data2)
    data1['client'] = mqttc1
    data2['client'] = mqttc2
    
    mqttc1.enable_logger()
    mqttc1.on_message = on_message1
    mqttc1.on_log = on_log
    mqttc1.connect(broker)
    mqttc1.subscribe('numbers')
    mqttc1.loop_start()
    
    mqttc2.enable_logger()
    mqttc2.on_message = on_message2
    mqttc2.on_log = on_log
    mqttc2.connect(broker)
    mqttc2.subscribe('temperature')
    mqttc2.loop_forever()

   


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} broker")
        sys.exit(1)
    broker = sys.argv[1]
    main(broker)
