from paho.mqtt.client import Client
from multiprocessing import Process, Manager
from time import sleep
import paho.mqtt.publish as publish
import time

def get_topic(msg):
    topic = ''
    i = 0
    while msg[i] != ' ':
        topic += msg[i]
        i += 1
    return topic 
    
def get_timeout(msg):
    timeout = ''
    i = 0
    while msg[i] != ' ':
        i += 1
    i += 1
    while msg[i] != ' ':
        timeout += msg[i]
        i += 1
    return timeout
    
def get_text(msg):
    text = ''
    i = 0
    n_espacios = 0
    while n_espacios < 2:
        if msg[i] == ' ' :
            n_espacios += 1
        i += 1
    while i < len(msg):
        text += msg[i]
        i += 1
    return text  

def on_message(mqttc, data, msg):
    print(f"MESSAGE:data:{data}, msg.topic:{msg.topic}, payload:{msg.payload}")
    topic = 'clients/timeout'
    #Los mensajes en topic deben ser del tipo "topic_name timeout message"
    message = msg.payload.decode()
    topic_read = get_topic(message)
    timeout = get_timeout(message)
    text = get_text(message)
    print("Contador iniciado")
    sleep(int(timeout))
    print("Fin del contador")
    topic = 'clients/timeout'
    mqttc.publish(topic, f'Publicando el mensaje {text} desde {topic_read}')


def on_log(mqttc, userdata, level, string):
    print("LOG", userdata, level, string)

def main(broker):
    data = {'status':0}
    mqttc = Client(userdata=data)
    mqttc.enable_logger()
    mqttc.on_message = on_message
    mqttc.on_log = on_log
    mqttc.connect(broker)

    topic = 'clients/a'

    mqttc.subscribe(topic)

    mqttc.loop_forever()


if __name__ == "__main__":
    import sys
    if len(sys.argv)<2:
        print(f"Usage: {sys.argv[0]} broker")
        sys.exit(1)
    broker = sys.argv[1]
    main(broker)
    
    
    
    
    
    
    
 
    
    
    
    
