from confluent_kafka import Producer
import json

def delivery_report(err, msg):
    if err is not None:
        print(f"ERROR entregando mensaje: {err}")
    else:
        print(f"Enviado a {msg.topic()} [partition {msg.partition()}], offset {msg.offset()}")


def create_producer(bootstrap_servers="localhost:9092"):
    config = {"bootstrap.servers": bootstrap_servers}
    return Producer(config)


def send_positions_to_kafka(producer, topic, generator):
  
    for position in generator:
        producer.produce(
            topic,
            key=str(position["progreso"]), 
            value=json.dumps(position),
            callback=delivery_report
        )
        producer.poll(0)
    
    producer.flush()
