from confluent_kafka import Producer
import json
from scripts.producer import delivery_report

class KafkaProducer:

    def __init__(self, id, bootstrap_servers, topic, config_extra=None):
        self.id = id
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.config_extra = config_extra or {}
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            **self.config_extra
        )

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"ERROR entregando mensaje: {err}")
        else:
            print(f"Enviado a {msg.topic()} [partition {msg.partition()}], offset {msg.offset()}")


    def create_producer(self, bootstrap_servers):
        config = {"bootstrap.servers": bootstrap_servers}
        return Producer(config)


    def send_positions_to_kafka(self,generator):
    
        for position in generator:
            self.producer.produce(
                self.topic,
                key=str(position["progreso"]), 
                value=json.dumps(position),
                callback=self.delivery_report
            )
            self.producer.poll(0)
        
        self.producer.flush()
