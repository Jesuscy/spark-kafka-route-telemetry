from confluent_kafka import Producer
import json


class KafkaRouteProducer:

    def __init__(self, producer_id, bootstrap_servers, topic, config_extra=None):
        self.producer_id = producer_id
        self.topic = topic

        config = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": producer_id
        }

        if config_extra:
            config.update(config_extra)

        self.producer = Producer(config)

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"ERROR entregando mensaje: {err}")
        else:
            print(
                f"Enviado a {msg.topic()} "
                f"[partition {msg.partition()}], offset {msg.offset()}"
            )

    def send_positions_to_kafka(self, generator):

        for position in generator:
            self.producer.produce(
                topic=self.topic,
                key=str(position["progress"]).encode("utf-8"),
                value=json.dumps(position).encode("utf-8"),
                callback=self.delivery_report
            )
            self.producer.poll(0)

        self.producer.flush()
