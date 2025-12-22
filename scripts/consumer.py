from kafka import KafkaConsumer
import time 
import json 

def start_consumer(topic_name = "posicion_vehiculo", bootstrap_servers = "localhost:9092", group_id="debug_consumer"):
    
    print(f'Iniciando consumer topic: {topic_name}')

    consumer = KafkaConsumer(
        topic_name = topic_name,
        bootstrap_servers = bootstrap_servers,
        group_id = group_id,
        auto_offset_reset = 'earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    ) 

    try: 
        for message in consumer:
            print("Mensaje recibido:")
            print(f'Mensaje offset: {message.offset}')
            print(f'Mensaje partition: {message.partition}')
            print(f'Mensaje value: {message.value}')

        
    except Exception as e:
        print(f'Error Kafka consumer: {e}')

    finally:

        consumer.close()
        print("Consumer cerrado.")