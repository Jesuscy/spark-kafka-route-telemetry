import Producer
import Vehicle
import Route

def main():
    
    #Creo producer.
    bootstrap_servers = "localhost:9092"
    topic = "vehicle_positions"

    kafka_producer = Producer.KafkaRouteProducer(id="producer_1",bootstrap_servers=bootstrap_servers,topic=topic)
    
    #Creo ruta y vehiculo
    route = Route.Route(dest_name="Destino A",start_lat=40.4168,start_lon=-3.7038, end_lat=40.4319,end_lon=-3.6920)
    vehicle = Vehicle.Vehicle(id="vehicle_1",model="Model X",route=route)

    #Obtengo ruta y simulo posiciones
    route_data = vehicle.get_route()
    posiciones = vehicle.simulate_route(route_data, paso=5)
    kafka_producer.send_positions_to_kafka(posiciones)



if __name__ == "__main__":
    main()