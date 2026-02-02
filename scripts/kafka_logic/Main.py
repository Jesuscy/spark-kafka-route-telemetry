from concurrent.futures import ThreadPoolExecutor
from Producer import KafkaRouteProducer
from Vehicle import Vehicle
from Route import Route

def main():
    
    #Creo producer.
    bootstrap_servers = "localhost:9092"
    topic = "vehicle_positions"
    vehicles = list()

    kafka_producer = KafkaRouteProducer(producer_id="trip_data_producer",bootstrap_servers=bootstrap_servers,topic=topic)
    
    def create_drive(id,model,dest_name,start_lat,start_lon,end_lat,end_lon):
        route = Route(dest_name=dest_name,start_lat=start_lat,start_lon=start_lon, end_lat=end_lat,end_lon=end_lon)
        vehicle = Vehicle(id=id,model=model,route=route)
        vehicles.append(vehicle)
        
    def send_drive_data(vehicle):
            route_data = vehicle.get_route()
            positions = vehicle.simulate_route(route_data)
            kafka_producer.send_positions_to_kafka(positions)

    create_drive(
        id="vehicle_1",
        model="Model X",
        dest_name="Destino A",
        start_lat=40.4168,
        start_lon=-3.7038,
        end_lat=40.4319,
        end_lon=-3.6920
    )

    with ThreadPoolExecutor(max_workers=50) as executor:
        for vehicle in vehicles:
            executor.submit(send_drive_data, vehicle)

    kafka_producer.producer.flush()



            
if __name__ == "__main__":
    main()
    