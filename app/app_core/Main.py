from queue import Queue
import threading
from Producer import KafkaRouteProducer
from Vehicle import Vehicle
from Route import Route
from Trip import Trip   
from concurrent.futures import ThreadPoolExecutor
from random import choice
from threading import Lock
#ES
#La idea de esto es que en algún momento haya una app desde la que crear los viajes, de momento los simulo para priorizar la parte de 
#envío y porcesamiento de datos del proyecto, por eso el enfoque puede parecer extraño.

#EN
#The idea is that at some point there will be an app from which to create the trips, for now I simulate them to prioritize the sending and processing of project data,
#that's why the approach may seem strange.

def main():

    lock = Lock()

    #Creo producer.
    bootstrap_servers = "localhost:9092"
    topic = "vehicle_positions"
    free_vehicles = list()
    ocupied_vehicles = list()
    trips = list()

    kafka_producer = KafkaRouteProducer(producer_id="trip_data_producer",bootstrap_servers=bootstrap_servers,topic=topic)
  
    #Creo 50 coches inciales libres.
    def create_vehicles():
        vehicle_names = ["Opel Corsa","Ford Fiesta","Toyota Corolla","Honda Civic","Mercedes C220","BMW Serie 3","Audi A4","Volkswagen Golf","Renault Clio","Peugeot 208"]
        for i in range(50):
            vehicle = Vehicle(id=f"vehicle_id_{i}",model=choice(vehicle_names), state=False)
            free_vehicles.append(vehicle)
    

    
    def create_trip(id,dest_name,start_lat,start_lon,end_lat,end_lon):
        if not free_vehicles:
            print("No hay vehículos libres disponibles.")
            return
        vehicle = free_vehicles.pop()
        vehicle.state = True 

        #Esta ruta la metería el ususario
        route = Route(dest_name=dest_name,start_lat=start_lat,start_lon=start_lon, end_lat=end_lat,end_lon=end_lon)
        trip = Trip(trip_id=id,vehicle=vehicle,route=route)
        ocupied_vehicles.append(vehicle)
        trips.append(trip)

    def send_trip_data(trip: Trip):
        kafka_producer.send_positions_to_kafka(trip=trip)
        with lock:

            vehicle = trip.get_vehicle()
            vehicle.state = False
            ocupied_vehicles.remove(vehicle)
            free_vehicles.append(vehicle)
            trips.remove(trip)

    #Creo los coches:
    create_vehicles()
    #Creo algunos viajes de ejemplo:
    create_trip(id="trip_1",dest_name="Airport",start_lat=40.4168,start_lon=-3.7038,end_lat=40.4893,end_lon=-3.5676)
    create_trip(id="trip_2",dest_name="Central Station",start_lat=41.4168,start_lon=-3.7038,end_lat=40.3966,end_lon=-3.6981)
    create_trip(id="trip_3",dest_name="City Center",start_lat=42.4168,start_lon=-3.7038,end_lat=40.4183,end_lon=-3.7074)


    #Envio los datos de los viajes concurrentemente:
    with ThreadPoolExecutor(max_workers=50) as executor:
        for trip in trips:
            executor.submit(send_trip_data, trip)

    kafka_producer.flush()



            
if __name__ == "__main__":
    main()
    