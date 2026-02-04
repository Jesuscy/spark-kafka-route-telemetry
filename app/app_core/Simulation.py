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
#Esta versión del código está adaptada para ser utilizada en un entorno de aplicación web, ejecutando las funciones a partir de llamadas HTTP.
#EN
#This version of the code is adapted to be used in a web application environment, executing functions based on HTTP calls.

lock = Lock()

#Creo producer.
free_vehicles = list()
ocupied_vehicles = list()
trips = list()

#Inicia el producer
def inint_producer():
    bootstrap_servers = "localhost:9092"
    topic = "vehicle_positions"
    global kafka_producer
    kafka_producer = KafkaRouteProducer(producer_id="trip_data_producer",bootstrap_servers=bootstrap_servers,topic=topic)

#Creo 50 coches inciales libres.
def create_vehicles(vehicle_id,model):
    vehicle = Vehicle(id=vehicle_id, model=model, state=False)
    free_vehicles.append(vehicle)
    return vehicle

    
def create_trip(id,dest_name,start_lat,start_lon,end_lat,end_lon):
    if not free_vehicles:
        print("No hay vehículos libres disponibles.")
        return
    vehicle = free_vehicles.pop()
    vehicle.state = True 

    route = Route(dest_name=dest_name,start_lat=start_lat,start_lon=start_lon, end_lat=end_lat,end_lon=end_lon)
    trip = Trip(trip_id=id,vehicle=vehicle,route=route)
    ocupied_vehicles.append(vehicle)
    trips.append(trip)
    return trip


def send_trip_data(trip: Trip):
    kafka_producer.send_positions_to_kafka(trip=trip)
    with lock:

        vehicle = trip.get_vehicle()
        vehicle.state = False
        ocupied_vehicles.remove(vehicle)
        free_vehicles.append(vehicle)
        trips.remove(trip)

def start_trip(trip_id):
    trip = next((t for t in trips if t.trip_id == trip_id), None)
    if not trip:
        return {"error": "Trip not found"}
    executor = ThreadPoolExecutor(max_workers=1)
    executor.submit(send_trip_data, trip)
    return {"status": "Trip started"}



