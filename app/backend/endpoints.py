from fastapi import FastAPI, WebSocket
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app_core import Simulation as simulation
import asyncio
import json

app = FastAPI()

#Aquí la api esta apuntando a rutas que llaman a las funciones, buttons en el frontend deben llamar a estas rutas.

@app.on_event("startup")
def startup_event():
    simulation.init_producer()

@app.on_event("shutdown")
def shutdown_event():
    simulation.shutdown_producer()

@app.post("/vehicles")
def api_create_vehicles():
    return simulation.create_vehicles()

@app.post("/trips")
def api_create_trip(trip_id: str, dest_name: str,
                    start_lat: float, start_lon: float, end_lat: float, end_lon: float):
    return simulation.create_trip(trip_id, dest_name, start_lat, start_lon, end_lat, end_lon)

@app.post("/trips/{trip_id}/start")
def api_start_trip(trip_id: str):
    return simulation.start_trip(trip_id)