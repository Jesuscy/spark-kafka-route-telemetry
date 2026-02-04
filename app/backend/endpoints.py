from fastapi import FastAPI, WebSocket
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app_core import Simulation as simulation
import asyncio
import json

app = FastAPI()

@app.on_event("startup")
def startup_event():
    simulation.init_producer(bootstrap_servers="kafka:9092")

@app.post("/vehicles")
def api_create_vehicle(vehicle_id: str, model: str):
    return simulation.create_vehicle(vehicle_id, model)

@app.post("/trips")
def api_create_trip(trip_id: str, vehicle_id: str, dest_name: str,
                    start_lat: float, start_lon: float, end_lat: float, end_lon: float):
    return simulation.create_trip(trip_id, dest_name, start_lat, start_lon, end_lat, end_lon)

@app.post("/trips/{trip_id}/start")
def api_start_trip(trip_id: str):
    return simulation.start_trip(trip_id)