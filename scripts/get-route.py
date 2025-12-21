import os
from dotenv import load_dotenv
import requests
import json

latitud_origen = 37.9439891
longitud_origen = -1.1636353

latitud_destino = 39.4561165
longitud_destino = -0.3545661

load_dotenv()

def route_request(lat_o,long_o,lat_d,long_d):

    url = (f"{os.getenv("API_URL")}"
        f"{latitud_origen},{longitud_origen};{latitud_destino},{longitud_destino}"
        f"?overview=full&geometries=geojson"
    )

    try:
        # response = requests.get(url)
        # if response.status_code != 200:
        #     raise Exception(f"Error en la llamada: {response.status_code}")
        # data = response.json()

        with open(f"response_example.json","r") as respuesta:
            respuesta = respuesta.read()
            json_respuesta = json.loads(respuesta)

        return json_respuesta

    except Exception as e:
        print(f"Error obteniendo ruta: {e}")

        

