import os
from dotenv import load_dotenv
import requests
import json


load_dotenv()

def route_request(lat_o,long_o,lat_d,long_d):

    url = (f"{os.getenv("API_URL")}"
        f"{lat_o},{long_o};{lat_d},{long_d}"
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

        

