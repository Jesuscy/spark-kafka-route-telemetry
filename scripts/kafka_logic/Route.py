import os
import json
from dotenv import load_dotenv

load_dotenv()

class Route: 
    
    def __init__(self, dest_name, start_lat, start_lon, end_lat, end_lon):
        
        self.dest_name = dest_name
        self.start_lat = start_lat
        self.start_lon = start_lon
        self.end_lat = end_lat
        self.end_lon = end_lon

    def route_request(self):

        url = (f"{os.getenv("API_URL")}"
            f"{self.start_lat},{self.start_lon};{self.end_lat},{self.end_lon}"
            f"?overview=full&geometries=geojson"
        )

        try:
            # response = requests.get(url)
            # if response.status_code != 200:
            #     raise Exception(f"Error en la llamada: {response.status_code}")
            # data = response.json()
            # return json_response


            with open(f"response_example.json","r") as response:
                response = response.read()
                json_response = json.loads(response)
                
            return json_response

        except Exception as e:
            print(f"Error obteniendo ruta: {e}")
