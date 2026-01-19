import Route
from datetime import datetime

class Vehicle:

    def __init__(self, id, model, route: Route):
        self.id = id
        self.model = model
        self.route = route
    
    def get_route(self):

        route_data = self.route.Route.route_request(self.route.start_lat,
                                                    self.route.start_lon, 
                                                    self.route.end_lat,
                                                    self.route.end_lon)
        return route_data
    
    
    def simulate_route(self, route_data, paso=4):
        posiciones = []
        coordinates = route_data["routes"][0]["geometry"]["coordinates"]
        for i in range(0, len(coordinates), paso):
            lon, lat = coordinates[i]
            posiciones.append({
                "vehicle_id": self.id,
                "model": self.model,
                "latitude": lat,
                "longitude": lon,
                "data_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "progreso": f"{(i / len(coordinates)) * 100}%"
            })
        
        return posiciones

    