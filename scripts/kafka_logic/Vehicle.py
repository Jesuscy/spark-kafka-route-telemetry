import Route
from datetime import datetime

class Vehicle:

    def __init__(self, id, model, route: Route):
        self.id = id
        self.model = model
        self.route = route
    
    def get_route(self):

        route_data = self.route.route_request()
        return route_data
    
    
    def simulate_route(self, route_data, jump=4):
        positions = []
        coordinates = route_data["routes"][0]["geometry"]["coordinates"]

        total_steps = (len(coordinates) + jump - 1) // jump
        last_progress = None

        for step, i in enumerate(range(0, len(coordinates), jump)):

            lon, lat = coordinates[i]
            progress = int((step / (total_steps - 1)) * 100)

            if progress != last_progress:

                positions.append({
                    "vehicle_id": self.id,
                    "model": self.model,
                    "latitude": lat,
                    "longitude": lon,
                    "data_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "progress": f"{progress}%"
                })

                last_progress = progress
        
        return positions

    