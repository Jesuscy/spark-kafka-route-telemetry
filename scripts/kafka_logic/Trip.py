from datetime import datetime
from Vehicle import Vehicle
from Route import Route

class Trip:

    def __init__(self, trip_id, vehicle=Vehicle, route=Route):
        self.trip_id = trip_id
        self.vehicle = vehicle
        self.route = route
        
    def get_vehicle(self):
        return self.vehicle
    
    def get_route(self):
        return self.route
    
    def get_route_data(self):
        return self.route.route_request()


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
                    "vehicle_id": self.vehicle.get_id(),
                    "model": self.vehicle.get_model(),
                    "latitude": lat,
                    "longitude": lon,
                    "data_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "progress": f"{progress}%"
                })

                last_progress = progress
        
        return positions

    