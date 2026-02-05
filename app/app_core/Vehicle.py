import Route
from datetime import datetime

class Vehicle:

    def __init__(self, id, model, state=False):
        self.id = id
        self.model = model
        self.state = state
    
    def get_id(self):
        return self.id

    def get_model(self):
        return self.model  
    
    def get_state(self):
        return self.state   
    
    
    