import datetime
import time

def simular_trayecto(trayecto):

    coordenadas_ruta = trayecto["routes"][0]["geometry"]["coordinates"]
    duracion = trayecto["duration"]
    distancia = trayecto["distance"]
    
    for i in range(0, len(coordenadas_ruta), 4):
        lat, long = coordenadas_ruta[i]
        progreso = i / len(coordenadas_ruta) * 100
        str_progreso = f"{progreso:.2f} %"

        report_msg = {
            "latitud": lat,
            "longitud": long,
            "progreso": str_progreso,
            "time_stamp" : datetime.datetime.now()
        }     

        yield report_msg
        time.sleep(0.1)
        
