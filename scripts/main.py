from route_request import route_request
from simulador import simular_trayecto
from producer import create_producer, send_positions_to_kafka

if __name__ == "__main__":

    #Obtener ruta
    trayecto = route_request(
        37.9439891,
        -1.1636353,
        39.4561165,
        -0.3545661
    )

    #Simular recorrer ruta
    posiciones = simular_trayecto(trayecto, paso=4, delay=0.5)

    #Creo el producer empiezo a mandar los datos de navegación simulados.
    producer = create_producer("localhost:9092")
    send_positions_to_kafka(producer, "car_positions", posiciones)
