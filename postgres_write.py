import psycopg2
import time
from opensky_api import OpenSkyApi
import json


def generate_OpenSky_data():
    '''
    Summary:
        - Get a list of the statevectors for all the current aircraft on OpenSky
    Inputs:
        - None
    Returns:
        - all_aircraft_states
    '''

    # Instantiate the OpenSky API class
    api = OpenSkyApi()

    states_instance = api.get_states() # <-- would contain bbox to define area

    all_aircraft_states = states_instance.states

    return all_aircraft_states

def format_opensky_for_consumer(all_aircraft_states):
    '''
    Summary:
        - Take in the opensky state vectors and format them into strings, and then bytecode
    Inputs:
        - all_aircraft_states :: exact output from generate_OpenSky_data()
    Returns:
        - 
    '''

    '''
    Want a dict of form {'flight_ID' : {'lat' : lat, 'long' : long } , ... }
    '''

    my_dict = {}

    for flight in all_aircraft_states:
        if (flight.latitude is not None) and (flight.longitude is not None):
            my_dict[str(flight.icao24)] = { 'lat' : flight.latitude, 'lon' : flight.longitude }

    return my_dict

def main():

    first_time = False

    # conn.autocommit = True

    if first_time:
        try:
            conn = psycopg2.connect(database="hackathon", user='postgres', password='MainDevAdmin1', host='127.0.0.1', port='5432')
        except:
            print("Could not connect")

        curr = conn.cursor()

        print('PostgreSQL database version:')
        curr.execute('SELECT version()')

        # Drop table if it exists:
        curr.execute("DROP TABLE IF EXISTS adsb")

        sql = '''CREATE TABLE adsb(
            icao_id CHAR(255) NOT NULL,
            lat FLOAT,
            lon FLOAT
        )'''

        curr.execute(sql)
        print("Created new table")
        conn.close()

    
    # Begin ADSB loop
    a = 1
    while a < 2:

        try:
            conn = psycopg2.connect(database="hackathon", user='postgres', password='MainDevAdmin1')
            curr = conn.cursor()
        except:
            print("Could not connect")

        all_aircraft_states = generate_OpenSky_data()
        my_dict = format_opensky_for_consumer(all_aircraft_states)
        print("ADSB received ...")

        # Loop over each individual entry, i.e icao24 ID
        for entry in my_dict:
            my_id = entry
            my_lat = my_dict[entry]['lat']
            my_lon = my_dict[entry]['lon']

            # And write to postgres
            query = '''INSERT INTO adsb(icao_id, lat, lon) VALUES('%s', '%s', '%s')''' % (my_id, str(my_lat), str(my_lon))
            curr.execute(query)
            conn.commit()
            print("Records inserted ...")

        print("Awaiting new data ...")
        # result = curr.fetchone()
        # print(result)
        # time.sleep(5)
        a = a + 1
        curr.close()
        conn.close()
        print("Connection closed")




if __name__ == "__main__" : main()