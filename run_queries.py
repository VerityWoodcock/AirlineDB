"""
Module to run the queries to address 'SQL Queries and Database Interaction'.
Includes function calls to fetch information (fetch_flights, fetch_pilots, fetch_destinations) to modify information
(modify_schedule, modify_destination), to assign pilots to flights (assign_pilot). Also includes function calls to
fetch summary data (unassigned_flights, flights_per_pilot, flights_per_destination).
"""

# Import the function calls from database_queries.
from database_queries import fetch_flights
from database_queries import fetch_pilots
from database_queries import fetch_destinations
from database_queries import modify_schedule
from database_queries import modify_destination
from database_queries import assign_pilot
from database_queries import unassigned_flights
from database_queries import flights_per_pilot
from database_queries import flights_per_destination


# Ensure this script only runs if this is the file is run directly
if __name__ == "__main__":
    # Carefully make several function calls to fetch flight information, based on multiple criteria.
    print("\n1) Retrieve flights based on multiple criteria (flight number, city, status, departure date).\n")
    print("1a) Flight retrieval -> Retrieving flights based on flight number being 'SI2203'.")
    fetch_flights(flight_number="SI2203")  # retrieve flights based on multiple criteria: flight number
    print("\n1b) Flight retrieval -> Retrieving flights based on departure city being 'Jersey'.")
    fetch_flights(departure_city="Jersey")  # retrieve flights based on multiple criteria: departure destination
    print("\n1c) Flight retrieval -> Retrieving flights based on arrival city being 'Southampton'.")
    fetch_flights(arrival_city="Southampton")  # retrieve flights based on multiple criteria: arrival destination
    print("\n1d) Flight retrieval -> Retrieving flights based on flight status being 'landed'.")
    fetch_flights(flight_status="landed")  # retrieve flights based on multiple criteria: flight status
    print("\n1e) Flight retrieval -> Retrieving flights based on departure date being '21 April 2025'.")
    fetch_flights(departure_date="2025-04-21")  # retrieve flights based on multiple criteria: departure date
    print("\n1f) Flight retrieval -> Retrieving all flights based on departure city being 'Jersey' and departure date"
          " being '21 April 2025'.")
    fetch_flights(departure_city="Jersey", departure_date="2025-04-21")  # retrieve flights based on multiple criteria
    print("\n1g) Flight retrieval -> Retrieving all flights, regardless of whether they have been scheduled or are "
          "missing a pilot.")
    fetch_flights()  # retrieve flights based on multiple criteria: departure date
    print("\n")

    # Carefully make several function calls to modify schedules, based on multiple criteria.
    print("2) Schedule Modification")
    print("\n2a) Schedule Modification -> Update flight schedules e.g. change departure time, arrival time or status")
    modify_schedule(schedule_id=2, new_departure_time="12:00")
    print("\n2b) Schedule Modification -> Update flight schedules e.g. change departure time, arrival time or status")
    modify_schedule(schedule_id=2, new_arrival_time="13:00")
    print("\n2c) Schedule Modification -> Update flight schedules e.g. change departure time, arrival time or status")
    modify_schedule(schedule_id=2, new_status="delayed")

    # Carefully make several function calls to fetch pilot schedule information, based on multiple criteria.
    print("\n3) Retrieve information about pilot schedules.")
    print("\n3a) Pilot schedule retrieval -> Retrieving schedule based on pilot ID being 'P0010001'.")
    fetch_pilots(pilot_id="P0010001")  # retrieve pilot schedule
    print("\n3b) Retrieving schedule information for all pilots.")
    fetch_pilots()  # retrieve pilot schedule

    # Carefully make a function call to assign a pilot to a flight, including multiple mandatory criteria.
    print("\n4) Assigning pilots to flights. Please check the information carefully before completing the change.")
    assign_pilot("P0010007", "SI2207", "2025-04-21", "Exeter", "Jersey")

    # Carefully make several function calls to fetch destination information, based on multiple criteria.
    print("\n5) Retrieve information about destinations.")
    print("\n5a) Destination information retrieval -> Retrieving schedule for destination country 'England'.")
    fetch_destinations(country="England")  # retrieve destination information
    print("\n5b) Retrieving information for all destinations.")
    fetch_destinations()  # retrieve pilot schedule

    # Carefully make several function calls to modify destination information, based on one of three options each time.
    print("\n6) Destination management")
    print("\n6a) Destination Modification -> Update destination e.g. change airport name, city or country.")
    modify_destination(destination_code="EMA", airport_name="East Midlands Airport Central")
    print("\n6b) Destination Modification -> Update destination e.g. change airport name, city or country.")
    modify_destination(destination_code="BRS", city="Paris")
    print("\n6c) Destination Modification -> Update destination e.g. change airport name, city or country.")
    modify_destination(destination_code="DUB", city="Dubline")  # retrieve destination information
    print("\n")

    # Carefully make several function calls to fetch useful flight summary information.
    print("\n7) Additional queries that summarise data:")
    print("\n7a) Number of flights without a pilot:")
    unassigned_flights()
    print("\n7b) Number of flights per pilot:")
    flights_per_pilot()
    print("\n7c) Number of flights per destination:")
    flights_per_destination()
