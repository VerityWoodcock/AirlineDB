"""
Contains the functions for each SQLite database query.
Used by run_queries.py to address the task SQL Queries and Database Interaction.
Used by database_application as the queries for the CLI application used by main.
Includes functions open_connection, close_connection, fetch_flights, modify_schedule, fetch_pilots, assign_pilot,
fetch_destinations, modify_destination
"""

# import the sqlite3 package
import sqlite3
# import tabulate to format tables
from tabulate import tabulate


def open_connection():
    """ Connects to the flight.db database, creating it if it doesn't
     already exist. Returns a connection and cursor.
     """
    # Open a database connection & create flight.db database if non-existent.
    connection = sqlite3.connect('flight.db')

    # Create a database cursor to query the database.
    cursor = connection.cursor()
    return connection, cursor


def close_connection(connection, cursor):
    """ Closes the cursor and connection to the flight.db database"""
    # close the cursor
    cursor.close()

    # close the connection to free resources used by the database
    connection.close()


def fetch_flights(flight_number=None, departure_city=None, arrival_city=None,
                  departure_date=None, flight_status=None):
    """ Function to fetch all flights with the option to enter additional
    criteria in order to filter the data (flight_number, departure_city,
    arrival_city, departure_date, flight_status). Returns scheduled & unscheduled
    flights. Protects against SQL injection by using parameterised queries.
    """
    # Open connection to the database & create cursor
    connection, cursor = open_connection()

    # Script to query the database for relevant flight data. WHERE instruction
    # is inserted once created, using if statements, for flexible querying
    # Left joins ensure that flights are returned, even when these have not
    # been scheduled.
    flight_query = '''
        SELECT df.flight_number, dd1.airport_name, dd2.airport_name,
        df.scheduled_departure_time, df.scheduled_arrival_time, 
        fs.departure_date, dp.pilot_ID, fs.status
        FROM d_flight df
        LEFT JOIN f_schedule fs on df.flight_ID = fs.flight_ID
        LEFT JOIN d_destination dd1 on df.departure_destination_code 
        = dd1.destination_code
        LEFT JOIN d_destination dd2 on df.arrival_destination_code 
        = dd2.destination_code
        LEFT JOIN d_pilot dp on fs.pilot_ID = dp.pilot_ID
        '''

    # Create tuple for holding the where elements for flight_query.
    where_criteria = ()
    # Create tuple for holding the where fields for the parameterised query.
    fields = ()

    # if criteria is specified in the function call, insert into holder tuples.
    if flight_number:
        where_criteria += ("df.flight_number = ?",)
        fields += (flight_number,)

    if departure_city:
        where_criteria += ("dd1.city = ?",)
        fields += (departure_city,)

    if arrival_city:
        where_criteria += ("dd2.city = ?",)
        fields += (arrival_city,)

    if departure_date:
        where_criteria += ("fs.departure_date = ?",)
        fields += (departure_date,)

    if flight_status:
        where_criteria += ("fs.status = ?",)
        fields += (flight_status,)

    #  Create the end of flight_query if there are elements in the tuple.
    if where_criteria:
        flight_query += " WHERE " + " AND ".join(where_criteria)

    # Order in reverse date order to have the newest dates at the top of the list.
    flight_query += "ORDER BY fs.departure_date DESC"

    #  Execute the parameterised query.
    cursor.execute(flight_query, fields)
    #  Fetch all query results.
    result = cursor.fetchall()

    #  Prepare headers for the tabulate function.
    flight_headers = ["Flight Number", "Departure Destination", "Arrival Destination", "Scheduled Departure",
                      "Scheduled Arrival", "Departure Date", "Pilot ID", "Flight Status"]

    #  Print the result in a neat table, unless empty. If empty, return a helpful message.
    if result:
        print(tabulate(result, headers=flight_headers, tablefmt="rounded_outline"))
    else:
        print("No data returned.")

    #  Close the connection to the database and the cursor.
    close_connection(connection, cursor)


def modify_schedule(schedule_id=None, new_departure_time=None,
                    new_arrival_time=None, new_status=None):
    """ Function to update flight schedules. User enters the schedule_ID for
    the relevant scheduled flight alongside one of three chosen options: 
    new_departure_time, new_arrival_time, new_status. Protects against SQL 
    injection by using parameterised queries.
    """
    # Check that the user has entered a schedule_ID, end and provide feedback otherwise.
    if schedule_id is None:
        print("***No schedule ID has been entered***")
        return

    # Provide feedback to the user, showing which schedule_ID was queried.
    print("Before making changes, the existing database information for ScheduleID", schedule_id, "was:")

    #  Open connection to the database and create cursor
    connection, cursor = open_connection()

    #  Create the parameterised query, to return the previous scheduled flight information to the user.
    schedule_lookup = '''
    SELECT schedule_ID, departure_date, actual_departure_time, 
    actual_arrival_time, flight_ID, pilot_ID, status
    FROM f_schedule
    WHERE schedule_ID = ?
    '''

    #  Execute the parameterised query, schedule_id is a primary key and will only return one result (if exists).
    cursor.execute(schedule_lookup, (schedule_id,))
    #  Fetch the query result.
    schedule_result = cursor.fetchone()

    #  Prepare headers for the tabulate function.
    schedule_headers = ["Schedule ID", "Departure Date", "Actual Departure Time", "Actual Arrival Time",
                        "Flight ID", "Pilot ID", "Flight Status"]
    #  Print the result if the schedule id exists.
    if schedule_result:
        print(tabulate([schedule_result], headers=schedule_headers, tablefmt="rounded_outline"))
    else:
        print("Schedule ID", schedule_id, "doesn't exist. Cancelled transaction. No changes have been made.")
        return

    #  Feedback to user that the schedule information is being adapted.
    print("Updating schedule information for schedule ID", schedule_id, "...")

    #  To comply with ACID - the entire transaction will complete or nothing. Begin transaction.
    cursor.execute("BEGIN")

    # the function only took one parameter, therefore, the script is carried out depending on which was entered
    # (departure time, arrival time, flight status).
    if new_departure_time:
        schedule_modification = ''' 
        UPDATE f_schedule 
        SET actual_departure_time = ? 
        WHERE schedule_ID = ?
        '''
        #  Execute the query.
        cursor.execute(schedule_modification, (new_departure_time, schedule_id))

    if new_arrival_time:
        schedule_modification = ''' 
        UPDATE f_schedule 
        SET actual_arrival_time = ? 
        WHERE schedule_ID = ?
        '''

        cursor.execute(schedule_modification, (new_arrival_time, schedule_id))

    if new_status:
        schedule_modification = '''
        UPDATE f_schedule
        SET status = ?
        WHERE schedule_ID = ?
        '''

        cursor.execute(schedule_modification, (new_status, schedule_id))

    #  Complete the transaction, commit the changes.
    connection.commit()

    #  Prepare to feed back to the user the new schedule information for that schedule ID.
    print("After any changes, the schedule information, for schedule ID", schedule_id, "is:")

    #  Execute the query that was first provided, this will return the current, updated state for that schedule ID.
    cursor.execute(schedule_lookup, (schedule_id,))

    #  Fetch the result, it will only be one as schedule ID is the primary key.
    updated_result = cursor.fetchone()

    #  Ensuring there is something to print, prepare headers for a nicely formatted table.
    if updated_result:
        print(tabulate([updated_result], headers=schedule_headers, tablefmt="rounded_outline"))
    # This line shouldn't ever run, but let the user know if there was no data.
    else:
        print("\nNo data returned. You need to check the database as it doesn't appear to exist.")

    # Close the database connection and cursor.
    close_connection(connection, cursor)


def fetch_pilots(pilot_id=None, first_name=None, last_name=None):
    """ Function to fetch all pilots, with the option to enter additional
    criteria in order to filter the data (pilot_id, first_name, last_name). Returns scheduled flights for pilots.
    . Protects against SQL injection by using parameterised queries.
    """
    # Open connection to the database & create cursor
    connection, cursor = open_connection()

    # Script to query the database for data that is relevant for pilot schedules. WHERE instruction
    # is inserted once created, using if statements, for flexible querying.
    # Inner join ensures that flights are returned only if they have a schedule ID and pilot ID
    pilot_query = '''
    SELECT fs.schedule_ID, fs.departure_date, fs.flight_ID, fs.status, 
    dp.pilot_ID, dp.first_name, dp.last_name, df.scheduled_departure_time, 
    df.scheduled_arrival_time, dd1.city, dd2.city  
    FROM f_schedule fs
    JOIN d_pilot dp on fs.pilot_ID = dp.pilot_ID
    JOIN d_flight df on fs.flight_ID = df.flight_ID
    JOIN d_destination dd1 on df.departure_destination_code = dd1.destination_code
    JOIN d_destination dd2 on df.arrival_destination_code = dd2.destination_code 
    '''

    #  Tuple to hold the values for the WHERE clause in pilot_query.
    where_criteria = ()
    #  Tuple to hold values for the parametrised query.
    fields = ()

    # Function will return schedules for all pilots or if specific pilot details were selected, the where_criteria
    # tuple will hold this to allow the user to filter for specific search criteria.
    if pilot_id:
        where_criteria += ("dp.pilot_ID = ?",)
        #  Add value to holder for the parametrised query.
        fields += (pilot_id,)

    if first_name:
        where_criteria += ("dp.first_name = ?",)
        fields += (first_name,)

    if last_name:
        where_criteria += ("dp.last_name = ?",)
        fields += (last_name,)

    #  Form the WHERE clause if parameters are included in the function call.
    if where_criteria:
        pilot_query += " WHERE " + " AND ".join(where_criteria)

    #  Execute the query.
    cursor.execute(pilot_query, fields)
    # Fetch all the query result(s).
    result = cursor.fetchall()

    # Prepare the headers for the neatly formatted table to return the query results.
    pilot_headers = ["Schedule ID", "Departure Date", "Flight ID", "Flight Status", "Pilot ID", "First Name",
                     "Last Name", "Scheduled Departure time", "Scheduled Arrival Time", "Departure City",
                     "Arrival City"]

    # If there is a result, print it in the table. Else provide a helpful message to the user.
    if result:
        print(tabulate(result, headers=pilot_headers, tablefmt="rounded_outline"))
    else:
        print("No data returned. Check the data you are using the search by and try again.")

    # Close the database connection and the cursor.
    close_connection(connection, cursor)


def assign_pilot(pilot_id=None, flight_number=None, departure_date=None,
                 departure_city=None, arrival_city=None):
    """ Assign pilot to a flight. User must provide the pilot_id, flight_number,
    departure_date, departure_city and arrival_city to complete the modification.
    To ensure that the correct flight is modified the flight_id is selected
    from the database and the user is asked to confirm the change."""

    # Create a database cursor to query the database.
    connection, cursor = open_connection()

    #  If no pilot id was provided, stop with a useful message for the user.
    if pilot_id is None:
        print("No pilot ID has been entered")
        close_connection(connection, cursor)
        return

    #  Prepare
    print("Fetching the schedule information from the database for pilotID: ", pilot_id)

    # Call the fetch_pilot function, using the pilot_id, to allow a user to double check before assigning a pilot.
    fetch_pilots(pilot_id)

    # Check the user has provided all necessary information, to be able to look up the one specific flight. If not,
    # stop and provide a useful message to the user.
    if not all([flight_number, departure_date, departure_city, arrival_city]):
        print(
            "In order to assign a pilot, please provide flight number, departure date, departure city and arrival city")
        return

    # Provide the user with the flight information, to allow them to double check before assigning a pilot to it.
    print("Retrieving the database information for flight number: ", flight_number, " departing on ", departure_date,
          " from ", departure_city, " to ", arrival_city, ":")

    #  Use the fetch_flights function to return the flight information, using the information provided by the user.
    fetch_flights(flight_number=flight_number, departure_date=departure_date, departure_city=departure_city,
                  arrival_city=arrival_city)

    #  Create a boolean to check if the user has confirmed the pilot assignment should go ahead before the change.
    needs_to_confirm = True
    while needs_to_confirm:
        confirmation_decision = input(
            f"Shall I assign the pilot {pilot_id} to the scheduled flight {flight_number}?  (enter 'yes' to complete "
            f"the change or 'no' to cancel): ").lower()

        #  Stop and provide a useful message to the user if they have chosen not to make the changes.
        if confirmation_decision == "no":
            print("No changes have been made")
            break
        #  If the user has confirmed the change is to go ahead then continue.
        elif confirmation_decision == "yes":
            needs_to_confirm = False

            # Create a database cursor to query the database.
            connection, cursor = open_connection()

            #  Prepare a script to look up the flight_id in the database.
            flight_id_lookup = '''
            SELECT df.flight_id
            FROM d_flight df
            LEFT JOIN d_destination dd1 ON df.departure_destination_code = dd1.destination_code
            LEFT JOIN d_destination dd2 ON df.arrival_destination_code = dd2.destination_code
            WHERE df.flight_number = ?
            AND dd1.city = ?
            AND dd2.city = ?
            '''

            # Execute the parameterised query to look up flight ID.
            cursor.execute(flight_id_lookup, (flight_number, departure_city, arrival_city))
            # Fetch one (flight ID is the primary key)
            flight_id = cursor.fetchone()

            # If no flight id has returned then stop and return a useful message to the user.
            if not flight_id:
                print(
                    "The flight details do not appear to exist, based on the criteria provided, check your inputs and "
                    "try again.")
                return

            # Save the flight id in a variable
            flight_id = flight_id[0]

            # Begin a transaction, complying with ACID - all or nothing transactions.
            cursor.execute("BEGIN")

            # Prepare the parameterised query to update the database.
            pilot_assignment = '''
            UPDATE f_schedule
            SET pilot_id = ?
            WHERE flight_id = ? 
            AND departure_date = ?
            '''

            # Execute the query, using the pilot_assignment script and the user details (plus the looked up flight_ID).
            cursor.execute(pilot_assignment, (pilot_id, flight_id, departure_date))

            # Commit the changes to the database.
            connection.commit()

            # Print a message to let the user know the pilot assignment was completed.
            print("pilot ", pilot_id, " has been assigned successfully.")

        #  Advise the user the pilot schedule, for the current state of the database is being looked up.
        print("Retrieving pilot schedule details for pilot", pilot_id, ":")
        # Call the fetch_pilot function to fetch the schedule for the specific pilot ID.
        fetch_pilots(pilot_id)

        # Advise the user that specific flight detail, for the current state of the database is being looked up.
        print("Retriv flight schedule details for flight_number", flight_number)
        # Call the fetch_fligths function to fetch the flight data for the specific flight the user had entered.
        fetch_flights(flight_number=flight_number, departure_city=departure_city, arrival_city=arrival_city,
                      departure_date=departure_date)

        # Close the database connection and cursor.
        close_connection(connection, cursor)


def fetch_destinations(destination_code=None, airport_name=None, city=None, country=None):
    """ Function to return information about all destinations. User can include a parameter for filtering:
    (destination_code, airport_name, city, country).
    """
    #  Advise the user information is being retrieved.
    print("Retrieve information about destinations.")

    # Create a database cursor to query the database.
    connection, cursor = open_connection()

    # Prepare the parameterised query to return destination information.
    destination_query = '''
    SELECT destination_code, airport_name, city, country  
    FROM d_destination
    '''

    # Hold the users inputted values within a tuple, to use in the WHERE claus.
    where_criteria = ()
    # Hold the values in a tuple for the parameterised query.
    fields = ()

    # If input values were included in the function call, these are added to the where_criteria and fields variables.
    if destination_code:
        where_criteria += ("destination_code = ?",)
        fields += (destination_code,)

    if airport_name:
        where_criteria += ("airport_name = ?",)
        fields += (airport_name,)

    if city:
        where_criteria += ("city = ?",)
        fields += (city,)

    if country:
        where_criteria += ("country = ?",)
        fields += (country,)

    # Process the input values, placing them in a string to add to the destination_query (if they were provided).
    if where_criteria:
        destination_query += " WHERE " + " AND ".join(where_criteria)

    # Execute the query.
    cursor.execute(destination_query, fields)
    # Fetch all query results.
    result = cursor.fetchall()

    # Prepare headers to prepare a neat table for printing the results.
    destination_headers = ["Destination Code", "Airport Name", "City", "Country"]

    #  Print the results in a formatted table.
    print(tabulate(result, headers=destination_headers, tablefmt="rounded_outline"))

    # Close the connection and the cursor.
    close_connection(connection, cursor)


def modify_destination(destination_code=None, airport_name=None, city=None, country=None):
    """ Function to allow user to edit the destination details. User provides the destination_code (primary key) for
    the destination that needs modifying as well as one value that needs updating (airport_name, city, country).
    """
    # If destination code has not been provided, stop and return a useful message to the user.
    if destination_code is None:
        print("No destination code has been entered")
        return

    # Create a connection to the database and cursor.
    connection, cursor = open_connection()

    # Change the destination_code to uppercase, as per the database format.
    destination_code = destination_code.upper()

    # Inform the user the database information is being retrieved.
    print("Retrieving the database information for Destination Code ", destination_code)
    #  Call the fetch_destination function with the destination_code to provide the current state to the user.
    fetch_destinations(destination_code=destination_code)

    # Advise the user that the information is being updated for that destination_code.
    print("Updating destination information for Destination Code ", destination_code)

    # Try to start a transaction (complying with ACID principle - all or nothing to be committed to the database).
    try:
        cursor.execute("BEGIN")

        # If the airport name was provided in the function call then prepare a parameterised query to update this in
        # the database. If city or country were provided then use the relevant attribute in the SET clause.
        if airport_name:
            cursor.execute('''
            UPDATE d_destination
            SET airport_name = ?
            WHERE destination_code = ?
            ''', (airport_name, destination_code))

        if city:
            cursor.execute('''
            UPDATE d_destination
            SET city = ?
            WHERE destination_code = ?
            ''', (city, destination_code))

        if country:
            cursor.execute('''
            UPDATE d_destination
            SET country = ?
            WHERE destination_code = ?
            ''', (country, destination_code))

        # Commit the change to the database.
        connection.commit()

        # Provide a useful message for the user that they have changed the destination information.
        print("You have updated the destination information for ", destination_code, "the row now contains:")

        # Call the fetch_destination function with the destination_code to return the updated information to the user.
        fetch_destinations(destination_code=destination_code)

    # If there is an error e.g. the destination code didn't exist, the user receives an indication.
    except ValueError as ve:
        print(f"Please check your inputs, there is an error {ve}.")

    # Close the database connection and the cursor.
    close_connection(connection, cursor)


def unassigned_flights():
    """ Function call returns to the user the number of flights that are in the fact table where no pilot id exists."""
    # Create a connection to the database and a cursor object.
    connection, cursor = open_connection()

    # Execute a query to count how many rows in the f_schedule fact table have a null for the pilot_id value.
    cursor.execute('''
        SELECT COUNT(schedule_id)
        FROM f_schedule 
        WHERE pilot_id IS null
        ''')
    # Fetch the result.
    result = cursor.fetchone()[0]

    # Close the connection and the cursor.
    close_connection(connection, cursor)

    # Return the count to the user, with correct grammar.
    if result == 0:
        print("No scheduled flights are lacking an assigned pilot.\n")
    elif result == 1:
        print("There is", result, "scheduled flight which does not have a pilot assigned to it.\n")
    else:
        print("There are", result, "scheduled flights which do not have a pilot assigned to them.\n")


def flights_per_pilot():
    """ Function call to count how many flights pilots have been allocated to on the fact table f_schedule. Does not
    return pilot_IDs with no flights.
    """
    # Create a connection to the database and a cursor object.
    connection, cursor = open_connection()

    # Execute the query to return pilot information and count of flights each pilot has (where a pilot has flights).
    cursor.execute('''
        SELECT fs.pilot_id, dp.first_name, dp.last_name, COUNT(fs.schedule_id) AS "Flight Count"
        FROM f_schedule fs 
        LEFT JOIN d_pilot dp ON fs.pilot_id = dp.pilot_id
        WHERE fs.pilot_id IS NOT NULL
        GROUP BY fs.pilot_id, dp.first_name, dp.last_name
        ''')
    # Fetch all rows from the database.
    result = cursor.fetchall()
    # Close the connection and cursor safely.
    close_connection(connection, cursor)

    # Print a header to explain to the user what information will be displayed.
    print("Summarising the number of scheduled flights assigned to pilots:")
    # Print pilot information and count of flights for each pilot neatly.
    for pilot_id, first_name, last_name, count in result:
        print(f"-Pilot ID", pilot_id, "is", first_name, last_name, "and has been assigned to", count, "flight(s).")


def flights_per_destination():
    """ Function call to return how many flights have a 'scheduled' status in the f_schedule fact table for each
    arrival destination. Returns a count for each arrival destination."""
    # Open a database connection and create a cursor object.
    connection, cursor = open_connection()

    # Create the query to return the number of 'scheduled' flights for each arrival destination.
    cursor.execute('''
        SELECT dd2.airport_name, dd2.city, dd2.country, COUNT(fs.schedule_id) AS "Flight Count"
        FROM f_schedule fs 
        LEFT JOIN d_flight df ON fs.flight_id = df.flight_id
        LEFT JOIN d_destination dd1 on df.departure_destination_code = dd1.destination_code
        LEFT JOIN d_destination dd2 on df.arrival_destination_code = dd2.destination_code
        WHERE fs.status = "scheduled"
        GROUP BY dd2.airport_name
        ORDER BY dd2.airport_name DESC
        ''')
    # Fetch all rows from the database.
    result = cursor.fetchall()
    # Safely close the database connection and cursor object.
    close_connection(connection, cursor)
    # Inform the user what they are being presented with.
    print("Summarising the number of flights for each arrival destination (with a status of 'scheduled':")
    # For each row, neatly print the details for each arrival destination.
    for destination_airport_name, city, country, count in result:
        print(f"-The destination airport {destination_airport_name} in {city}, {country} is served by {count} flight(s)"
              f" with a 'scheduled' status.")
