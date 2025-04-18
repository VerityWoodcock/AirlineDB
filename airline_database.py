"""
Sets up the SQLite flight database 'flight.db'
Includes functions to connect and disconnect, create tables and populate tables.
Tables include 1 fact table (f_schedule) and 3 dimension tables (d_flight,
d_pilot, d_destination).
"""

# import the sqlite3 package
import sqlite3


def open_connection():
    """ Connects to the flight.db database, creating it if it doesn't
     already exist. Returns a connection and cursor.
     """
    # Open a database connection & create flight.db database if non-existent.
    connection = sqlite3.connect('flight.db', isolation_level=None)

    # Create a database cursor to query the database.
    cursor = connection.cursor()
    return connection, cursor


def close_connection(connection, cursor):
    """ Closes the cursor and connection to the flight.db database"""
    # close the cursor
    cursor.close()

    # close the connection to free resources used by the database
    connection.close()


# def drop_all_tables():
#     """ Deletes the 4 sample database tables. Commented out for safety."""
#     # uncomment to drop all tables if needed.
#     connection, cursor = open_connection()
#
#     # delete all 4 tables.
#     cursor.executescript('''
#     DROP TABLE IF EXISTS d_pilot;
#     DROP TABLE IF EXISTS d_flight;
#     DROP TABLE IF EXISTS d_destination;
#     DROP TABLE IF EXISTS f_schedule;
#     ''')
#
#     # Close cursor and connection to the database.
#     close_connection(connection, cursor)


def create_database_tables():
    """ Connects to the database to create 3 dimension tables (d_pilot,
    d_flight, d_destination) and the fact table(f_schedule). Relationships are
    created between the fact table and pilot & flight tables as well as between
    the destination and flight tables. Tables are created in strict mode, where
    every column must have a datatype. Raises an integrity error exception if
    the wrong type is entered into a table.
    """
    # Open connection to the database & create cursor.
    connection, cursor = open_connection()

    # Script to create the Pilot lookup table.
    table_schema_pilot = '''
    CREATE TABLE IF NOT EXISTS d_pilot (
    pilot_ID TEXT PRIMARY KEY NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    licence_number TEXT NOT NULL,
    licence_expiry TEXT NOT NULL,
    night_flag INTEGER NOT NULL
    ) STRICT; 
    '''

    # Script to create the flight lookup table.
    table_schema_flight = '''
    CREATE TABLE IF NOT EXISTS d_flight (
    flight_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    flight_number TEXT NOT NULL,
    departure_destination_code TEXT NOT NULL,
    arrival_destination_code TEXT NOT NULL,
    scheduled_departure_time TEXT NOT NULL,
    scheduled_arrival_time TEXT NOT NULL,
    FOREIGN KEY(departure_destination_code) REFERENCES d_destination(destination_code),
    FOREIGN KEY(arrival_destination_code) REFERENCES d_destination(destination_code)
    ) STRICT;
    '''

    # Script to create the destination lookup table.
    table_schema_destination = '''
    CREATE TABLE IF NOT EXISTS d_destination (
    destination_code TEXT PRIMARY KEY NOT NULL,
    airport_name TEXT NOT NULL,
    city TEXT NOT NULL,
    country TEXT NOT NULL
    ) STRICT;
    '''

    # Script to create the schedule fact table.
    table_schema_schedule = '''
    CREATE TABLE IF NOT EXISTS f_schedule (
    schedule_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    departure_date TEXT NOT NULL,
    actual_departure_time TEXT,
    actual_arrival_time TEXT,
    flight_ID INTEGER NOT NULL,
    pilot_ID TEXT,
    status TEXT NOT NULL,
    FOREIGN KEY(flight_ID) REFERENCES d_flight(flight_ID),
    FOREIGN KEY(pilot_ID) REFERENCES d_pilot(pilot_ID)
    ) STRICT;
    '''

    # Allow creation of relationships (primary and foreign keys).
    cursor.execute('PRAGMA foreign_keys = ON')

    # Command creates the 4 tables, using the previously created scripts.
    cursor.execute(table_schema_destination)
    cursor.execute(table_schema_pilot)
    cursor.execute(table_schema_flight)
    cursor.execute(table_schema_schedule)

    # Commit changes to the database.
    connection.commit()

    # Close cursor and connection to the database.
    close_connection(connection, cursor)


def load_sample_data():
    """Loads sample data into the database tables (d_destination, d_flight,
     d_pilot, f_schedule). Protects against SQL injection using parameterised
     queries. Exception included to catch duplicate primary key entries.
     """
    # Open connection to the database & create cursor.
    connection, cursor = open_connection()

    # create an object pilots to be inserted to the d_pilots table.
    pilots = [('P0010001', 'Bhaagyashree', 'Patil', 'ATPL8954', "2026-03-31", 1),
              ('P0010002', 'Jo', 'Hyde', 'ATPL5235', "2027-01-26", 1),
              ('P0010003', 'Christina', 'Keating', 'ATPL8648', "2025-09-03", 0),
              ('P0010004', 'Zach', 'Lyons', 'CPL85695', "2026-02-28", 1),
              ('P0010005', 'Raghubir', 'Singh', 'CPL64585', "2025-10-15", 0),
              ('P0010006', 'Matthew', 'Albertyn', 'CPL89563', "2026-08-22", 1),
              ('P0010007', 'James', 'Davenport', 'CPL85685', "2025-12-17", 1),
              ('P0010008', 'Paola', 'Bruscoli', 'ATPL5684', "2026-11-01", 1),
              ('P0010009', 'Ben', 'Ralph', 'CPL58695', "2026-05-08", 0),
              ('P0010010', 'Neil', 'Langmead', 'ATPL2135', "2025-06-30", 1)
              ]

    # create an object destination to be inserted to the d_destination table.
    destinations = [('DUB', 'Dublin International', 'Dublin', 'Ireland'),
                    ('EMA', 'East Midlands', 'Nottingham', 'England'),
                    ('BHX', 'Birmingham Airport', 'Birmingham', 'England'),
                    ('NWI', 'Norwich International', 'Norwich', 'England'),
                    ('BRS', 'Bristol Airport', 'Bristol', 'England'),
                    ('EXE', 'Exeter Airport', 'Exeter', 'England'),
                    ('SOU', 'Southampton Airport', 'Southampton', 'England'),
                    ('GCI', 'Guernsey Airport', 'Guernsey', 'Channel Islands'),
                    ('JER', 'Jersey Airport', 'Jersey', 'Channel Islands'),
                    ('NCL', 'Newcastle International', 'Newcastle', 'England'),
                    ('CDG', 'Charles de Gaulle', 'Paris', 'France'),
                    ('BIO', 'Bilbao Airport', 'Bilbao', 'Spain'),
                    ('MUC', 'Munich International', 'Munich', 'Germany'),
                    ('VRN', 'Verona Villafranca Airport', 'Verona', 'Italy'),
                    ('PMI', 'Palma de Mallorca Airport', 'Palma de Mallorca', 'Spain')
                    ]

    # create an object flights to be inserted to the d_flights table.
    flights = [(1, 'SI2206', 'GCI', 'JER', '08:30:00', '08:50:00'),
               (2, 'SI3350', 'JER', 'SOU', '07:15:00', '08:05:00'),
               (3, 'SI3351', 'SOU', 'JER', '08:35:00', '09:25:00'),
               (4, 'SI5580', 'JER', 'DUB', '10:35:00', '12:20:00'),
               (5, 'SI2206', 'JER', 'EXE', '09:20:00', '10:05:00'),
               (6, 'SI2207', 'EXE', 'JER', '10:45:00', '11:30:00'),
               (7, 'SI2203', 'EXE', 'JER', '08:35:00', '09:20:00'),
               (8, 'SI2202', 'JER', 'EXE', '07:20:00', '08:05:00'),
               (9, 'SI5552', 'GCI', 'JER', '12:45:00', '13:05:00'),
               (10, 'SI5581', 'DUB', 'JER', '13:00:00', '14:45:00'),
               (11, 'SI3312', 'GCI', 'SOU', '10:00:00', '10:45:00'),
               (12, 'SI3328', 'GCI', 'SOU', '13:50:00', '14:35:00'),
               (13, 'SI3342', 'GCI', 'SOU', '18:20:00', '19:05:00'),
               (14, 'SI3329', 'SOU', 'GCI', '15:05:00', '15:50:00'),
               (15, 'SI3313', 'SOU', 'GCI', '11:15:00', '12:00:00')
               ]

    # create an object schedule to be inserted to the f_schedule table.
    schedule = [(1, "2025-04-21", None, None, 2, "P0010001", "scheduled"),
                (2, "2025-04-21", None, None, 3, "P0010001", "scheduled"),
                (3, "2025-04-21", None, None, 5, "P0010002", "scheduled"),
                (4, "2025-04-21", None, None, 6, "P0010002", "scheduled"),
                (5, "2025-04-06", None, None, 7, "P0010002", "landed"),
                (6, "2025-04-06", None, None, 8, "P0010002", "landed"),
                (7, "2025-04-07", None, None, 4, "P0010003", "landed"),
                (8, "2025-04-07", None, None, 10, "P0010003", "landed"),
                (9, "2025-04-08", None, None, 9, "P0010004", "landed"),
                (10, "2025-04-09", None, None, 1, None, "cancelled"),
                (11, "2025-04-29", None, None, 2, None, "scheduled")
                ]

    # Use of ? protects against SQL injection attack, with the execute method.
    # replacing ? with the variable values.
    # Insert pilot data into database.
    try:
        cursor.executemany('''INSERT INTO d_pilot (
        pilot_ID, first_name, last_name, licence_number, licence_expiry, night_flag
        ) VALUES   (?,?,?,?,?,?)
        ''', pilots)
    # In case of data issues, catch integrity error and print a useful message.
    except sqlite3.IntegrityError as ie:
        if "UNIQUE constraint failed: d_pilot.pilot_ID" in str(ie):
            print("There has been a data integrity error (you've already entered a pilot with that pilot_id). "
                  "See error message -> ", ie)
        else:
            print("There has been an integrity error. Data entry failed. See error message -> ", ie)

    # Insert destination data into database.
    try:
        cursor.executemany('''INSERT INTO d_destination (
        destination_code, airport_name, city, country
        ) VALUES   (?,?,?,?)
        ''', destinations)
    # In case of data issues, catch integrity error and print a useful message.
    except sqlite3.IntegrityError as ie:
        if "UNIQUE constraint failed: d_destination.destination_code" in str(ie):
            print("There has been a data integrity error (you've already entered a destination with that "
                  "destination_code). See error message -> ", ie)
        else:
            print("There has been an integrity error. Data entry failed. See error message -> ", ie)

    # Insert flight data into database.
    try:
        cursor.executemany('''INSERT INTO d_flight (
        flight_ID, flight_number, departure_destination_code, 
        arrival_destination_code, scheduled_departure_time, scheduled_arrival_time
        ) VALUES   (?,?,?,?,?,?)
        ''', flights)
    # In case of data issues, catch integrity error and print a useful message.
    except sqlite3.IntegrityError as ie:
        if "UNIQUE constraint failed: d_flight.flight_id" in str(ie):
            print("There has been a data integrity error (you've already entered a flight with that "
                  "flight ID). See error message -> ", ie)
        else:
            print("There has been an integrity error. Data entry failed. See error message -> ", ie)

    # Insert scheduled flight data into database.
    try:
        cursor.executemany('''INSERT INTO f_schedule (
        schedule_ID, departure_date, actual_departure_time, actual_arrival_time, 
        flight_ID, pilot_ID, status
        ) VALUES   (?, ?, ?, ?, ?, ?, ?)
        ''', schedule)
    # In case of data issues, catch integrity error and print a useful message.
    except sqlite3.IntegrityError as ie:
        if "UNIQUE constraint failed: f_schedule.schedule_ID" in str(ie):
            print("There has been a data integrity error (you've already scheduled a flight with that "
                  "schedule ID). See error message -> ", ie)
        else:
            print("There has been an integrity error. Data entry failed. See error message -> ", ie)

    # Commit changes to the database.
    connection.commit()

    # Close cursor and connection to the database.
    close_connection(connection, cursor)


# Ensure the databse is only set up if the file is run directly.
if __name__ == "__main__":
    # Database Setup in SQLite. Create and populate the database in SQLite with sample data to facilitate testing and
    # demonstration. Drop_all_tables is commented out to avoid accidental or intended deletion.
    # drop_all_tables()  # Delete database tables.
    create_database_tables()  # Creates the database tables.
    load_sample_data()  # Populates the database with sample data.
