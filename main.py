""" Command Line Interface for the flight.db database application. """

# Import function that sets off the application.
from database_application import welcome_message

# Create a continuous loop for the database to run continuously.
application_running = True
while application_running:
    welcome_message()
