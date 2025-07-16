import psycopg2
from psycopg2 import OperationalError
from Config import loadConfig

def createConnection():
  try:
    connection = psycopg2.connect(**loadConfig())
    print("Connected to database")
    return connection
  except OperationalError as e:
    print(f"Error connect to database: {e}")
    return None
  
if __name__ == "__main__":
  createConnection()