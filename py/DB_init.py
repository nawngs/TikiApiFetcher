from DB_connect import createConnection

def executeSQLFile(filePath):
  connection = createConnection()

  if (not connection):
    print("No connection")
    return None
  
  try:
    cursor = connection.cursor()

    with open(filePath, "r") as f:
      SQLScript = f.read()

    # Execute statement in file
    for statement in SQLScript.split(';'):
      statement = statement.strip()
      if (statement):
        cursor.execute(statement + ';')
    
    connection.commit()
    print("SQL file executed successfully.")
  except Exception as e:
    print(f"Error executing SQL file: {e}")

  finally:
    if (cursor):
      cursor.close()
    if (connection):
      connection.close()
      print("Connection closed")

if __name__ == "__main__":
  executeSQLFile("sql/schema.sql")