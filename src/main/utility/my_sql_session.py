import mysql.connector
from resources.dev.config import properties
def get_mysql_connection():
    connection = mysql.connector.connect(
        host=properties['host'],
        user=properties['user'],
        password=properties['password'],
        database=properties['database']
    )
    return connection

















# connection = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     password="password",
#     database="manish"
# )
#
# # Check if the connection is successful
# if connection.is_connected():
#     print("Connected to MySQL database")
#
# cursor = connection.cursor()
#
# # Execute a SQL query
# query = "SELECT * FROM manish.testing"
# cursor.execute(query)
#
# # Fetch and print the results
# for row in cursor.fetchall():
#     print(row)
#
# # Close the cursor
# cursor.close()
#
# connection.close()
