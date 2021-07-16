import pandas as pd
import pyodbc
# Some other example server values are
# server = "localhost\sqlexpress" # for a named instance
# server = "myserver,port" # to specify an alternate port
# server = "tcp:myserver.database.windows.net"
server = "192.168.200.33,50020\cloud"
database = "Rumaos"
username = "gpedro" 
password = "s3rv1d0"
try:
    cnxn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};\
        SERVER="+server+";DATABASE="+database+";UID="+username+";PWD="+ password)
except Exception as e:
    print("Ocurrió un error al conectar a SQL Server: ", e)

cursor = cnxn.cursor()

#Sample Query
cursor.execute("SELECT * FROM [Rumaos].[dbo].[Mail]")
row = cursor.fetchone()
while row:
    print(row[1])
    row = cursor.fetchone()