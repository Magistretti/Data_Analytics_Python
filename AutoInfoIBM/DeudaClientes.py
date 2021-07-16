import pandas as pd
from DatosLogin import login
import pyodbc

server = login[0]
database = login[1]
username = login[2] 
password = login[3]
try:
    cnxn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};\
        SERVER="+server+";DATABASE="+database+";UID="+username+";PWD="+ password)
except Exception as e:
    print("Ocurrió un error al conectar a SQL Server: ", e)
    exit()

cursor = cnxn.cursor()

#Sample Query
cursor.execute("SELECT * FROM [Rumaos].[dbo].[Mail]")
row = cursor.fetchone()
while row:
    print(row[1])
    row = cursor.fetchone()