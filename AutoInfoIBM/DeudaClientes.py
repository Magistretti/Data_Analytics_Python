import pandas as pd

# We are connecting to a Microsoft SQL Server so we can use pyodbc library

import pyodbc

from DatosLogin import login

# "login" is a list that holds the connection data to the SQL Server

server = login[0]
database = login[1]
username = login[2] 
password = login[3]

# Try connecting to the SQL Server, will report error and stop if failed

try:
    db_conex = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};\
        SERVER="+server+";DATABASE="+database+";UID="+username+";PWD="+ password)
except Exception as e:
    print("Ocurrió un error al conectar a SQL Server: ", e)
    exit()

cursor = db_conex.cursor()

#Sample Query
# cursor.execute("SELECT * FROM [Rumaos].[dbo].[Mail]")
# row = cursor.fetchone()
# while row:
#     print(row[1])
#     row = cursor.fetchone()

df_mail = pd.read_sql("SELECT * FROM [Rumaos].[dbo].[Mail]",db_conex)

print(df_mail)