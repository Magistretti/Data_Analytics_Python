###################################
#
#     INFORME TOTALES POR COMBUSTIBLE
#
###################################

import os
import pandas as pd
import dataframe_image as dfi
import pyodbc #Library to connect to Microsoft SQL Server
from DatosLogin import login #Holds connection data to the SQL Server

tiempoInicio = pd.to_datetime("today")

#########
# Formating display of dataframes with comma separator for numbers
pd.options.display.float_format = "{:20,.0f}".format 
#########

# Try connecting to the SQL Server, will report error and stop if failed
try:
    db_conex = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};\
        SERVER="+login[0]+";\
        DATABASE="+login[1]+";\
        UID="+login[2]+";\
        PWD="+ login[3]
    )
except Exception as e:
    print("Ocurrió un error al conectar a SQL Server: ", e)
    exit()

########
# Convert dbo.EmpVenta SQL table to Dataframe
# 
# Requirements:
# -Filter unused columns
# -Get data only from yesterday and with VTATOTVOL > 0
########

df_empVenta = pd.read_sql("""
    SELECT  
        [UEN]
        ,[FECHASQL]
        ,[CODPRODUCTO]
        ,[VTATOTVOL]
    FROM [Rumaos].[dbo].[EmpVenta]
    WHERE FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
	    AND EmpVenta.VTATOTVOL > '0'
""", db_conex)

# print(df_empVenta.info())
# print(df_empVenta.head())

def grupo(codproducto):
    if codproducto == "GO" or codproducto == "EU":
        return "GASÓLEOS"
    elif codproducto == "NS" or codproducto == "NU":
        return "NAFTAS"
    else:
        return "GNC"

#########
df_empVenta["GRUPO"]= grupo(df_empVenta["CODPRODUCTO"])
print(df_empVenta.head())
#####ERROR


tablita = pd.pivot_table(df_empVenta
    , values="VTATOTVOL"
    , index="UEN"
    , columns="CODPRODUCTO"
    , fill_value=0
    , margins=True
)
# print(tablita)