###################################
#
#     INFORME TOTALES POR COMBUSTIBLE
#
###################################

import os
from numpy import row_stack
import pandas as pd
from pandas.api.types import CategoricalDtype
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

df_empVenta = df_empVenta.convert_dtypes()
# print(df_empVenta.info())
# print(df_empVenta.head())

# Removing trailing whitespace from the UEN and CODPRODUCTO columns
df_empVenta["UEN"] = df_empVenta["UEN"].str.strip()
df_empVenta["CODPRODUCTO"] = df_empVenta["CODPRODUCTO"].str.strip()


def grupo(codproducto):
    if codproducto == "GO" or codproducto == "EU":
        return "GASÓLEOS"
    elif codproducto == "NS" or codproducto == "NU":
        return "NAFTAS"
    else:
        return "GNC"


df_empVenta["GRUPO"] = df_empVenta.apply(
    lambda row: grupo(row["CODPRODUCTO"])
        , axis= 1
)

# Creating an ordered categorical type of GRUPO
categoriaGrupo = CategoricalDtype(
    categories=[
        "GASÓLEOS"
        ,"NAFTAS"
        ,"GNC"
    ], ordered=True
)

# Casting GRUPO column as ordered categorical
df_empVenta["GRUPO"] = df_empVenta["GRUPO"].astype(categoriaGrupo)

# tablita = pd.pivot_table(df_empVenta
#     , values="VTATOTVOL"
#     , index="UEN"
#     , columns="GRUPO"
#     , aggfunc=sum
#     , fill_value=0
#     , margins=True
#     , margins_name="TOTAL"
# )
# print(tablita.iloc[:, :-1])

df_regalosTraslados = pd.read_sql("""
    SELECT
        EmP.[UEN]
        ,EmP.[FECHASQL]
        ,EmP.[CODPRODUCTO]
        ,EmP.[VOLUMEN] as VTATOTVOL
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
        AND EmP.VOLUMEN > '0' 
        AND (EmP.[CODPROMO] = '30'
            OR P.[DESCRIPCION] like '%PRUEBA%'
            OR P.[DESCRIPCION] like '%TRASLADO%'
            OR P.[DESCRIPCION] like '%MAYORISTA%'
        )
""", db_conex)

df_regalosTraslados = df_regalosTraslados.convert_dtypes()

df_regalosTraslados["UEN"] = df_regalosTraslados["UEN"].str.strip()
df_regalosTraslados["CODPRODUCTO"] = \
    df_regalosTraslados["CODPRODUCTO"].str.strip()