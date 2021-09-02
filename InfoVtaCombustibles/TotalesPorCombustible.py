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
# Removing trailing whitespace from the UEN and CODPRODUCTO columns
df_empVenta["UEN"] = df_empVenta["UEN"].str.strip()
df_empVenta["CODPRODUCTO"] = df_empVenta["CODPRODUCTO"].str.strip()

# print(df_empVenta.info())
# print(df_empVenta.head())


########
# Join SQL tables dbo.EmpPromo and dbo.Promocio and convert to Dataframe
# 
# Requirements:
# -Filter unused columns
# -Rename VOLUMEN as VTATOTVOL and set it NEGATIVE
# -Get data from yesterday, VOLUMEN > 0 and EmpPromo.CODPROMO = 30 
# OR data from yesterday, VOLUMEN > 0 and Promocio.DESCRIPCION that contains
# "PRUEBA", "TRASLADO" or "MAYORISTA"
#
########

df_regalosTraslados = pd.read_sql("""
    SELECT
        EmP.[UEN]
        ,EmP.[FECHASQL]
        ,EmP.[CODPRODUCTO]
        ,-EmP.[VOLUMEN] as VTATOTVOL
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
# Removing whitespace
df_regalosTraslados["UEN"] = df_regalosTraslados["UEN"].str.strip()
df_regalosTraslados["CODPRODUCTO"] = \
    df_regalosTraslados["CODPRODUCTO"].str.strip()

# print(df_regalosTraslados.info())
# print(df_regalosTraslados.head())


# Append our previous dataframes to have the real sales volume
df_empVentaNeteado = df_empVenta.append(df_regalosTraslados, ignore_index=True)


def grupo(codproducto):
    if codproducto == "GO" or codproducto == "EU":
        return "GASÓLEOS"
    elif codproducto == "NS" or codproducto == "NU":
        return "NAFTAS"
    else:
        return "GNC"

# Create column GRUPO from column CODPRODUCTO
df_empVentaNeteado["GRUPO"] = df_empVentaNeteado.apply(
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
df_empVentaNeteado["GRUPO"] = df_empVentaNeteado["GRUPO"].astype(categoriaGrupo)

# Pivot table of data to get results of VTATOTVOL per UEN grouped by Grupo
df_resultados = pd.pivot_table(df_empVentaNeteado
    , values="VTATOTVOL"
    , index="UEN"
    , columns="GRUPO"
    , aggfunc=sum
    , fill_value=0
    , margins=True
    , margins_name="TOTAL"
)
# Eliminate column "TOTAL"
df_resultados = df_resultados.iloc[:, :-1]
# Restore index UEN like a column
df_resultados = df_resultados.reset_index(level=0)
#print(df_resultados)


##############
# STYLING of the dataframe
##############

df_conEstilo_resultados = \
    df_resultados.style \
        .format("{0:,.0f}",subset=["GASÓLEOS","NAFTAS","GNC"]) \
        .hide_index() \
        .set_caption("VOLUMEN DE VENTAS"
            +" "
            +((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d-%m-%y"))
        ) \
        .set_properties(subset=["GASÓLEOS", "NAFTAS", "GNC"]
            , **{"text-align": "center", "width": "100px"}) \
        .set_properties(border= "2px solid black") \
        .set_table_styles([
            {"selector": "caption", "props": 
                [("font-size", "20px")
                ]
            }
            , {"selector": "th", "props": 
                [("text-align", "center")
                    ,("background-color","black")
                    ,("color","white")
                ]
            }
        ]) \
        .apply(lambda x: ["background: black" if x.name == 14 else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == 14 else "" for i in x]
            , axis=1)

# NOTE: display() will show styler object in Jupyter
try:
    display(df_conEstilo_resultados) # type: ignore
except:
    print("")


##############
# PRINTING dataframe as an image
##############

# This will print the df with a unique name and will erase the old image 
# everytime the script is run

ubicacion = "C:\Informes\InfoVtaCombustibles\\"

if os.path.exists(ubicacion+"Info_VolumenVentas.png"):
    os.remove(ubicacion+"Info_VolumenVentas.png")
    dfi.export(df_conEstilo_resultados, 
        ubicacion+"Info_VolumenVentas.png")
else:
    dfi.export(df_conEstilo_resultados, 
        ubicacion+"Info_VolumenVentas.png")


# Timer
tiempoFinal = pd.to_datetime("today")
print("\nInfo Volumen de Ventas"+"\nTiempo de Ejecucion Total:")
print(tiempoFinal-tiempoInicio)