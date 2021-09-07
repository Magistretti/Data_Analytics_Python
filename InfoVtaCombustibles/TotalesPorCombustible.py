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

# # Creating an ordered categorical type of GRUPO for when we need to show
# # an ordered pivot_table by category
# categoriaGrupo = CategoricalDtype(
#     categories=[
#         "GASÓLEOS"
#         ,"NAFTAS"
#         ,"GNC"
#     ], ordered=True
# )
#
# # Casting GRUPO column as ordered categorical
# df_empVentaNeteado["GRUPO"] = df_empVentaNeteado["GRUPO"].astype(categoriaGrupo)

# Pivot table of data to get results of VTATOTVOL per UEN grouped by Grupo
df_resultados = pd.pivot_table(df_empVentaNeteado
    , values="VTATOTVOL"
    , index="UEN"
    , columns="GRUPO"
    , aggfunc=sum
    , fill_value=0
    # , margins=True
    # , margins_name="TOTAL"
)
# If we add margins then we eliminate column "TOTAL"
# df_resultados = df_resultados.iloc[:, :-1]

# Restore index UEN like a column
df_resultados = df_resultados.reset_index()
# print(df_resultados)

######################
# Now we will split the pivot table by group to get 3 set of data ordered in
# descending manner
######################

####### GASÓLEOS #######

# Filter and sort by Descending order
df_resultadosGOEU = df_resultados[["UEN","GASÓLEOS"]].sort_values(
    by=["GASÓLEOS"]
    , ascending=False
)
# Creating a total row
df_resultadosGOEU.loc["colTOTAL"]= pd.Series(
    df_resultadosGOEU["GASÓLEOS"].sum()
    , index=["GASÓLEOS"]
)
# Give name to total row in the "UEN" column filling the NaN
df_resultadosGOEU = df_resultadosGOEU.fillna({"UEN":"TOTAL"})

####### NAFTAS #######

df_resultadosNSNU = df_resultados[["UEN","NAFTAS"]].sort_values(
    by=["NAFTAS"]
    , ascending=False
)
df_resultadosNSNU.loc["colTOTAL"]= pd.Series(
    df_resultadosNSNU["NAFTAS"].sum()
    , index=["NAFTAS"]
)
df_resultadosNSNU = df_resultadosNSNU.fillna({"UEN":"TOTAL"})

####### GNC #######

df_resultadosGNC = df_resultados[["UEN","GNC"]].sort_values(
    by=["GNC"]
    , ascending=False
)
df_resultadosGNC.loc["colTOTAL"]= pd.Series(
    df_resultadosGNC["GNC"].sum()
    , index=["GNC"]
)
df_resultadosGNC = df_resultadosGNC.fillna({"UEN":"TOTAL"})


##############
# STYLING of the dataframe
##############

def estiladorVtaTitulo(df,columnaValores):
    resultado = df.style \
        .format("{0:,.0f}", subset=[columnaValores]) \
        .hide_index() \
        .set_caption("VOLUMEN DE VENTAS"
            +"\n"
            +((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
        ) \
        .set_properties(subset=[columnaValores]
            , **{"text-align": "center", "width": "100px"}) \
        .set_properties(border= "2px solid black") \
        .set_table_styles([
            {"selector": "caption", 
                "props": [
                    ("font-size", "20px")
                    ,("text-align", "center")
                ]
            }
            , {"selector": "th", 
                "props": [
                    ("text-align", "center")
                    ,("background-color","black")
                    ,("color","white")
                ]
            }
        ]) \
        .apply(lambda x: ["background: black" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1)
    return resultado

def estiladorVtaSinTitulo(df,columnaValores):
    resultado = df.style \
        .format("{0:,.0f}", subset=[columnaValores]) \
        .hide_index() \
        .set_properties(subset=[columnaValores]
            , **{"text-align": "center", "width": "100px"}) \
        .set_properties(border= "2px solid black") \
        .set_table_styles([
            {"selector": "th",
                "props": [
                    ("text-align", "center")
                    ,("background-color","black")
                    ,("color","white")
                ]
            }
        ]) \
        .apply(lambda x: ["background: black" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1)
    return resultado

df_resultadosGOEU_Estilo = estiladorVtaSinTitulo(df_resultadosGOEU, "GASÓLEOS")
df_resultadosNSNU_Estilo = estiladorVtaTitulo(df_resultadosNSNU, "NAFTAS")
df_resultadosGNC_Estilo = estiladorVtaSinTitulo(df_resultadosGNC, "GNC")


# NOTE: display() will show styler object in Jupyter
try:
    display(df_resultadosGOEU_Estilo) # type: ignore
    display(df_resultadosNSNU_Estilo) # type: ignore
    display(df_resultadosGNC_Estilo) # type: ignore
except:
    print("")


##############
# PRINTING dataframe as an image
##############

# This will print the df with a unique name and will erase the old image 
# everytime the script is run

ubicacion = "C:\Informes\InfoVtaCombustibles\\"
nombreGOEU = "Info_VolVtas_Gasoleos.png"
nombreNSNU = "Info_VolVtas_Naftas.png"
nombreGNC = "Info_VolVtas_GNC.png"

def df_to_image(df, ubicacion, nombre):
    if os.path.exists(ubicacion+nombre):
        os.remove(ubicacion+nombre)
        dfi.export(df, ubicacion+nombre)
    else:
        dfi.export(df, ubicacion+nombre)


df_to_image(df_resultadosGOEU_Estilo, ubicacion, nombreGOEU)
df_to_image(df_resultadosNSNU_Estilo, ubicacion, nombreNSNU)
df_to_image(df_resultadosGNC_Estilo, ubicacion, nombreGNC)

#import sys
from PIL import Image

images = [Image.open(x) for x in [ubicacion+nombreGOEU, ubicacion+nombreNSNU, ubicacion+nombreGNC]]
widths, heights = zip(*(i.size for i in images))

total_width = sum(widths)
max_height = max(heights)

new_im = Image.new('RGB', (total_width, max_height))

x_offset = 0
for im in images:
  new_im.paste(im, (x_offset,0))
  x_offset += im.size[0]

new_im.save('test.jpg')

# Timer
tiempoFinal = pd.to_datetime("today")
print("\nInfo Volumen de Ventas"+"\nTiempo de Ejecucion Total:")
print(tiempoFinal-tiempoInicio)