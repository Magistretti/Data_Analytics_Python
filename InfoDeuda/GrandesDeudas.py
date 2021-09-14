###################################
#
#     INFORME DEUDAS > $400.000,00
#
###################################

import os
from PIL import Image
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
    listaErrores = e.args[1].split(".")
    print("\nOcurrió un error al conectar a SQL Server:")
    for i in listaErrores:
        print(i)
    print("")
    exit()

########
# Convert dbo.FacCli SQL table to Dataframe
# 
# Requirements:
# -Filter unused columns
# -Show (FC.SALDOPREPAGO - FC.SALDOREMIPENDFACTU) AS "DEUDA TOTAL"
# -Show vendor name
# -Get data where (FC.SALDOPREPAGO - FC.SALDOREMIPENDFACTU) < '-400000'
# Extra:
# -Get "DEUDA TOTAL" per vendor
########

df_facCliDet = pd.read_sql("""
    SELECT
        FC.[NROCLIPRO] AS "N° CLIENTE"
        ,FC.[NOMBRE]
        ,FC.SALDOPREPAGO - FC.SALDOREMIPENDFACTU AS "DEUDA TOTAL"
        ,FC.[SALDOPREPAGO] AS "DEUDA FACTURADA"
        ,-FC.[SALDOREMIPENDFACTU] AS "DEUDA REMITADA"
        ,V.NOMBREVEND AS VENDEDOR
    FROM [Rumaos].[dbo].[FacCli] AS FC
        LEFT OUTER JOIN dbo.Vendedores AS V on FC.NROVEND = V.NROVEND
    where ListaSaldoCC = '1' 
        and (FC.SALDOPREPAGO - FC.SALDOREMIPENDFACTU) < '-400000'
    order by "DEUDA TOTAL"
""", db_conex)

df_facCliPorVend = pd.read_sql("""
    SELECT
        V.NOMBREVEND AS VENDEDOR
        ,sum(FC.SALDOPREPAGO - FC.SALDOREMIPENDFACTU) AS "DEUDA TOTAL"
        ,sum(FC.[SALDOPREPAGO]) AS "DEUDA FACTURADA"
        ,sum(-FC.[SALDOREMIPENDFACTU]) AS "DEUDA REMITADA"
    FROM [Rumaos].[dbo].[FacCli] AS FC
        LEFT OUTER JOIN dbo.Vendedores AS V on FC.NROVEND = V.NROVEND
    where ListaSaldoCC = '1' 
        and (FC.SALDOPREPAGO - FC.SALDOREMIPENDFACTU) < '-400000'
    group by V.NOMBREVEND
    order by "DEUDA TOTAL"
""", db_conex)

# If N° CLIENTE is cast as a string without casting as an integer first,
# it will return a decimal point.
df_facCliDet["N° CLIENTE"] = df_facCliDet["N° CLIENTE"].astype(int).astype(str)
df_facCliDet = df_facCliDet.convert_dtypes()
df_facCliPorVend = df_facCliPorVend.convert_dtypes()
# Whitespace cleaning
df_facCliDet["NOMBRE"] = df_facCliDet["NOMBRE"].str.strip()
df_facCliDet["VENDEDOR"] = df_facCliDet["VENDEDOR"].str.strip()    
df_facCliPorVend["VENDEDOR"] = df_facCliPorVend["VENDEDOR"].str.strip()    

# print(df_facCliDet.info())
# print(df_facCliPorVend.info())

# Creating Total row
df_facCliDet.loc["colTOTAL"]= pd.Series(df_facCliDet.sum())
df_facCliPorVend.loc["colTOTAL"]= pd.Series(df_facCliPorVend.sum())
# Cleaning NaN
df_facCliDet = df_facCliDet.fillna({
    "N° CLIENTE":""
    , "NOMBRE":"TOTAL"
    , "VENDEDOR":""
})
df_facCliPorVend = df_facCliPorVend.fillna({
    "VENDEDOR":"TOTAL"
})

# print(df_facCliDet.tail())
# print(df_facCliPorVend.tail())


##############
# STYLING of the dataframe
##############

def estiladorVtaTitulo(df,listaColNumericas,titulo):
    """
This function will return a styled dataframe that must be assign to a variable.
ARGS:
    df: Dataframe that will be styled.
    listaColNumericas: List of numeric columns that will be formatted with
    zero decimals and thousand separator.
    titulo: String for the table caption.
    """
    resultado = df.style \
        .format("{0:,.0f}", subset=listaColNumericas) \
        .hide_index() \
        .set_caption(titulo
            +"\n"
            +((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
        ) \
        .set_properties(subset=listaColNumericas
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

df_facCliDet_Estilo = estiladorVtaTitulo(df_facCliDet
    , ["DEUDA TOTAL","DEUDA FACTURADA","DEUDA REMITADA"]
    , "DEUDORES CON SALDO MAYOR A $400.000"
)
df_facCliPorVend_Estilo = estiladorVtaTitulo(df_facCliPorVend
    , ["DEUDA TOTAL","DEUDA FACTURADA","DEUDA REMITADA"]
    , "DEUDAS MAYORES A $400.000 POR VENDEDOR"
)

# NOTE: display() will show styler object in Interactive Window
try:
    display(df_facCliDet_Estilo) # type: ignore
    display(df_facCliPorVend_Estilo) # type: ignore
except:
    print("")


##############
# PRINTING dataframe as an image
##############

# This will print the df with a unique name and will erase the old image 
# everytime the script is run

ubicacion = "C:\Informes\InfoDeuda\\"
nombreFacCliDet = "Info_GrandesDeudores.png"
nombreFacCliPorVend = "Info_GrandesDeudasPorVend.png"

def df_to_image(df, ubicacion, nombre):
    """
    Esta función usa las biblioteca "dataframe_Image as dfi" y "os" para 
    generar un archivo .png de un dataframe. Si el archivo ya existe, este será
    reemplazado por el nuevo archivo.

    Args:
        df: dataframe a convertir
         ubicacion: ubicacion local donde se quiere grabar el archivo
          nombre: nombre del archivo incluyendo extensión .png (ej: "hello.png")

    """
    if os.path.exists(ubicacion+nombre):
        os.remove(ubicacion+nombre)
        dfi.export(df, ubicacion+nombre)
    else:
        dfi.export(df, ubicacion+nombre)


df_to_image(df_facCliDet_Estilo, ubicacion, nombreFacCliDet)
df_to_image(df_facCliPorVend_Estilo, ubicacion, nombreFacCliPorVend)


# Timer
tiempoFinal = pd.to_datetime("today")
print("\nInfo Grandes Deudas"+"\nTiempo de Ejecucion Total:")
print(tiempoFinal-tiempoInicio)
