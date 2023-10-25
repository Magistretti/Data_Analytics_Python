import os
import math
import re
import numpy as np
from DatosLogin import login
from Conectores import conectorMSSQL
from PIL import Image
import pandas as pd
from pandas.api.types import CategoricalDtype
import dataframe_image as dfi
import pyodbc #Library to connect to Microsoft SQL Server
from DatosLogin import login #Holds connection data to the SQL Server
import sys
import pathlib
from datetime import datetime
from datetime import timedelta
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))
import logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)

#################################

tiempoInicio = pd.to_datetime("today")
#########
# Formating display of dataframes with comma separator for numbers
pd.options.display.float_format = "{:20,.0f}".format 
#########

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
    logger.error("\nOcurrió un error al conectar a SQL Server: ")
    for i in listaErrores:
        logger.error(i)
    exit()
    

controlDespa = pd.read_sql(   
'''	
    Select UEN, TURNO from VIEW_DESPAPRO_FILTRADA
	where FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
	group by TURNO, UEN


  '''      ,db_conex)
controlDespa = controlDespa.convert_dtypes()

turno1 = controlDespa['TURNO'] == "1"
turno1 = controlDespa[turno1]
turno1["TURNO"] = turno1["TURNO"].astype(int)
controlDespa["TURNO1"]= turno1['TURNO'] * 1

turno2 = controlDespa['TURNO'] == "2"
turno2 = controlDespa[turno2]
turno2["TURNO"] = turno2["TURNO"].astype(int)
controlDespa["TURNO2"]= (turno2['TURNO']-1) * 2

turno3 = controlDespa['TURNO'] == "3"
turno3 = controlDespa[turno3]
turno3["TURNO"] = turno3["TURNO"].astype(int)
controlDespa["TURNO3"]= (turno3['TURNO']-2) * 3

controlDespa = controlDespa.loc[:,controlDespa.columns!="TURNO"]
controlDespa = controlDespa.loc[:,controlDespa.columns!="index"]

controlDespa = controlDespa.groupby(
        ["UEN"]
        , as_index=False
    ).sum()
controlDespa = controlDespa.fillna('0')
controlDespa = controlDespa.replace({ 0 : 'Sin Datos', 1 : 'Con Datos', 2 : 'Con Datos', 3 : 'Con Datos'})

def dios(df,columnas,titulo):

    def asd(columna):

     
        return ['background-color: red' if i == 'Sin Datos' else 'background-color: green' for i in columna]
    resultado = df.style \
        .hide_index() \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>") \
        .set_properties(subset= columnas
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
        .apply(asd, subset=columnas, axis=0)
    return resultado
colum = ["TURNO1","TURNO2","TURNO3"]
controlDespa = dios(controlDespa, colum, "CONTROL EXISTENCIA DE DESPACHOS")






ubicacion = "C:/Informes/Control/"
nombreGNC = "Info_Presupuesto_GNC.png"
nombreGNCproy = "ControlDespa.png"
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

df_to_image(controlDespa, ubicacion, nombreGNCproy)



#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)












