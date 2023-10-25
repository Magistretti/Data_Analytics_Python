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
    

controlVtas = pd.read_sql(   
'''	
SELECT  [UEN] , sum(VTATOTVOL) as 'Total Ventas'
  FROM [Rumaos].[dbo].[EmpVenta] where FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
  group by UEN

  '''      ,db_conex)
controlVtas = controlVtas.convert_dtypes()
controlVtas1 = pd.read_sql(   
'''	
SELECT  [UEN] , sum(VTATOTVOL) as 'Total Ventas'
  FROM [Rumaos].[dbo].[EmpVenta] where FECHASQL = DATEADD(day, -36, CAST(GETDATE() AS date))
  group by UEN

  '''      ,db_conex)
controlVtas1 = controlVtas1.convert_dtypes()
controlVtas2 = pd.read_sql(   
'''	
SELECT  [UEN] , sum(VTATOTVOL) as 'Total Ventas'
  FROM [Rumaos].[dbo].[EmpVenta] where FECHASQL = DATEADD(day, -8, CAST(GETDATE() AS date))
  group by UEN

  '''      ,db_conex)
controlVtas2 = controlVtas2.convert_dtypes()
controlVtas3 = pd.read_sql(   
'''	
SELECT  [UEN] , sum(VTATOTVOL) as 'Total Ventas'
  FROM [Rumaos].[dbo].[EmpVenta] where FECHASQL = DATEADD(day, -15, CAST(GETDATE() AS date))
  group by UEN

  '''      ,db_conex)
controlVtas3 = controlVtas3.convert_dtypes()
controlVtas4 = pd.read_sql(   
'''	
SELECT  [UEN] , sum(VTATOTVOL) as 'Total Ventas'
  FROM [Rumaos].[dbo].[EmpVenta] where FECHASQL = DATEADD(day, -29, CAST(GETDATE() AS date))
  group by UEN

  '''      ,db_conex)
controlVtas4 = controlVtas4.convert_dtypes()

controlVtas = controlVtas.merge(controlVtas1, on= 'UEN', how='outer')
controlVtas2= controlVtas2.merge(controlVtas3, on='UEN', how='outer')
controlVtas = controlVtas.merge(controlVtas2, on='UEN', how='outer')
controlVtasT = controlVtas.merge(controlVtas4, on= 'UEN', how= 'outer')

controlVtasT = controlVtasT.assign(PromedioVtas=(controlVtasT['Total Ventas_y_x']+controlVtasT['Total Ventas_x_y']
+ controlVtasT["Total Ventas_y_y"]+controlVtasT['Total Ventas'])/ 4)

controlVtasT = controlVtasT.assign(DesvioVtas= controlVtasT["Total Ventas_x_x"] - controlVtasT["PromedioVtas"])
controlVtasT = controlVtasT.assign(DesvioVtasPorc= controlVtasT["DesvioVtas"] / controlVtasT["PromedioVtas"])

controlVtasT = controlVtasT.loc[:,controlVtasT.columns!="Total Ventas_x_x"]
controlVtasT = controlVtasT.loc[:,controlVtasT.columns!="Total Ventas_y_x"]
controlVtasT = controlVtasT.loc[:,controlVtasT.columns!="Total Ventas_x_y"]
controlVtasT = controlVtasT.loc[:,controlVtasT.columns!="Total Ventas_y_y"]
controlVtasT = controlVtasT.loc[:,controlVtasT.columns!="DesvioVtas"]
controlVtasT = controlVtasT.loc[:,controlVtasT.columns!="Total Ventas"]
controlVtasT = controlVtasT.loc[:,controlVtasT.columns!="PromedioVtas"]


controlVtasT = controlVtasT.groupby(
        ["UEN"]
        , as_index=False
    ).sum()
controlVtasT = controlVtasT.fillna(0)
controlVtasT = controlVtasT.assign(DesvioVtas1= 'hola')



controlVtasT.loc[controlVtasT.DesvioVtasPorc<0.2,'DesvioVtas1']='Dentro del Parametro'
controlVtasT.loc[controlVtasT.DesvioVtasPorc>=0.2,'DesvioVtas1']='Fuera del Parametro +'
controlVtasT.loc[controlVtasT.DesvioVtasPorc<=-0.2,'DesvioVtas1']='Fuera del Parametro -'

#controlVtasT.loc[controlVtasT.DesvioVtasPorc>0.2 and controlVtasT.DesvioVtasPorc<-0.2 ,'DesvioVtas1']='Fuera del Parametro'
controlVtasT = controlVtasT.loc[:,controlVtasT.columns!="DesvioVtasPorc"]

controlVtasT = controlVtasT.rename({"UEN":"UEN","DesvioVtas1":"Datos de Ventas"}, axis=1)




def dios(df,columnas,titulo):

    def asdd(columna):

     
        return ['background-color: blue' if i == 'Fuera del Parametro +' else '' for i in columna]

    def asd(columna):

     
        return ['background-color: red' if i == 'Fuera del Parametro -' else 'background-color: green' for i in columna]
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
        .apply(asd, subset=columnas, axis=0)\
        .apply(asdd,subset=columnas,axis=0)
    return resultado

colum = ["Datos de Ventas"]
controlVtasT = dios(controlVtasT, colum, "CONTROL Datos Ventas")






ubicacion = "C:/Informes/Control/"
nombreGNC = "Info_Presupuesto_GNC.png"
nombreGNCproy = "ControlVtas.png"
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

df_to_image(controlVtasT, ubicacion, nombreGNCproy)



#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)
