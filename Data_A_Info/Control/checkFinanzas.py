import os
import math
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


from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import os
import sys
import pathlib
from datetime import date
import pandas as pd

hoy = date.today()

hoy=hoy.strftime("%d/%m/%Y")

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
KEY='C:/Informes/Control/check_kamel_Key.json'


SPREADSHEET_ID='1n6gNh0orQpbVXN73-C3XPoaaBY-1KIKSAqY7koaf2xM'

creds= None
creds = service_account.Credentials.from_service_account_file(KEY, scopes=SCOPES)

service = build('sheets','v4',credentials=creds)

sheet=service.spreadsheets() 

try:
    result= sheet.values().get(spreadsheetId=SPREADSHEET_ID,range='Verificacion Kamel!A1:AB').execute()
    values = result.get('values',[])
    # Convertir los valores en un DataFrame de pandas
    verificacionK = pd.DataFrame(values[1:], columns=values[0])
    verificacionK = verificacionK.loc[(verificacionK["FECHA"] == hoy),:]
    if isinstance(verificacionK, pd.DataFrame) and not verificacionK.empty and len(verificacionK.index) > 0:
        x=1
    else:
        verificacionK = pd.DataFrame()
        verificacionK.loc[0,'FECHA']= hoy
        verificacionK.loc[0,'INFORME']='Reportes Financieros'
        verificacionK.loc[0,'OBSERVACION']='No Checkeado'

except:
    verificacionK = pd.DataFrame()
    verificacionK.loc[0,'FECHA']= hoy
    verificacionK.loc[0,'INFORME']='Reportes Financieros'
    verificacionK.loc[0,'OBSERVACION']='No Checkeado'


try:
    result= sheet.values().get(spreadsheetId=SPREADSHEET_ID,range='Verificacion Eduardo!A1:AB').execute()
    values = result.get('values',[])
    # Convertir los valores en un DataFrame de pandas
    verificacionE = pd.DataFrame(values[1:], columns=values[0])
    verificacionE = verificacionE.loc[(verificacionE["FECHA"] == hoy),:]
    
    if isinstance(verificacionE, pd.DataFrame) and not verificacionE.empty and len(verificacionE.index) > 0:
        x=1
    else:
        verificacionE = pd.DataFrame()
        verificacionE.loc[0,'FECHA'] = hoy
        verificacionE.loc[0,'INFORME'] = 'Estado de Deuda'
        verificacionE.loc[0,'OBSERVACION'] ='No Checkeado'
    
    
except:
    
    verificacionE = pd.DataFrame()
    verificacionE.loc[0,'FECHA'] = hoy
    verificacionE.loc[0,'INFORME'] = 'Estado de Deuda'
    verificacionE.loc[0,'OBSERVACION'] = 'No Check'
    

def _estiladorVtaTitulo(df, list_Col_Num, list_Col_Perc, titulo):
    """
This function will return a styled dataframe that must be assign to a variable.
ARGS:
    df: Dataframe that will be styled.
    list_Col_Num: List of numeric columns that will be formatted with
    zero decimals and thousand separator.
    list_Col_Perc: List of numeric columns that will be formatted 
    as percentage.
    titulo: String for the table caption.
    """
    resultado = df.style \
        .format("{:,.2%}", subset=list_Col_Perc) \
        .hide(axis=0) \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio).strftime("%d/%m/%y"))
            + "<br>") \
        .set_properties(subset= list_Col_Perc + list_Col_Num
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

#### Defino columnas para cada Dataframe (Numericas)
numCols = ["INFORME"
            ,'OBSERVACION'
         ]
### COLUMNAS PARA INFORME PENETRACION
percColsPen = [
]

#### COLUMNAS INFORME EJECUCION PANADERIA PRESUPUESTADO DIARIO
percColsDiaria = [

]
###### Aplico el formato elegido a la imagen


checkE = _estiladorVtaTitulo(verificacionE,numCols,percColsPen, "Check Eduardo")
checkK = _estiladorVtaTitulo(verificacionK,numCols,percColsPen, "Check Kamel")


###### DEFINO NOMBRE Y UBICACION DE DONDE SE VA A GUARDAR LA IMAGEN

ubicacion = "C:/Informes/Control/"

nombrepng1 = "checkeo_Eduardo.png"
nombrepng2 = "checkeo_Kamel.png"

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

df_to_image(checkE, ubicacion, nombrepng1)
df_to_image(checkK, ubicacion, nombrepng2)

#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)









