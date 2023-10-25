import os
import math
import numpy as np
from DatosLogin import login
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

###HISTORIAL DE AFOROS
histAforosFinal= pd.read_sql('''
DECLARE @inicioMesActual DATETIME
	SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, CURRENT_TIMESTAMP), 0)

	DECLARE @inicioMesAnterior DATETIME
	SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

	--Divide por la cant de días del mes anterior y multiplica por la cant de días del
	--mes actual
	
	DECLARE @hoy DATETIME
	SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)
    
SELECT H.UEN, H.VOLINIMEC, H.VOLFINMEC, H.VOLINIELEC, H.VOLFINELEC, VOLINIAUTO, VOLFINAUTO
FROM dbo.HistAfor as H
WHERE H.FECHASQL = DATEADD(DAY,-1,CAST(GETDATE() AS date))
AND H.CODPRODUCTO='GNC'
AND H.TURNO=3 
ORDER BY H.UEN


''',db_conex
)
histAforosFinal= histAforosFinal.convert_dtypes()
histAforosFinal=histAforosFinal.groupby(
        ["UEN"]
        , as_index=False
    ).sum(numeric_only=True)

histAforosInicial= pd.read_sql('''
DECLARE @inicioMesActual DATETIME
	SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, CURRENT_TIMESTAMP), 0)

	DECLARE @inicioMesAnterior DATETIME
	SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

	--Divide por la cant de días del mes anterior y multiplica por la cant de días del
	--mes actual
	
	DECLARE @hoy DATETIME
	SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)
    
SELECT H.UEN, H.VOLINIMEC, H.VOLFINMEC, H.VOLINIELEC, H.VOLFINELEC, VOLINIAUTO, VOLFINAUTO
FROM dbo.HistAfor as H
WHERE H.FECHASQL = DATEADD(DAY,-8,CAST(GETDATE() AS date))
AND H.CODPRODUCTO='GNC'
AND H.TURNO=1
ORDER BY H.UEN


''',db_conex
)

histAforosInicial= histAforosInicial.convert_dtypes()
histAforosInicial=histAforosInicial.groupby(
        ["UEN"]
        , as_index=False
    ).sum(numeric_only=True)

def calcularCalibracion(histAforosInicial, histAforosFinal): 
    histAforosInicial['Vta s/Puente']= histAforosFinal['VOLFINMEC']-histAforosInicial['VOLINIMEC']
    histAforosInicial['Vta s/Elec']= histAforosFinal['VOLFINELEC']-histAforosInicial['VOLINIELEC']
    histAforosInicial['Vta s/Auto']= histAforosFinal['VOLFINAUTO']-histAforosInicial['VOLINIAUTO']
    #histAforosInicial = histAforosInicial.drop(histAforosInicial[histAforosInicial['Vta s/Puente'] == 0].index)
    #calibracionGNC=histAforosInicial.reindex(columns=['UEN', 'Vta s/Puente', 'Vta s/Elec', 'Vta s/Auto'])
    #calibracionGNC= histAforos
    calibracionGNC=histAforosInicial.reindex(columns=['UEN', 'Vta s/Puente', 'Vta s/Elec', 'Vta s/Auto'])

    calibracionGNC['Calibracion']=(histAforosInicial['Vta s/Elec']/histAforosInicial['Vta s/Puente'])-1
    calibracionGNC=calibracionGNC.reindex(columns=['UEN', 'Calibracion'])

    #calibracionGNC['Calibracion Auto']=(histAforosInicial['Vta s/Auto']/histAforosInicial['Vta s/Puente'])-1

    #    alerta = calibracionGNC.loc[(calibracionGNC['Calibracion Elec'] < 0.06) | (calibracionGNC['Calibracion Elec'] > 0.08) ]


    '''calibracionGNC.loc["colTOTAL"]= pd.Series(
        calibracionGNC.sum(numeric_only=True)
        ,index=['Vta s/Puente','Vta s/Elec','Vta s/Auto']
    )
    calibracionGNC.fillna({"UEN":"TOTAL"}, inplace=True)

    calibracionGNC.loc["colTOTAL", 'Calibracion Elec']= (calibracionGNC['Vta s/Elec'].sum()/calibracionGNC['Vta s/Puente'].sum())-1
    calibracionGNC.loc["colTOTAL", 'Calibracion Auto']= (calibracionGNC['Vta s/Auto'].sum()/calibracionGNC['Vta s/Puente'].sum())-1
    '''
    return calibracionGNC

'''calibracionGNC_Inicial = calcularCalibracion(histAforosInicial)
calibracionGNC_Final = calcularCalibracion(histAforosFinal)

alerta1 = calibracionGNC_Inicial.loc[(calibracionGNC_Inicial['Calibracion Elec'] < 0.06) | (calibracionGNC_Inicial['Calibracion Elec'] > 0.08) ]
alerta2 = calibracionGNC_Final.loc[(calibracionGNC_Final['Calibracion Elec'] < 0.06) | (calibracionGNC_Final['Calibracion Elec'] > 0.08) ]
'''

calibracionGNC=calcularCalibracion(histAforosInicial, histAforosFinal)

def _estiladorVtaTituloP(df,list_Col_Num, list_Col_Num0,list_Col_Perc, titulo, evitarTotal):
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
        .format("{0:,.0f}", subset=list_Col_Num0) \
        .format("{0:,.2f}", subset=list_Col_Num) \
        .format("{:,.2%}", subset=list_Col_Perc) \
        .hide(axis=0) \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(8,"days")).strftime("%d/%m/%y"))
            + " - "
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>") \
        .set_properties(subset= list_Col_Perc + list_Col_Num + list_Col_Num0
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
    
    evitarTotales = df.index.get_level_values(0) 
    if evitarTotal==1:
        subset_columns = pd.IndexSlice[evitarTotales[:-1],list_Col_Perc]
    else:
        subset_columns = pd.IndexSlice[list_Col_Perc]

        

    resultado= resultado.applymap(table_color,subset=subset_columns)
    
    return resultado

def table_color(val):
    """
    Takes a scalar and returns a string with
    the css property `'color: red'` for less than 60 marks, green otherwise.
    """
    if pd.notnull(val) and val > 0.0 and val < 0.1:
        color = 'blue'
    else:
        color = 'red'
    return 'color: % s' % color
##Columnas sin decimales
numCols0 = []
numCols1=[]
##Columnas con decimales

numCols = []

num=[]

## Columnas porcentajes
percColsPen = ['Calibracion']

alerta= _estiladorVtaTituloP(calibracionGNC, numCols, numCols0, percColsPen, 'INFO Calibracion GNC Alerta',0)



ubicacion = str(pathlib.Path(__file__).parent)+"\\"
nombreCalibracionGNC = "Calibracion_GNC_semanal.png"
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

df_to_image(alerta, ubicacion, nombreCalibracionGNC)



