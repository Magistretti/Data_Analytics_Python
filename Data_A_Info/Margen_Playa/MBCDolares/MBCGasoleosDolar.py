import os
import math
import re
import numpy as np
from Margen_Playa.MBCDolares.MBCGasoleosDolarHoy import mbcTOTALGO as mbcGO, mbcTotalEU as mbcEU
from Margen_Playa.MBCDolares.MBCGasoleosDolarAñoAnterior import mbcTotalGOAñoant as mbcGOAñoAnt, mbcTOTALEUAñoant as mbcEUAñoAnt
from DatosLogin import login
from Conectores import conectorMSSQL
from PIL import Image
from calendar import monthrange
import pandas as pd
from pandas.api.types import CategoricalDtype
import dataframe_image as dfi
import pyodbc #Library to connect to Microsoft SQL Server
from DatosLogin import login #Holds connection data to the SQL Server
import sys
import pathlib
from datetime import datetime
import datetime
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


#### Creo datos para volumen Proyectado
diasdelmes=(tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d")
mes=(tiempoInicio-pd.to_timedelta(1,"days")).strftime("%m")
año=(tiempoInicio-pd.to_timedelta(1,"days")).strftime("%y")
diasdelmes = int(diasdelmes)
mes=int(mes)
año=int(año)
num_days = monthrange(año,mes)[1] # num_days = 31.
num_days=int(num_days)

## Completo con 0
mbcGOAñoAnt=mbcGOAñoAnt.fillna(0)
mbcEUAñoAnt=mbcEUAñoAnt.fillna(0)
mbcGO=mbcGO.fillna(0)
mbcEU=mbcEU.fillna(0)
### Armo talba de GO
mbcGOAñoAnt = mbcGOAñoAnt.rename({'Volumen Total Vendido':'Volumen Total Vendido Interanual','Volumen YER':'Volumen YER Interanual','Total Vendido USD':'Total Vendido USD Interanual','Comision USD':'Comision USD Interanual'},axis=1)
mbcGOAñoAnt['Volumen Vendido Interanual']=mbcGOAñoAnt['Volumen Total Vendido Interanual']+mbcGOAñoAnt['Volumen YER Interanual']

mbcGO['Volumen Total Vendido Proyectado']=mbcGO['Volumen Total Vendido']/diasdelmes*num_days
mbcGO['Volumen YER Proyectado']=mbcGO['Volumen YER']/diasdelmes*num_days
mbcGO['Volumen Vendido Proyectado']=mbcGO['Volumen Total Vendido Proyectado']+mbcGO['Volumen YER Proyectado']
mbcGO['Comision USD Proyectado']=mbcGO['Comision USD']/diasdelmes*num_days
mbcGO['Total Vendido USD Proyectado']=mbcGO['Total Vendido USD']/diasdelmes*num_days

mbcGO=mbcGO.reindex(columns=['UEN','Volumen Vendido Proyectado','Volumen YER Proyectado','Comision USD Proyectado','Total Vendido USD Proyectado'])

mbcGO=mbcGO.merge(mbcGOAñoAnt,on='UEN',how='outer')
mbcGO=mbcGO.fillna(0)

mbcGO['Desvio Volumen']=mbcGO['Volumen Vendido Proyectado']-mbcGO['Volumen Vendido Interanual']
mbcGO['Desvio Volumen %']=(mbcGO['Desvio Volumen']/mbcGO['Volumen Vendido Interanual'])

mbcGO['Desvio Comision USD']=mbcGO['Comision USD Proyectado']-mbcGO['Comision USD Interanual']
mbcGO['Desvio Comision USD %']=(mbcGO['Desvio Comision USD']/mbcGO['Comision USD Interanual'])

for i in mbcGO.index:
    if mbcGO.loc[i,'Desvio Volumen %'] == 1.00:
        mbcGO['Desvio Comision \nUSD / L']= 1.0
    elif mbcGO.loc[i,'Desvio Volumen %'] == -1.00:
        mbcGO['Desvio Comision \nUSD / L']= -1.0
    else:
        mbcGO['Desvio Comision \nUSD / L']=((mbcGO['Comision USD Proyectado']/mbcGO['Volumen Vendido Proyectado'])/(mbcGO['Comision USD Interanual']/mbcGO['Volumen Vendido Interanual']))-1

mbcGO=mbcGO.reindex(columns=['UEN','Volumen Vendido Proyectado','Volumen Vendido Interanual'
                             ,'Desvio Volumen','Desvio Volumen %','Comision USD Proyectado'
                             ,'Comision USD Interanual','Desvio Comision USD','Desvio Comision USD %','Desvio Comision \nUSD / L'])

for i in mbcGO.index:
    if mbcGO.loc[i,'Desvio Volumen %']>1.00:
        mbcGO.loc[i,'Desvio Volumen %']=1.00
    elif mbcGO.loc[i,'Desvio Volumen %']<-1.00:
        mbcGO.loc[i,'Desvio Volumen %']=-1.00

    if mbcGO.loc[i,'Desvio Comision USD %']>1.00:
        mbcGO.loc[i,'Desvio Comision USD %']=1.00
    elif mbcGO.loc[i,'Desvio Comision USD %']<-1.00:
        mbcGO.loc[i,'Desvio Comision USD %']=-1.00

mbcGO[['Volumen Vendido Proyectado','Volumen Vendido Interanual'
                             ,'Desvio Volumen','Comision USD Proyectado'
                             ,'Comision USD Interanual','Desvio Comision USD']] = mbcGO[['Volumen Vendido Proyectado','Volumen Vendido Interanual'
                             ,'Desvio Volumen','Comision USD Proyectado'
                             ,'Comision USD Interanual','Desvio Comision USD']].astype(float)

### TOTALES GO

###### Columnas de Desvio y Totales GO
mbcGO.loc["colTOTAL"]= pd.Series(
    mbcGO.sum()
    , index=['Volumen Vendido Proyectado','Volumen Vendido Interanual'
                             ,'Desvio Volumen','Comision USD Proyectado'
                             ,'Comision USD Interanual','Desvio Comision USD']
)
mbcGO.fillna({"UEN":"TOTAL"}, inplace=True)

tasa = (mbcGO.loc["colTOTAL",'Desvio Volumen'] /
    mbcGO.loc["colTOTAL",'Volumen Vendido Interanual'])
mbcGO.fillna({"Desvio Volumen %":tasa}, inplace=True)

tasa2 = (mbcGO.loc["colTOTAL",'Desvio Comision USD'] /
    mbcGO.loc["colTOTAL",'Comision USD Interanual'])
mbcGO.fillna({'Desvio Comision USD %':tasa2}, inplace=True)

tasa3 = ((mbcGO.loc["colTOTAL",'Comision USD Proyectado'] /
    mbcGO.loc["colTOTAL",'Volumen Vendido Proyectado'])/(mbcGO.loc["colTOTAL",'Comision USD Interanual'] /
    mbcGO.loc["colTOTAL",'Volumen Vendido Interanual']))-1
mbcGO.fillna({'Desvio Comision \nUSD / L':tasa3}, inplace=True)




### Armo talba de EU
mbcEUAñoAnt = mbcEUAñoAnt.rename({'Volumen Total Vendido':'Volumen Total Vendido Interanual','Volumen YER':'Volumen YER Interanual','Total Vendido USD':'Total Vendido USD Interanual','Comision USD':'Comision USD Interanual'},axis=1)
mbcEUAñoAnt=mbcEUAñoAnt.fillna(0)
mbcEUAñoAnt['Volumen Vendido Interanual']=mbcEUAñoAnt['Volumen Total Vendido Interanual']+mbcEUAñoAnt['Volumen YER Interanual']

mbcEU['Volumen Total Vendido Proyectado']=mbcEU['Volumen Total Vendido']/diasdelmes*num_days
mbcEU['Volumen YER Proyectado']=mbcEU['Volumen YER']/diasdelmes*num_days
mbcEU['Volumen Vendido Proyectado']=mbcEU['Volumen Total Vendido Proyectado']+mbcEU['Volumen YER Proyectado']
mbcEU['Comision USD Proyectado']=mbcEU['Comision USD']/diasdelmes*num_days
mbcEU['Total Vendido USD Proyectado']=mbcEU['Total Vendido USD']/diasdelmes*num_days

mbcEU=mbcEU.reindex(columns=['UEN','Volumen Vendido Proyectado','Volumen YER Proyectado','Comision USD Proyectado','Total Vendido USD Proyectado'])

mbcEU=mbcEU.merge(mbcEUAñoAnt,on='UEN',how='outer')
mbcEU=mbcEU.fillna(0)
mbcEU['Desvio Volumen']=mbcEU['Volumen Vendido Proyectado']-mbcEU['Volumen Vendido Interanual']
mbcEU['Desvio Volumen %']=(mbcEU['Desvio Volumen']/mbcEU['Volumen Vendido Interanual'])

mbcEU['Desvio Comision USD']=mbcEU['Comision USD Proyectado']-mbcEU['Comision USD Interanual']
mbcEU['Desvio Comision USD %']=(mbcEU['Desvio Comision USD']/mbcEU['Comision USD Interanual'])

for i in mbcEU.index:
    if mbcEU.loc[i,'Desvio Volumen %'] == 1.00:
        mbcEU['Desvio Comision \nUSD / L']= 1.0
    elif mbcEU.loc[i,'Desvio Volumen %'] == -1.00:
        mbcEU['Desvio Comision \nUSD / L']= -1.0
    else:
        mbcEU['Desvio Comision \nUSD / L']=((mbcEU['Comision USD Proyectado']/mbcEU['Volumen Vendido Proyectado'])/(mbcEU['Comision USD Interanual']/mbcEU['Volumen Vendido Interanual']))-1


mbcEU=mbcEU.reindex(columns=['UEN','Volumen Vendido Proyectado','Volumen Vendido Interanual'
                             ,'Desvio Volumen','Desvio Volumen %','Comision USD Proyectado'
                             ,'Comision USD Interanual','Desvio Comision USD','Desvio Comision USD %','Desvio Comision \nUSD / L'])

for i in mbcEU.index:
    if mbcEU.loc[i,'Desvio Volumen %']>1.00:
        mbcEU.loc[i,'Desvio Volumen %']=1.00
    elif mbcEU.loc[i,'Desvio Volumen %']<-1.00:
        mbcEU.loc[i,'Desvio Volumen %']=-1.00

    if mbcEU.loc[i,'Desvio Comision USD %']>1.00:
        mbcEU.loc[i,'Desvio Comision USD %']=1.00
    elif mbcEU.loc[i,'Desvio Comision USD %']<-1.00:
        mbcEU.loc[i,'Desvio Comision USD %']=-1.00

### TOTALES EU
###### Columnas de Desvio y Totales EU
mbcEU.loc["colTOTAL"]= pd.Series(
    mbcEU.sum()
    , index=['Volumen Vendido Proyectado','Volumen Vendido Interanual'
                             ,'Desvio Volumen','Comision USD Proyectado'
                             ,'Comision USD Interanual','Desvio Comision USD']
)
mbcEU.fillna({"UEN":"TOTAL"}, inplace=True)

tasa = (mbcEU.loc["colTOTAL",'Desvio Volumen'] /
    mbcEU.loc["colTOTAL",'Volumen Vendido Interanual'])
mbcEU.fillna({"Desvio Volumen %":tasa}, inplace=True)

tasa2 = (mbcEU.loc["colTOTAL",'Desvio Comision USD'] /
    mbcEU.loc["colTOTAL",'Comision USD Interanual'])
mbcEU.fillna({'Desvio Comision USD %':tasa2}, inplace=True)

tasa3 = ((mbcEU.loc["colTOTAL",'Comision USD Proyectado'] /
    mbcEU.loc["colTOTAL",'Volumen Vendido Proyectado'])/(mbcEU.loc["colTOTAL",'Comision USD Interanual'] /
    mbcEU.loc["colTOTAL",'Volumen Vendido Interanual']))-1
mbcEU.fillna({'Desvio Comision \nUSD / L':tasa3}, inplace=True)

mbcGO = mbcGO.rename({'Desvio Volumen':'Variacion Volumen','Comision USD Proyectado':'MBC USD Proyectado',
                      'Comision USD Interanual':'MBC USD 2022','Desvio Comision USD':'Variacion MBC USD','Volumen Vendido Interanual':'Volumen Vendido 2022',
                      'Desvio Comision USD %':'Variacion MBC USD %','Desvio Volumen %':'Variacion Volumen %','Desvio Comision \nUSD / L':'Variacion MBC \nUSD / L'},axis=1)

mbcEU = mbcEU.rename({'Desvio Volumen':'Variacion Volumen','Comision USD Proyectado':'MBC USD Proyectado','Volumen Vendido Interanual':'Volumen Vendido 2022',
                      'Comision USD Interanual':'MBC USD 2022','Desvio Comision USD':'Variacion MBC USD',
                      'Desvio Comision USD %':'Variacion MBC USD %','Desvio Volumen %':'Variacion Volumen %','Desvio Comision \nUSD / L':'Variacion MBC \nUSD / L'},axis=1)

mbcEU = mbcEU.loc[(mbcEU["Volumen Vendido Proyectado"] > 100),:]

mbcGO= mbcGO.replace({'nan':1.00})
######### LE DOY FORMATO AL DATAFRAME
def _estiladorVtaTituloD(df, list_Col_Num, list_Col_Perc,presioCols, titulo):
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
        .format("{0:,.0f} L", subset=presioCols) \
        .format("$ {0:,.0f}", subset=list_Col_Num) \
        .format("{:,.2%}", subset=list_Col_Perc) \
        .hide_index() \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>") \
        .set_properties(subset= list_Col_Perc + list_Col_Num + presioCols
            , **{"text-align": "center", "width": "80px"}) \
        .set_properties(border= "2px solid black") \
        .set_table_styles([
            {"selector": "caption", 
                "props": [
                    ("font-size", "18px")
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
    resultado = resultado.background_gradient(
        cmap="RdYlGn" # Red->Yellow->Green
        ,vmin=-0.1
        ,vmax=0.05
        ,subset=pd.IndexSlice[evitarTotales[:-1],'Variacion MBC USD %']
    )
    resultado = resultado.background_gradient(
        cmap="RdYlGn" # Red->Yellow->Green
        ,vmin=-0.1
        ,vmax=0.05
        ,subset=pd.IndexSlice[evitarTotales[:-1],'Variacion Volumen %']
    )
    resultado = resultado.background_gradient(
        cmap="RdYlGn" # Red->Yellow->Green
        ,vmin=-0.1
        ,vmax=0.05
        ,subset=pd.IndexSlice[evitarTotales[:-1],'Variacion MBC \nUSD / L']
    )
    return resultado

#  columnas sin decimales
numCols = [ 'Volumen Vendido Proyectado','Volumen Vendido 2022','Variacion Volumen']
presioCols=['MBC USD Proyectado','MBC USD 2022','Variacion MBC USD']
# Columnas Porcentaje
percColsPen = ['Variacion MBC USD %','Variacion Volumen %','Variacion MBC \nUSD / L']

### APLICO EL FORMATO A LA TABLA
mbcTOTALGO = _estiladorVtaTituloD(mbcGO,presioCols,percColsPen,numCols, "Flujo Ultra Diesel")
mbcTotalEU = _estiladorVtaTituloD(mbcEU,presioCols,percColsPen,numCols, "Flujo Infinia Diesel")

ubicacion = "C:/Informes/Margen_Playa/"
nombreGO = "MBCDolarGO.png"
nombreEU = "MBCDolarEU.png"

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

df_to_image(mbcTOTALGO, ubicacion, nombreGO)
df_to_image(mbcTotalEU, ubicacion, nombreEU)

#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)