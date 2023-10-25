import os
import math
import re
import numpy as np
from Margen_Playa.MBCDolares.MBCNaftasDolarHoy import mbcTOTALNS as mbcNS, mbcTotalNU as mbcNU
from Margen_Playa.MBCDolares.MBCNaftasDolarAñoAnterior import mbcTOTALNSAñoant as mbcNSAñoAnt, mbcTotalNUAñoant as mbcNUAñoAnt
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
mbcNSAñoAnt=mbcNSAñoAnt.fillna(0)
mbcNUAñoAnt=mbcNUAñoAnt.fillna(0)
mbcNS=mbcNS.fillna(0)
mbcNU=mbcNU.fillna(0)
### Armo talba de NS
mbcNSAñoAnt = mbcNSAñoAnt.rename({'Volumen Total Vendido':'Volumen Total Vendido Interanual','Volumen YER':'Volumen YER Interanual','Total Vendido USD':'Total Vendido USD Interanual','Comision USD':'Comision USD Interanual'},axis=1)
mbcNSAñoAnt['Volumen Vendido Interanual']=mbcNSAñoAnt['Volumen Total Vendido Interanual']+mbcNSAñoAnt['Volumen YER Interanual']

mbcNS['Volumen Total Vendido Proyectado']=mbcNS['Volumen Total Vendido']/diasdelmes*num_days
mbcNS['Volumen YER Proyectado']=mbcNS['Volumen YER']/diasdelmes*num_days
mbcNS['Volumen Vendido Proyectado']=mbcNS['Volumen Total Vendido Proyectado']+mbcNS['Volumen YER Proyectado']
mbcNS['Comision USD Proyectado']=mbcNS['Comision USD']/diasdelmes*num_days
mbcNS['Total Vendido USD Proyectado']=mbcNS['Total Vendido USD']/diasdelmes*num_days

mbcNS=mbcNS.reindex(columns=['UEN','Volumen Vendido Proyectado','Volumen YER Proyectado','Comision USD Proyectado','Total Vendido USD Proyectado'])

mbcNS=mbcNS.merge(mbcNSAñoAnt,on='UEN',how='outer')
mbcNS=mbcNS.fillna(0)
mbcNS['Desvio Volumen']=mbcNS['Volumen Vendido Proyectado']-mbcNS['Volumen Vendido Interanual']
mbcNS['Desvio Volumen %']=(mbcNS['Desvio Volumen']/mbcNS['Volumen Vendido Interanual'])

mbcNS['Desvio Comision USD']=mbcNS['Comision USD Proyectado']-mbcNS['Comision USD Interanual']
mbcNS['Desvio Comision USD %']=(mbcNS['Desvio Comision USD']/mbcNS['Comision USD Interanual'])

for i in mbcNS.index:
    if mbcNS.loc[i,'Volumen Vendido Interanual'] < 1:
        mbcNS.loc[i,'Desvio Comision \nUSD / L']= 1.0
    elif mbcNS.loc[i,'Volumen Vendido Proyectado']<1:
        mbcNS.loc[i,'Desvio Comision \nUSD / L']= -1.0
    else:
        mbcNS.loc[i,'Desvio Comision \nUSD / L']=((mbcNS.loc[i,'Comision USD Proyectado']/mbcNS.loc[i,'Volumen Vendido Proyectado'])/(mbcNS.loc[i,'Comision USD Interanual']/mbcNS.loc[i,'Volumen Vendido Interanual']))-1

mbcNS=mbcNS.reindex(columns=['UEN','Volumen Vendido Proyectado','Volumen Vendido Interanual'
                             ,'Desvio Volumen','Desvio Volumen %','Comision USD Proyectado'
                             ,'Comision USD Interanual','Desvio Comision USD','Desvio Comision USD %','Desvio Comision \nUSD / L'])

for i in mbcNS.index:
    if mbcNS.loc[i,'Desvio Volumen %']>1.00:
        mbcNS.loc[i,'Desvio Volumen %']=1.00
    elif mbcNS.loc[i,'Desvio Volumen %']<-1.00:
        mbcNS.loc[i,'Desvio Volumen %']=-1.00

    if mbcNS.loc[i,'Desvio Comision USD %']>1.00:
        mbcNS.loc[i,'Desvio Comision USD %']=1.00
    elif mbcNS.loc[i,'Desvio Comision USD %']<-1.00:
        mbcNS.loc[i,'Desvio Comision USD %']=-1.00

### TOTALES NS
###### Columnas de Desvio y Totales NS
mbcNS.loc["colTOTAL"]= pd.Series(
    mbcNS.sum()
    , index=['Volumen Vendido Proyectado','Volumen Vendido Interanual'
                             ,'Desvio Volumen','Comision USD Proyectado'
                             ,'Comision USD Interanual','Desvio Comision USD']
)
mbcNS.fillna({"UEN":"TOTAL"}, inplace=True)

tasa = (mbcNS.loc["colTOTAL",'Desvio Volumen'] /
    mbcNS.loc["colTOTAL",'Volumen Vendido Interanual'])
mbcNS.fillna({"Desvio Volumen %":tasa}, inplace=True)

tasa2 = (mbcNS.loc["colTOTAL",'Desvio Comision USD'] /
    mbcNS.loc["colTOTAL",'Comision USD Interanual'])
mbcNS.fillna({'Desvio Comision USD %':tasa2}, inplace=True)

tasa3 = ((mbcNS.loc["colTOTAL",'Comision USD Proyectado'] /
    mbcNS.loc["colTOTAL",'Volumen Vendido Proyectado'])/(mbcNS.loc["colTOTAL",'Comision USD Interanual'] /
    mbcNS.loc["colTOTAL",'Volumen Vendido Interanual']))-1
mbcNS.fillna({'Desvio Comision \nUSD / L':tasa3}, inplace=True)




### Armo talba de NU
mbcNUAñoAnt = mbcNUAñoAnt.rename({'Volumen Total Vendido':'Volumen Total Vendido Interanual','Volumen YER':'Volumen YER Interanual','Total Vendido USD':'Total Vendido USD Interanual','Comision USD':'Comision USD Interanual'},axis=1)
mbcNUAñoAnt['Volumen Vendido Interanual']=mbcNUAñoAnt['Volumen Total Vendido Interanual']+mbcNUAñoAnt['Volumen YER Interanual']

mbcNU['Volumen Total Vendido Proyectado']=mbcNU['Volumen Total Vendido']/diasdelmes*num_days
mbcNU['Volumen YER Proyectado']=mbcNU['Volumen YER']/diasdelmes*num_days
mbcNU['Volumen Vendido Proyectado']=mbcNU['Volumen Total Vendido Proyectado']+mbcNU['Volumen YER Proyectado']
mbcNU['Comision USD Proyectado']=mbcNU['Comision USD']/diasdelmes*num_days
mbcNU['Total Vendido USD Proyectado']=mbcNU['Total Vendido USD']/diasdelmes*num_days

mbcNU=mbcNU.reindex(columns=['UEN','Volumen Vendido Proyectado','Volumen YER Proyectado','Comision USD Proyectado','Total Vendido USD Proyectado'])

mbcNU=mbcNU.merge(mbcNUAñoAnt,on='UEN',how='outer')
mbcNU=mbcNU.fillna(0)
mbcNU['Desvio Volumen']=mbcNU['Volumen Vendido Proyectado']-mbcNU['Volumen Vendido Interanual']
mbcNU['Desvio Volumen %']=(mbcNU['Desvio Volumen']/mbcNU['Volumen Vendido Interanual'])

mbcNU['Desvio Comision USD']=mbcNU['Comision USD Proyectado']-mbcNU['Comision USD Interanual']
mbcNU['Desvio Comision USD %']=(mbcNU['Desvio Comision USD']/mbcNU['Comision USD Interanual'])


for i in mbcNU.index:
    if mbcNU.loc[i,'Volumen Vendido Interanual'] < 1:
        mbcNU.loc[i,'Desvio Comision \nUSD / L']= 1.0
    elif mbcNU.loc[i,'Volumen Vendido Proyectado']<1:
        mbcNU.loc[i,'Desvio Comision \nUSD / L']= -1.0
    else:
        mbcNU.loc[i,'Desvio Comision \nUSD / L']=((mbcNU.loc[i,'Comision USD Proyectado']/mbcNU.loc[i,'Volumen Vendido Proyectado'])/(mbcNU.loc[i,'Comision USD Interanual']/mbcNU.loc[i,'Volumen Vendido Interanual']))-1

mbcNU=mbcNU.reindex(columns=['UEN','Volumen Vendido Proyectado','Volumen Vendido Interanual'
                             ,'Desvio Volumen','Desvio Volumen %','Comision USD Proyectado'
                             ,'Comision USD Interanual','Desvio Comision USD','Desvio Comision USD %','Desvio Comision \nUSD / L'])

for i in mbcNU.index:
    if mbcNU.loc[i,'Desvio Volumen %']>1.00:
        mbcNU.loc[i,'Desvio Volumen %']=1.00
    elif mbcNU.loc[i,'Desvio Volumen %']<-1.00:
        mbcNU.loc[i,'Desvio Volumen %']=-1.00

    if mbcNU.loc[i,'Desvio Comision USD %']>1.00:
        mbcNU.loc[i,'Desvio Comision USD %']=1.00
    elif mbcNU.loc[i,'Desvio Comision USD %']<-1.00:
        mbcNU.loc[i,'Desvio Comision USD %']=-1.00

### TOTALES NU
###### Columnas de Desvio y Totales NU
mbcNU.loc["colTOTAL"]= pd.Series(
    mbcNU.sum()
    , index=['Volumen Vendido Proyectado','Volumen Vendido Interanual'
                             ,'Desvio Volumen','Comision USD Proyectado'
                             ,'Comision USD Interanual','Desvio Comision USD']
)
mbcNU.fillna({"UEN":"TOTAL"}, inplace=True)

tasa = (mbcNU.loc["colTOTAL",'Desvio Volumen'] /
    mbcNU.loc["colTOTAL",'Volumen Vendido Interanual'])
mbcNU.fillna({"Desvio Volumen %":tasa}, inplace=True)

tasa2 = (mbcNU.loc["colTOTAL",'Desvio Comision USD'] /
    mbcNU.loc["colTOTAL",'Comision USD Interanual'])
mbcNU.fillna({'Desvio Comision USD %':tasa2}, inplace=True)

tasa3 = ((mbcNU.loc["colTOTAL",'Comision USD Proyectado'] /
    mbcNU.loc["colTOTAL",'Volumen Vendido Proyectado'])/(mbcNU.loc["colTOTAL",'Comision USD Interanual'] /
    mbcNU.loc["colTOTAL",'Volumen Vendido Interanual']))-1
mbcNU.fillna({'Desvio Comision \nUSD / L':tasa3}, inplace=True)


mbcNS = mbcNS.rename({'Volumen Vendido Interanual':'Volumen Vendido 2022','Desvio Volumen':'Variacion Volumen','Comision USD Proyectado':'MBC USD Proyectado',
                      'Comision USD Interanual':'MBC USD 2022','Desvio Comision USD':'Variacion MBC USD',
                      'Desvio Comision USD %':'Variacion MBC USD %','Desvio Volumen %':'Variacion Volumen %','Desvio Comision \nUSD / L':'Variacion MBC \nUSD / L'},axis=1)

mbcNU = mbcNU.rename({'Volumen Vendido Interanual':'Volumen Vendido 2022','Desvio Volumen':'Variacion Volumen','Comision USD Proyectado':'MBC USD Proyectado',
                      'Comision USD Interanual':'MBC USD 2022','Desvio Comision USD':'Variacion MBC USD',
                      'Desvio Comision USD %':'Variacion MBC USD %','Desvio Volumen %':'Variacion Volumen %','Desvio Comision \nUSD / L':'Variacion MBC \nUSD / L'},axis=1)


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
            , **{"text-align": "center", "width": "90px"}) \
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
mbcTOTALNS = _estiladorVtaTituloD(mbcNS,presioCols,percColsPen,numCols, "MBC Nafta Super")
mbcTotalNU = _estiladorVtaTituloD(mbcNU,presioCols,percColsPen,numCols, "MBC Infinia Nafta")

ubicacion = "C:/Informes/Margen_Playa/"
nombreNS = "MBCDolarNS.png"
nombreNU = "MBCDolarNU.png"

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

df_to_image(mbcTOTALNS, ubicacion, nombreNS)
df_to_image(mbcTotalNU, ubicacion, nombreNU)

#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)