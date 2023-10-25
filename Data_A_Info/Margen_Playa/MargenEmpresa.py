import os
import math
import re
import numpy as np
from DatosLogin import login
from PIL import Image
import pandas as pd
from pandas.api.types import CategoricalDtype
import dataframe_image as dfi
import pyodbc #Library to connect to Microsoft SQL Server
from DatosLogin import login #Holds connection data to the SQL Server
import sys
from Margen_Playa.MargenNaftasN import mbcNaftasNS
from Margen_Playa.MargenNaftasN import mbcNaftasNU
from Margen_Playa.MargenGasoleosN import mbcGasoleosGO
from Margen_Playa.MargenGasoleosN import mbcGasoleosEU
from Margen_Playa.MargenGNCN import mbcGNC
from Margen_Playa.ConsolidadoSalon import mbcSalon
from Margen_Playa.MargenLubricantes import mbcLubricantes
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


####### Rentas

gasoleosGO = mbcGasoleosGO
gasoleosGO = gasoleosGO.loc[gasoleosGO['UEN'] == 'TOTAL',:]
gasoleosGO.loc[gasoleosGO['UEN'] == 'TOTAL', 'UEN'] = 'Ultra Diesel'

gasoleosEU = mbcGasoleosEU
gasoleosEU = gasoleosEU.loc[gasoleosEU['UEN'] == 'TOTAL',:]
gasoleosEU.loc[gasoleosEU['UEN'] == 'TOTAL', 'UEN'] = 'Infinia Diesel'

naftasNS = mbcNaftasNS
naftasNS = naftasNS.loc[naftasNS['UEN'] == 'TOTAL',:]
naftasNS.loc[naftasNS['UEN'] == 'TOTAL', 'UEN'] = 'Nafta Super'

naftasNU = mbcNaftasNU
naftasNU = naftasNU.loc[naftasNU['UEN'] == 'TOTAL',:]
naftasNU.loc[naftasNU['UEN'] == 'TOTAL', 'UEN'] = 'Infinia Nafta'

gnc = mbcGNC
gnc = gnc.loc[gnc['UEN'] == 'TOTAL',:]
gnc.loc[gnc['UEN'] == 'TOTAL', 'UEN'] = 'GNC'

lubri = mbcLubricantes
lubri = lubri.loc[lubri['UEN'] == 'TOTAL',:]
lubri.loc[lubri['UEN'] == 'TOTAL', 'UEN'] = 'Lubricantes'

salon = mbcSalon
salon = salon.loc[salon['UEN'] == 'TOTAL',:]
salon.loc[salon['UEN'] == 'TOTAL', 'UEN'] = 'Salon'

## Concateno los totales de cada Negocio
consolidado = gasoleosGO.merge(gasoleosEU,on=['UEN','Presupuesto Acumulado $','Ventas Acumuladas $','Desvio Presupuestado %','MBC Presupuestado $','MBC Acumulado $','Desvio MBC %'],how='outer')
consolidado = consolidado.merge(naftasNS,on=['UEN','Presupuesto Acumulado $','Ventas Acumuladas $','Desvio Presupuestado %','MBC Presupuestado $','MBC Acumulado $','Desvio MBC %'],how='outer')
consolidado = consolidado.merge(naftasNU,on=['UEN','Presupuesto Acumulado $','Ventas Acumuladas $','Desvio Presupuestado %','MBC Presupuestado $','MBC Acumulado $','Desvio MBC %'],how='outer')
liquidos = consolidado
consolidado = consolidado.merge(gnc,on=['UEN','Presupuesto Acumulado $','Ventas Acumuladas $','Desvio Presupuestado %','MBC Presupuestado $','MBC Acumulado $','Desvio MBC %'],how='outer')
consolidado = consolidado.merge(lubri,on=['UEN','Presupuesto Acumulado $','Ventas Acumuladas $','Desvio Presupuestado %','MBC Presupuestado $','MBC Acumulado $','Desvio MBC %'],how='outer')
consolidado = consolidado.merge(salon,on=['UEN','Presupuesto Acumulado $','Ventas Acumuladas $','Desvio Presupuestado %','MBC Presupuestado $','MBC Acumulado $','Desvio MBC %'],how='outer')
periferia = consolidado.loc[(consolidado["UEN"] == 'GNC')|(consolidado["UEN"] == 'Salon')|(consolidado["UEN"] == 'Lubricantes'),:]

## Creo columna Subtotal Liquidos

liquidos.loc["colTOTAL"]= pd.Series(
    liquidos.sum()
    , index=['Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $']
)
liquidos.fillna({"UEN":"TOTAL"}, inplace=True)
tasa = (liquidos.loc["colTOTAL",'Ventas Acumuladas $'] /
    liquidos.loc["colTOTAL",'Presupuesto Acumulado $'])-1
liquidos.fillna({"Desvio Presupuestado %":tasa}, inplace=True)

tasa2 = (liquidos.loc["colTOTAL",'MBC Acumulado $'] /
    liquidos.loc["colTOTAL",'MBC Presupuestado $'])-1
liquidos.fillna({'Desvio MBC %':tasa2}, inplace=True)

liquidos = liquidos.loc[liquidos['UEN'] == 'TOTAL',:]
liquidos.loc[liquidos['UEN'] == 'TOTAL', 'UEN'] = 'SubTotal Liquidos'

## Creo columna subtotal Periferia

periferia.loc["colTOTAL"]= pd.Series(
    periferia.sum()
    , index=['Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $']
)
periferia.fillna({"UEN":"TOTAL"}, inplace=True)
tasa = (periferia.loc["colTOTAL",'Ventas Acumuladas $'] /
    periferia.loc["colTOTAL",'Presupuesto Acumulado $'])-1
periferia.fillna({"Desvio Presupuestado %":tasa}, inplace=True)

tasa2 = (periferia.loc["colTOTAL",'MBC Acumulado $'] /
    periferia.loc["colTOTAL",'MBC Presupuestado $'])-1
periferia.fillna({'Desvio MBC %':tasa2}, inplace=True)

periferia = periferia.loc[periferia['UEN'] == 'TOTAL',:]
periferia.loc[periferia['UEN'] == 'TOTAL', 'UEN'] = 'SubTotal Periferia'



## Creo columna Totales
consolidado.loc["colTOTAL"]= pd.Series(
    consolidado.sum()
    , index=['Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $']
)
consolidado.fillna({"UEN":"TOTAL"}, inplace=True)
tasa = (consolidado.loc["colTOTAL",'Ventas Acumuladas $'] /
    consolidado.loc["colTOTAL",'Presupuesto Acumulado $'])-1
consolidado.fillna({"Desvio Presupuestado %":tasa}, inplace=True)

tasa2 = (consolidado.loc["colTOTAL",'MBC Acumulado $'] /
    consolidado.loc["colTOTAL",'MBC Presupuestado $'])-1
consolidado.fillna({'Desvio MBC %':tasa2}, inplace=True)


# agregar una fila extra para el subtotal

consolidado = consolidado.merge(liquidos,on=['UEN','Presupuesto Acumulado $','Ventas Acumuladas $','Desvio Presupuestado %','MBC Presupuestado $','MBC Acumulado $','Desvio MBC %'],how='outer')
consolidado = consolidado.merge(periferia,on=['UEN','Presupuesto Acumulado $','Ventas Acumuladas $','Desvio Presupuestado %','MBC Presupuestado $','MBC Acumulado $','Desvio MBC %'],how='outer')

# reordenar las filas para que el subtotal aparezca entre panaderia y salon
consolidado['orden'] = consolidado['UEN'].map({'Ultra Diesel': 1, 'Infinia Diesel': 2, 'Nafta Super': 3,'Infinia Nafta':4,
                       'SubTotal Liquidos':5,'GNC':6,'Salon':7,'Lubricantes':8, 'SubTotal Periferia':9,'TOTAL':10})
consolidado = consolidado.sort_values('orden')
consolidado = consolidado.loc[:,consolidado.columns!="orden"]
consolidado = consolidado.rename({'UEN':'Negocio'},axis=1)

######### LE DOY FORMATO AL DATAFRAME
def _estiladorVtaTituloD(df, list_Col_Num, list_Col_Perc, titulo):
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
        .format("$ {0:,.0f}", subset=list_Col_Num) \
        .format("{:,.2%}", subset=list_Col_Perc) \
        .hide_index() \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>") \
        .applymap(lambda x: 'color: blue' if x >= 0 else 'color: #FF0000', subset=pd.IndexSlice[:, ['Desvio MBC %']]) \
        .applymap(lambda x: 'color: blue' if x >= 0 else 'color: #FF0000', subset=pd.IndexSlice[:, ['Desvio Presupuestado %']]) \
        .set_properties(subset= list_Col_Perc + list_Col_Num
            , **{"text-align": "center", "width": "120px"}) \
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
        .apply(lambda x: ["background: black" if x.name in [
            df.index[-1]
            ,df.index[-2]
            , df.index[4]
        ]
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name in [
            df.index[-1]
            ,df.index[-2]
            , df.index[4]
        ]
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["font-size: 15px" if x.name in [
            df.index[-1]
            ,df.index[-2]
            , df.index[4]
        ]
            else "" for i in x]
            , axis=1)  

    return resultado

#  columnas sin decimales
numCols = [ 'Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $']

# Columnas Porcentaje
percColsPen = ["Desvio MBC %"
            ,"Desvio Presupuestado %"
]


consolidado = _estiladorVtaTituloD(consolidado,numCols,percColsPen, "Consolidado Totales")
ubicacion = "C:/Informes/Margen_Playa/"
nombre = "ConsolidadoEmpresa.png"


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

df_to_image(consolidado, ubicacion, nombre)



#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)



