import os
import math
import re
import numpy as np
from DatosLogin import login
from PIL import Image
import pandas as pd
import dataframe_image as dfi
import pyodbc #Library to connect to Microsoft SQL Server
from DatosLogin import login #Holds connection data to the SQL Server
import sys
import pathlib
from calendar import monthrange
from datetime import datetime
from datetime import timedelta
from InfoDeuda.DeudaClientesAyer import deuda as deudaAyer,total as totalAyer, adelantado as adelantadoAyer,precioGO as precioAyer
from InfoDeuda.DeudaClientesMesPasado import deuda as deudaMes,total as totalMes, adelantado as adelantadoMes,precioGO as precioMes
from InfoDeuda.DeudaClientesSemanaPasada import deuda as deudaSem,total as totalSem, adelantado as adelantadoSem,precioGO as precioSem
import datetime
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))
import logging
import matplotlib.pyplot as plt
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


### Creo dataframe de raio
ratioMens = pd.DataFrame()
ratioMens = ratioMens.assign(Incremento_Mensual=[])
### Creo dataframe de Deudas
deudaEvol = pd.DataFrame()
deudaEvol = ratioMens.assign(Incremento_Mensual=[])
### Creo dataframe de Deudas
adelEvol = pd.DataFrame()
adelEvol = adelEvol.assign(Incremento_Mensual=[])
## Reseteo Indices
totalAyer=totalAyer.reset_index()
totalSem=totalSem.reset_index()
totalMes=totalMes.reset_index()
## Promedio Dias de Venta Aduedado
deudaAyer=deudaAyer.loc[(deudaAyer['Días Desde Última Compra'] < 60),:]
deudaSem=deudaSem.loc[(deudaSem['Días Desde Última Compra'] < 60),:]
deudaMes=deudaMes.loc[(deudaMes['Días Desde Última Compra'] < 60),:]
promDiasVtaAyer=deudaAyer['Dias de Venta Adeudada'].mean()
promDiasVtaSem=deudaSem['Dias de Venta Adeudada'].mean()
promDiasVtaMes=deudaMes['Dias de Venta Adeudada'].mean()
## Creo tabla Deudores
deudaEvol.loc[0,'Estado Deuda']='Deuda Clientes'
deudaEvol.loc[0,'Ayer']=totalAyer.loc[4,'SALDOCUENTA']
deudaEvol.loc[0,'Semana Anterior']=totalSem.loc[4,'SALDOCUENTA']
deudaEvol.loc[0,'Mes Anterior']=totalMes.loc[4,'SALDOCUENTA']

deudaEvol.loc[1,'Estado Deuda']='Adelanto Clientes'
deudaEvol.loc[1,'Ayer']=adelantadoAyer
deudaEvol.loc[1,'Semana Anterior']=adelantadoSem
deudaEvol.loc[1,'Mes Anterior']=adelantadoMes

deudaEvol.loc[2,'Estado Deuda']='Balance'
deudaEvol.loc[2,'Ayer']=adelantadoAyer+totalAyer.loc[4,'SALDOCUENTA']
deudaEvol.loc[2,'Semana Anterior']=adelantadoSem+totalSem.loc[4,'SALDOCUENTA']
deudaEvol.loc[2,'Mes Anterior']=adelantadoMes+totalMes.loc[4,'SALDOCUENTA']
volumenDeuda=deudaEvol

deudaEvol.loc[3,'Estado Deuda']='Promedio Dias de Venta Adeudada'
deudaEvol.loc[3,'Ayer']=int(promDiasVtaAyer)
deudaEvol.loc[3,'Semana Anterior']=int(promDiasVtaSem)
deudaEvol.loc[3,'Mes Anterior']=int(promDiasVtaMes)

#### Ratios Deuda
deudaEvol['Variacion InterMensual %']=((deudaEvol['Ayer']/deudaEvol['Mes Anterior'])-1)*-1
deudaEvol['Variacion InterSemanal %']=((deudaEvol['Ayer']/deudaEvol['Semana Anterior'])-1)*-1
deudaEvol['Variacion InterMensual']=(deudaEvol['Ayer']-deudaEvol['Mes Anterior'])
deudaEvol['Variacion InterSemanal']=(deudaEvol['Ayer']-deudaEvol['Semana Anterior'])

deudaEvol = deudaEvol.reset_index()
deudaEvol=deudaEvol.reindex(columns=['Estado Deuda','Ayer','Semana Anterior','Variacion InterSemanal'
                                     ,'Variacion InterSemanal %' ,'Mes Anterior','Variacion InterMensual'
                                     ,'Variacion InterMensual %'])

########## Cambio la fila 1
if deudaEvol.loc[1, 'Variacion InterSemanal %'] < 0:
    deudaEvol.loc[1, 'Variacion InterSemanal %'] = abs(deudaEvol.loc[1, 'Variacion InterSemanal %'])
else:
    deudaEvol.loc[1, 'Variacion InterSemanal %'] = -1 * deudaEvol.loc[1, 'Variacion InterSemanal %']

if deudaEvol.loc[1, 'Variacion InterMensual %'] < 0:
    deudaEvol.loc[1, 'Variacion InterMensual %'] = abs(deudaEvol.loc[1, 'Variacion InterMensual %'])
else:
    deudaEvol.loc[1, 'Variacion InterMensual %'] = -1 * deudaEvol.loc[1, 'Variacion InterMensual %']

########### Cambio La fila 4
if deudaEvol.loc[3, 'Variacion InterSemanal'] < 0:
    deudaEvol.loc[3, 'Variacion InterSemanal'] = abs(deudaEvol.loc[3, 'Variacion InterSemanal'])
else:
    deudaEvol.loc[3, 'Variacion InterSemanal'] = -1 * deudaEvol.loc[3, 'Variacion InterSemanal']

if deudaEvol.loc[3, 'Variacion InterMensual'] < 0:
    deudaEvol.loc[3, 'Variacion InterMensual'] = abs(deudaEvol.loc[3, 'Variacion InterMensual'])
else:
    deudaEvol.loc[3, 'Variacion InterMensual'] = -1 * deudaEvol.loc[3, 'Variacion InterMensual']

####################################### TABLA VOLUMENES #############################################################

volumenDeuda['Ayer']=volumenDeuda['Ayer']/precioAyer
volumenDeuda['Semana Anterior']=volumenDeuda['Semana Anterior']/precioSem
volumenDeuda['Mes Anterior']=volumenDeuda['Mes Anterior']/precioMes

volumenDeuda['Variacion InterMensual %']=((volumenDeuda['Ayer']/volumenDeuda['Mes Anterior'])-1)*-1
volumenDeuda['Variacion InterSemanal %']=((volumenDeuda['Ayer']/volumenDeuda['Semana Anterior'])-1)*-1
volumenDeuda['Variacion InterMensual']=(volumenDeuda['Ayer']-volumenDeuda['Mes Anterior'])
volumenDeuda['Variacion InterSemanal']=(volumenDeuda['Ayer']-volumenDeuda['Semana Anterior'])
volumenDeuda = volumenDeuda.reset_index()
volumenDeuda=volumenDeuda.reindex(columns=['Estado Deuda','Ayer','Semana Anterior','Variacion InterSemanal'
                                     ,'Variacion InterSemanal %' ,'Mes Anterior','Variacion InterMensual'
                                     ,'Variacion InterMensual %'])

if volumenDeuda.loc[1, 'Variacion InterSemanal %'] < 0:
    volumenDeuda.loc[1, 'Variacion InterSemanal %'] = abs(volumenDeuda.loc[1, 'Variacion InterSemanal %'])
else:
    volumenDeuda.loc[1, 'Variacion InterSemanal %'] = -1 * volumenDeuda.loc[1, 'Variacion InterSemanal %']

if volumenDeuda.loc[1, 'Variacion InterMensual %'] < 0:
    volumenDeuda.loc[1, 'Variacion InterMensual %'] = abs(volumenDeuda.loc[1, 'Variacion InterMensual %'])
else:
    volumenDeuda.loc[1, 'Variacion InterMensual %'] = -1 * volumenDeuda.loc[1, 'Variacion InterMensual %']
    
volumenDeuda = volumenDeuda.drop(index=volumenDeuda.index[-1])
######### LE DOY FORMATO AL DATAFRAME
def _estiladorVtaTituloDTotalVOLUMEN(df,list_Col_Numpes,listaporcentaje, titulo):
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
        .format("{0:,.0f} L", subset=list_Col_Numpes) \
        .format("{:,.2%}", subset=listaporcentaje) \
        .hide_index() \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>") \
        .set_properties(subset= list_Col_Numpes+listaporcentaje
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
    resultado = resultado.background_gradient(
        cmap="RdYlGn" # Red->Yellow->Green
        ,vmin=-0.05
        ,vmax=0.05
        ,subset=pd.IndexSlice["Variacion InterSemanal %"]
    )
    resultado = resultado.background_gradient(
        cmap="RdYlGn" # Red->Yellow->Green
        ,vmin=-0.05
        ,vmax=0.05
        ,subset=pd.IndexSlice["Variacion InterMensual %"]
    )
    resultado = resultado.background_gradient(
        cmap="RdYlGn" # Red->Yellow->Green
        ,vmin=-0.05
        ,vmax=0.05
        ,subset=pd.IndexSlice['Variacion InterSemanal']
    )
    resultado = resultado.background_gradient(
        cmap="RdYlGn" # Red->Yellow->Green
        ,vmin=-0.05
        ,vmax=0.05
        ,subset=pd.IndexSlice['Variacion InterMensual']
    )
    return resultado




def _estiladorVtaTituloDTotal(df,list_Col_Numpes,listaporcentaje, titulo):
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
    evitarTotales = df.index.get_level_values(0)
    resultado = df.style \
        .format("{0:,.0f}", subset=list_Col_Numpes+listaporcentaje) \
        .format("{:,.2%}", subset=listaporcentaje) \
        .format("$ {0:,.0f}", subset=pd.IndexSlice[evitarTotales[:-1],'Ayer']) \
        .format("$ {0:,.0f}", subset=pd.IndexSlice[evitarTotales[:-1],'Semana Anterior']) \
        .format("$ {0:,.0f}", subset=pd.IndexSlice[evitarTotales[:-1],'Mes Anterior']) \
        .format("$ {0:,.0f}", subset=pd.IndexSlice[evitarTotales[:-1],'Variacion InterSemanal']) \
        .format("$ {0:,.0f}", subset=pd.IndexSlice[evitarTotales[:-1],'Variacion InterMensual']) \
        .hide_index() \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>") \
        .set_properties(subset= list_Col_Numpes+listaporcentaje
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
    resultado = resultado.background_gradient(
        cmap="RdYlGn" # Red->Yellow->Green
        ,vmin=-0.05
        ,vmax=0.05
        ,subset=pd.IndexSlice["Variacion InterSemanal %"]
    )
    resultado = resultado.background_gradient(
        cmap="RdYlGn" # Red->Yellow->Green
        ,vmin=-0.05
        ,vmax=0.05
        ,subset=pd.IndexSlice["Variacion InterMensual %"]
    )
    resultado = resultado.background_gradient(
        cmap="RdYlGn" # Red->Yellow->Green
        ,vmin=-0.05
        ,vmax=0.05
        ,subset=pd.IndexSlice['Variacion InterSemanal']
    )
    resultado = resultado.background_gradient(
        cmap="RdYlGn" # Red->Yellow->Green
        ,vmin=-0.05
        ,vmax=0.05
        ,subset=pd.IndexSlice['Variacion InterMensual']
    )
    return resultado

#  columnas sin decimales

colpesos1=['Ayer','Semana Anterior','Mes Anterior','Variacion InterSemanal','Variacion InterMensual']
# Columnas Porcentaje

percCol2 = ['Variacion InterSemanal %','Variacion InterMensual %']

deudaEvol = _estiladorVtaTituloDTotal(deudaEvol,colpesos1,percCol2, "Estado de Deuda")
volumenDeuda = _estiladorVtaTituloDTotalVOLUMEN(volumenDeuda,colpesos1,percCol2, "Estado de Deuda en Volumenes")

###### DEFINO NOMBRE Y UBICACION DE DONDE SE VA A GUARDAR LA IMAGEN

ubicacion = "C:/Informes/InfoDeuda/"
nombreMes = 'Deuda Comercial Mes Anterior.png'
nombreSem = 'accreedores.png'
nombreAyer = 'deuda.png'
nombreratio='TablaDeuda.png'
nombreVolum='TablaDeudaVolumen.png'
# Creo una imagen en funcion al dataframe 
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

df_to_image(deudaEvol, ubicacion, nombreratio)
df_to_image(volumenDeuda, ubicacion, nombreVolum)


#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)

