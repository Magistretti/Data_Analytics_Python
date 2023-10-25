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

##########################################
################ MENSUAL ACUMULADO
########################################

############### DESPACHOS ACUMULADOS MENSUAL por Empleado

df_despachosM = pd.read_sql('''
	
     DECLARE @ayer DATETIME
	SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))
	DECLARE @inicioMesActual DATETIME
	SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

        DECLARE @inicioMesAnterior DATETIME
        SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

        --Divide por la cant de días del mes anterior y multiplica por la cant de días del
        --mes actual
        
        DECLARE @hoy DATETIME
        SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

        --Divide por la cantidad de días cursados del mes actual y multiplica por la cant
        --de días del mes actual
    select a.UEN,sum(a.[Ventas Efectivo]+a.[Venta Cta Cte]+a.[ventas Promos]) as 'Despachos Totales' from (    
    SELECT UEN  
		,(VTAPRECARTELVOL) AS 'Ventas Efectivo'
		,(VTACTACTEVOL + VTAADELVOL) AS 'Venta Cta Cte'
		,(VTAPROMOSVOL) AS 'ventas Promos'
    FROM [Rumaos].[dbo].[EmpVenta]
    WHERE FECHASQL >= '2023-01-07'
        AND FECHASQL < @hoy
	    AND VTATOTVOL > '0'
		AND (CODPRODUCTO != 'GNC' AND CODPRODUCTO != 'GO')
		and UEN IN (
            'LAMADRID'
            ,'AZCUENAGA'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
            ,'SAN JOSE'
        )) as a
		group BY a.UEN
		order by a.UEN


''',db_conex)
df_despachosM = df_despachosM.convert_dtypes()



df_VentasYPFAPPGO = pd.read_sql('''
         DECLARE @ayer DATETIME
	SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))
	DECLARE @inicioMesActual DATETIME
	SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

        DECLARE @inicioMesAnterior DATETIME
        SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

        --Divide por la cant de días del mes anterior y multiplica por la cant de días del
        --mes actual
        
        DECLARE @hoy DATETIME
        SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

        --Divide por la cantidad de días cursados del mes actual y multiplica por la cant
        --de días del mes actual
        
SELECT count(distinct Y.ID) AS 'cant despachos', Y.UEN,sum(d.VOLUMEN) as 'Despachos APPYPFGO'
  FROM [MercadoPago].[dbo].[Transacciones]  as Y
  join [Rumaos].[dbo].[VIEW_DESPAPRO_FILTRADA] as D
  on Y.UEN = D.UEN
  AND Y.IDDESPACHO = D.IDMOVIM
    where
	FECHASQL >= '2023-01-07'
    AND FECHASQL < @hoy
	and d.CODPRODUCTO = 'GO'
	and AppYPF = 1
	group by Y.UEN		order by y.UEN
  '''      ,db_conex)
df_VentasYPFAPPGO = df_VentasYPFAPPGO.convert_dtypes()



df_despachosM = df_VentasYPFAPPGO.merge(df_despachosM,on=['UEN'],how='outer')
df_despachosM = df_despachosM.fillna(0)
df_despachosM['Despachos Base']=df_despachosM['Despachos Totales']-df_despachosM['Despachos APPYPFGO']


############### VENTA Con App YPF por Empleado 


df_VentasYPFAPP = pd.read_sql('''
            DECLARE @ayer DATETIME
	SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))
	DECLARE @inicioMesActual DATETIME
	SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

        DECLARE @inicioMesAnterior DATETIME
        SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

        --Divide por la cant de días del mes anterior y multiplica por la cant de días del
        --mes actual
        
        DECLARE @hoy DATETIME
        SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

        --Divide por la cantidad de días cursados del mes actual y multiplica por la cant
        --de días del mes actual
        
SELECT count(distinct Y.ID) AS 'cant despachos', Y.UEN,sum(d.VOLUMEN) as 'Despachos APPYPF'
  FROM [MercadoPago].[dbo].[Transacciones]  as Y
  join [Rumaos].[dbo].[VIEW_DESPAPRO_FILTRADA] as D
  on Y.UEN = D.UEN
  AND Y.IDDESPACHO = D.IDMOVIM
    where
	FECHASQL >= '2023-01-07'
    AND FECHASQL < @hoy
    AND (D.CODPRODUCTO != 'GO' AND D.CODPRODUCTO != 'GNC')
	and AppYPF = 1
	group by Y.UEN		order by y.UEN
  '''      ,db_conex)
df_VentasYPFAPP = df_VentasYPFAPP.convert_dtypes()


df_VentasYPFAPP = df_VentasYPFAPP.merge(df_despachosM,on=['UEN'],how='outer')
df_VentasYPFAPP = df_VentasYPFAPP.fillna(0)

## Creo Columna de Penetracion App YPF
df_VentasYPFAPP['Despachos Sin AppYPF'] = (df_VentasYPFAPP['Despachos Base']-df_VentasYPFAPP['Despachos APPYPF'])
df_VentasYPFAPP['Penetracion App YPF'] = df_VentasYPFAPP['Despachos APPYPF'] / df_VentasYPFAPP['Despachos Sin AppYPF']


#creo totales
df_VentasYPFAPP.loc["colTOTAL"]= pd.Series(
    df_VentasYPFAPP.sum()
    , index=['Despachos APPYPF',"Despachos Sin AppYPF"]
)
df_VentasYPFAPP.fillna({"UEN":"TOTAL"}, inplace=True)

#Creo totales de Penetracion App YPF
tasa = (df_VentasYPFAPP.loc["colTOTAL","Despachos APPYPF"] /
    (df_VentasYPFAPP.loc["colTOTAL","Despachos Sin AppYPF"]))
df_VentasYPFAPP.fillna({'Penetracion App YPF':tasa}, inplace=True)

#Elimino Columnas Que no entraran en el informe

df_VentasYPFAPP = df_VentasYPFAPP.reindex(columns=['UEN','Penetracion App YPF','Despachos APPYPF','Despachos Sin AppYPF'])
df_VentasYPFAPPREPORTE=df_VentasYPFAPP



def _estiladorVtaTitulo(df, list_Col_Num, list_Col_Perc,colcaract, titulo):
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
        .format("{0:,.0f}", subset=list_Col_Num) \
        .format("{:,.2%}", subset=list_Col_Perc) \
        .hide_index() \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>") \
        .set_properties(subset= list_Col_Perc + list_Col_Num + colcaract
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

### COLUMNAS Con Numeros Enteros
numCols = ['Despachos APPYPF','Despachos Sin AppYPF'
         ]
### COLUMNAS con Porcentajes
percColsPen = ['Penetracion App YPF'
]

#### COLUMNAS Con caracteres
colcaract = [
]
###### Aplico el formato elegido a la imagen

df_VentasYPFAPP = _estiladorVtaTitulo(df_VentasYPFAPP,numCols,percColsPen,colcaract, "INFO Penetracion App YPF por Estacion")
###### DEFINO NOMBRE Y UBICACION DE DONDE SE VA A GUARDAR LA IMAGEN

ubicacion = "C:/Informes/MasYPF/"
nombrePen = "Info_Penetracion_UEN1.png"
nombrePenDiario = "Info_Penetracion_UEN.png"
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

df_to_image(df_VentasYPFAPP, ubicacion, nombrePen)
#df_to_image(penetracionPanDiario,ubicacion,nombrePenDiario)


#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)