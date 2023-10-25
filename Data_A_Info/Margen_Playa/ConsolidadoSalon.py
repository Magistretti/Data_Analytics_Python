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
from calendar import monthrange
from datetime import datetime
from datetime import timedelta
import datetime
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

### VARIABLES DE TIRMPO
hoy = datetime.datetime.now()
ayer = hoy - timedelta(days=1)
primer_dia_mes = hoy.replace(day=1)

## GSHEET CON EL PRESUPUESTO ##
sheet_id='1ofJ38O2-dLfw7TZOxeTDYUzlWgl6hN2iMLZCMscTgGo'
hoja='Presupuesto'
gsheet_url = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id, hoja)
df_presupuesto=pd.read_csv(gsheet_url)
df_presupuesto = df_presupuesto.convert_dtypes()
df_presupuesto=df_presupuesto.loc[df_presupuesto['CODPRODUCTO']=='SALON']

## PRESUPUESTOS POR UEN
egncX=df_presupuesto.loc[df_presupuesto['UEN']=='XPRESS']
egncX = egncX.convert_dtypes()
egncM2=df_presupuesto.loc[df_presupuesto['UEN']=='MERCADO 2']
egncM2 = egncM2.convert_dtypes()
egncP1=df_presupuesto.loc[df_presupuesto['UEN']=='PERDRIEL']
egncP1 = egncP1.convert_dtypes()
egncSJ=df_presupuesto.loc[df_presupuesto['UEN']=='SAN JOSE']
egncSJ = egncSJ.convert_dtypes()
egncL=df_presupuesto.loc[df_presupuesto['UEN']=='LAMADRID']
egncL = egncL.convert_dtypes()
egncPO=df_presupuesto.loc[df_presupuesto['UEN']=='PUENTE OLIVE']
egncPO = egncPO.convert_dtypes()
egncP2=df_presupuesto.loc[df_presupuesto['UEN']=='PERDRIEL2']
egncP2 = egncP2.convert_dtypes()
egncA=df_presupuesto.loc[df_presupuesto['UEN']=='AZCUENAGA']
egncA = egncA.convert_dtypes()
egncTotal = pd.concat([egncA,egncP2,egncPO,egncL,egncSJ,egncP1,egncM2,egncX])
egncTotal = egncTotal.rename({'Fecha':'FECHASQL'},axis=1)
egncTotal=egncTotal.rename({'Fecha':'FECHASQL'},axis=1)


presupuestoVentas = egncTotal
presupuestoVentas['FECHASQL'] = pd.to_datetime(presupuestoVentas['FECHASQL'],  format='%d/%m/%Y')
presupuestoVentas = presupuestoVentas.loc[(presupuestoVentas["FECHASQL"] <= ayer.strftime('%Y-%m-%d')) & (presupuestoVentas["FECHASQL"] >= primer_dia_mes.strftime('%Y-%m-%d'))]
presupuestoVentas=presupuestoVentas.reindex(columns=['UEN','VENTAS'])
presupuestoVentas = presupuestoVentas.groupby(
        ["UEN"]
        , as_index=False
    ).sum(numeric_only=True)

## CONSULTAS SQL
presupuestoCostos = pd.read_sql(   
'''	
  
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
	SELECT p.UEN,(( SUM(E.IMPORTE)-SUM(e.PreCostoImpIncl))/SUM(E.IMPORTE)) as '% Margen Presupuestado'
    FROM [Rumaos].[dbo].[SCEgreso] as  e
	left join dbo.SCprodUEN as P on
	E.UEN = P.UEN
	AND E.CODIGO = P.CODIGO

	where
  	FECHASQL >= @inicioMesActual
	and	FECHASQL < @hoy
	and p.AGRUPACION  Not like '%INSUMO%'
	and p.AGRUPACION not like '%FRANQUICIA%'
    AND P.AGRUPACION NOT LIKE 'REGAL%'
	and p.AGRUPACION not like 'Premios%'
	AND E.IMPORTE > 1
    and e.PreCostoImpIncl > 1
	group by p.UEN
  '''      ,db_conex)
presupuestoCostos = presupuestoCostos.convert_dtypes()
presupuestoCostos['UEN']=presupuestoCostos['UEN'].str.strip()

###### Ventas Acumuladas , Costo Acumulado  y MBC Del mes actual menos XPRESS
controlVtasM = pd.read_sql(   
'''	
  
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
	SELECT p.UEN,SUM(E.IMPORTE) AS 'Ventas Acumuladas',SUM(e.PreCostoImpIncl) AS 'Costo Acumulado', ( SUM(E.IMPORTE)-SUM(e.PreCostoImpIncl)) AS 'Margen Acumulado', (( SUM(E.IMPORTE)-SUM(e.PreCostoImpIncl))/SUM(E.IMPORTE)) as '% Margen Acumulado'
    FROM [Rumaos].[dbo].[SCEgreso] as  e
	left join dbo.SCprodUEN as P on
	E.UEN = P.UEN
	AND E.CODIGO = P.CODIGO

	where
  	FECHASQL >= @inicioMesActual
	and	FECHASQL < @hoy
	and p.AGRUPACION  Not like '%INSUMO%'
	and p.AGRUPACION not like '%FRANQUICIA%'
    AND P.AGRUPACION NOT LIKE 'REGAL%'
	and p.AGRUPACION not like 'Premios%'
	AND E.IMPORTE > 1
    and e.PreCostoImpIncl > 1
	group by p.UEN
  '''      ,db_conex)
controlVtasM = controlVtasM.convert_dtypes()
controlVtasM['UEN']=controlVtasM['UEN'].str.strip()



controlVtasM = controlVtasM.merge(presupuestoVentas,on=['UEN'],how='outer')
controlVtasM = controlVtasM.merge(presupuestoCostos,on=['UEN'],how='outer')
controlVtasM['MBC Presupuestado $'] = controlVtasM['VENTAS']*controlVtasM['% Margen Presupuestado']
controlVtasM=controlVtasM.rename({'Ventas Acumuladas':'Ventas Acumuladas $','Margen Acumulado':'MBC Acumulado $','VENTAS':'Presupuesto Acumulado $'},axis=1)


controlVtasM = controlVtasM.drop(controlVtasM[controlVtasM['UEN'] == 'MERCADO 2'].index)

###### Columnas de Desvio y Totales NS
controlVtasM['Desvio Presupuestado %']=(controlVtasM['Ventas Acumuladas $']/controlVtasM['Presupuesto Acumulado $'])-1
controlVtasM['Desvio MBC %']=(controlVtasM['MBC Acumulado $']/controlVtasM['MBC Presupuestado $'])-1
###### Creo fila Totales
controlVtasM.loc["colTOTAL"]= pd.Series(
    controlVtasM.sum(numeric_only=True)
    , index=['Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $']
)
controlVtasM.fillna({"UEN":"TOTAL"}, inplace=True)

tasa = (controlVtasM.loc["colTOTAL",'Ventas Acumuladas $'] /
    controlVtasM.loc["colTOTAL",'Presupuesto Acumulado $'])-1
controlVtasM.fillna({"Desvio Presupuestado %":tasa}, inplace=True)

tasa2 = (controlVtasM.loc["colTOTAL",'MBC Acumulado $'] /
    controlVtasM.loc["colTOTAL",'MBC Presupuestado $'])-1
controlVtasM.fillna({'Desvio MBC %':tasa2}, inplace=True)
controlVtasM=controlVtasM.reindex(columns=['UEN','Presupuesto Acumulado $','Ventas Acumuladas $','Desvio Presupuestado %','MBC Presupuestado $','MBC Acumulado $','Desvio MBC %'])

#Creo Variable Para Margen Empresa
mbcSalon =controlVtasM
mbcSalon

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
        .format("${0:,.0f}", subset=list_Col_Num) \
        .format("{:,.2%}", subset=list_Col_Perc) \
        .hide(axis=0) \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>") \
        .applymap(lambda x: 'color: blue' if x >= 0 else 'color: #FF0000', subset=pd.IndexSlice[:, ['Desvio MBC %']]) \
        .applymap(lambda x: 'color: blue' if x >= 0 else 'color: #FF0000', subset=pd.IndexSlice[:, ['Desvio Presupuestado %']]) \
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

#  columnas sin decimales
numCols = [ 'Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $']

# Columnas Porcentaje
percColsPen = ["Desvio MBC %"
            ,"Desvio Presupuestado %"
]

### APLICO EL FORMATO A LA TABLA
controlVtasM = _estiladorVtaTituloD(controlVtasM,numCols,percColsPen, "MBC Salon")

ubicacion= str(pathlib.Path(__file__).parent)+"\\"
nombre = "MBCSalon.png"


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

df_to_image(controlVtasM, ubicacion, nombre)



#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)



