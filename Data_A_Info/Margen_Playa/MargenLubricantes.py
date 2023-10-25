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
from datetime import date
from datetime import timedelta,datetime

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

today = date.today()  # obtiene la fecha actual
start_of_month = date(today.year, today.month, 1)  # obtiene la fecha de inicio del mes actual
start_of_month=start_of_month.strftime('%Y-%m-%d')
today=today.strftime('%Y-%m-%d')

now = datetime.now()  # Obtiene la fecha y hora actual
primer_dia_mes_actual = datetime(now.year, now.month, 1)  # Obtiene el primer día del mes actual
ultimo_dia_mes_anterior = primer_dia_mes_actual - timedelta(days=1)  # Resta un día para obtener el último día del mes anterior
primer_dia_mes_anterior = datetime(ultimo_dia_mes_anterior.year, ultimo_dia_mes_anterior.month, 1)  # Obtiene el primer día del mes anterior
fecha_inicio_mes_anterior = primer_dia_mes_anterior.strftime('%Y-%m-%d')

hoy = datetime.now()
ayer = hoy - timedelta(days=1)
primer_dia_mes = hoy.replace(day=1)


###PRESUPUESTO
sheet_id='1ofJ38O2-dLfw7TZOxeTDYUzlWgl6hN2iMLZCMscTgGo'
hoja='Presupuesto'
gsheet_url_presupuesto = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id, hoja)
df_presupuesto=pd.read_csv(gsheet_url_presupuesto)
df_presupuesto = df_presupuesto.convert_dtypes()
df_presupuesto['FECHASQL'] = pd.to_datetime(df_presupuesto['FECHASQL'], format='%d/%m/%Y')
df_presupuesto= df_presupuesto.loc[(df_presupuesto["FECHASQL"] <= ayer.strftime('%Y-%m-%d'))& (df_presupuesto["FECHASQL"] >= primer_dia_mes.strftime('%Y-%m-%d')),:]

##########################################
# TRABAJO CON TABLA DE EXCEL    ################
##########################################

df_presupuesto_lubricante=df_presupuesto.loc[(df_presupuesto['CODPRODUCTO']=='LUBRI')]
egnctotales =df_presupuesto_lubricante

egncAC=df_presupuesto_lubricante.loc[df_presupuesto['UEN']=='ADOLFO CALLE']
egncAC = egncAC.convert_dtypes()

egncU=df_presupuesto_lubricante.loc[df_presupuesto['UEN']=='URQUIZA']
egncU = egncU.convert_dtypes()

egncVN=df_presupuesto_lubricante.loc[df_presupuesto['UEN']=='VILLANUEVA']
egncVN = egncVN.convert_dtypes()

egncLH=df_presupuesto_lubricante.loc[df_presupuesto['UEN']=='LAS HERAS']
egncLH = egncLH.convert_dtypes()

egncM=df_presupuesto_lubricante.loc[df_presupuesto['UEN']=='MITRE']
egncM = egncM.convert_dtypes()

egncS=df_presupuesto_lubricante.loc[df_presupuesto['UEN']=='SARMIENTO']
egncS = egncS.convert_dtypes()

egncM1=df_presupuesto_lubricante.loc[df_presupuesto['UEN']=='MERC GUAYMALLEN']
egncM1 = egncM1.convert_dtypes()

egncM2=df_presupuesto_lubricante.loc[df_presupuesto['UEN']=='MERCADO 2']
egncM2 = egncM2.convert_dtypes()

egncP1=df_presupuesto_lubricante.loc[df_presupuesto['UEN']=='PERDRIEL']
egncP1 = egncP1.convert_dtypes()

egncSJ=df_presupuesto_lubricante.loc[df_presupuesto['UEN']=='SAN JOSE']
egncSJ = egncSJ.convert_dtypes()

egncL=df_presupuesto_lubricante.loc[df_presupuesto['UEN']=='LAMADRID']
egncL = egncL.convert_dtypes()

egncPO=df_presupuesto_lubricante.loc[df_presupuesto['UEN']=='PUENTE OLIVE']
egncPO = egncPO.convert_dtypes()

egncP2=df_presupuesto_lubricante.loc[df_presupuesto['UEN']=='PERDRIEL2']
egncP2 = egncP2.convert_dtypes()

egncA=df_presupuesto_lubricante.loc[df_presupuesto['UEN']=='AZCUENAGA']
egncA = egncA.convert_dtypes()


egncTotal = pd.concat([egncAC,egncA,egncP2,egncPO,egncL,egncSJ,egncP1,egncM2,egncM1,egncM,egncS,egncLH,egncVN,egncU])
egncTotal = egncTotal.rename({'Fecha':'FECHASQL','VENTAS':'Venta diaria'},axis=1)
egncTotal = egncTotal.reindex(columns=['UEN','FECHASQL','Venta diaria'])
###### Informacion Sobre Ventas, costos y Margenes Acumuladas
################ Precio Promedio del litro de Lubricantes
preciolitrolubri = pd.read_sql(   
'''	
  
	DECLARE @inicioMesActual DATETIME
	SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, CURRENT_TIMESTAMP), 0)

	DECLARE @inicioMesAnterior DATETIME
	SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

	--Divide por la cant de días del mes anterior y multiplica por la cant de días del
	--mes actual
	
	DECLARE @hoy DATETIME
	SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

	--Divide por la cantidad de días cursados del mes actual y multiplica por la cant
	--de días del mes actual
	select count(DISTINCT m.id) as a, m.UEN,(sum(m.IMPORTE)/sum(m.CANTIDAD*p.Envase*-1)) as 'Precio Litro'
	from dbo.VMovDet as M
	left join dbo.PLProdUEN as P
	on M.CODPRODUCTO = P.CODIGO
	and m.UEN = p.UEN
	where  M.TIPOMOVIM = 3
	and M.FECHASQL >= @inicioMesActual
	and	M.FECHASQL < @hoy
	and p.AGRUPACION = 'lubricantes'
	group by m.UEN
	order by m.UEN
  '''      ,db_conex)
preciolitrolubri = preciolitrolubri.convert_dtypes()
preciolitrolubri=preciolitrolubri.reindex(columns=['UEN','Precio Litro'])



###### Informacion Sobre Ventas, costos y Margenes Acumuladas


controlVtasM = pd.read_sql(   
'''	
  
	DECLARE @inicioMesActual DATETIME
	SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, CURRENT_TIMESTAMP), 0)

	DECLARE @inicioMesAnterior DATETIME
	SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

	--Divide por la cant de días del mes anterior y multiplica por la cant de días del
	--mes actual
	
	DECLARE @hoy DATETIME
	SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

	--Divide por la cantidad de días cursados del mes actual y multiplica por la cant
	--de días del mes actual
	select DISTINCT(m.id), p.DESCRIPCION,m.PreCostoFinal,m.IMPORTE,m.CANTIDAD,p.Envase,m.FECHASQL,m.UEN
	from dbo.VMovDet as M
	left join dbo.PLProdUEN as P
	on M.CODPRODUCTO = P.CODIGO
	and m.UEN = p.UEN
	where  M.TIPOMOVIM = 3
	and M.FECHASQL >= '2022-01-06'
	and	M.FECHASQL < @hoy
	and p.AGRUPACION = 'lubricantes'
	order by FECHASQL
  '''      ,db_conex)
controlVtasM = controlVtasM.convert_dtypes()

# ordena el dataframe por producto y fecha de venta
controlVtasM = controlVtasM.sort_values(['DESCRIPCION', 'FECHASQL'])
presupuestoLubri = egncTotal
# rellena los valores nulos en la columna de precio con el valor anterior en la misma columna
controlVtasM['PreCostoFinal'] = controlVtasM['PreCostoFinal'].fillna(method='ffill')

#MBC Presupuestado + 1% mes anterior
presuCostos = controlVtasM
presuCostos = presuCostos.loc[(presuCostos['FECHASQL'] >= fecha_inicio_mes_anterior) & (presuCostos['FECHASQL'] < start_of_month),:]
presuCostos['Costo Producto']=presuCostos['PreCostoFinal']*presuCostos['CANTIDAD']
presuCostos=presuCostos.fillna(0)
presuCostos = presuCostos.reindex(columns=['UEN','FECHASQL','DESCRIPCION','IMPORTE','Costo Producto'])
presuCostos = presuCostos.sort_values(['UEN', 'FECHASQL'])

presuCostos = presuCostos.groupby(
        ["UEN"]
        , as_index=False
    ).sum(numeric_only=True)

presuCostos['% Margen Presupuestado']=(presuCostos['IMPORTE']+presuCostos['Costo Producto'])/presuCostos['IMPORTE']
presuCostos=presuCostos.rename({'IMPORTE':'Ventas Mes anterior','Costo Producto':'Costo Producto Presu'},axis=1)

# Mes Actual
controlVtasM = controlVtasM.loc[controlVtasM['FECHASQL'] >= start_of_month ,:]
controlVtasM['Costo Producto']=controlVtasM['PreCostoFinal']*controlVtasM['CANTIDAD']
controlVtasM = controlVtasM.reindex(columns=['FECHASQL','UEN','IMPORTE','Costo Producto'])
controlVtasM = controlVtasM.sort_values(['UEN', 'FECHASQL'])
controlVtasM = controlVtasM.groupby(
        ["UEN","FECHASQL"]
        , as_index=False
    ).sum(numeric_only=True)

presupuestoLubri['UEN']=presupuestoLubri['UEN'].str.strip()
preciolitrolubri['UEN']=preciolitrolubri['UEN'].str.strip()

#presupuestoLubri['CODPRODUCTO']=presupuestoLubri['CODPRODUCTO'].str.strip()
controlVtasM['UEN']=controlVtasM['UEN'].str.strip()

controlVtasM = controlVtasM.merge(presupuestoLubri,on=['UEN','FECHASQL'],how='outer')
controlVtasM = controlVtasM.loc[controlVtasM['FECHASQL'] < today ,:]
controlVtasM = controlVtasM.reindex(columns=['UEN','IMPORTE','Costo Producto','Venta diaria'])
controlVtasM = controlVtasM.groupby(
        ["UEN"]
        , as_index=False
    ).sum(numeric_only=True)
controlVtasM=controlVtasM.merge(preciolitrolubri,on='UEN',how='outer')
controlVtasM['Presupuesto Acumulado $']=controlVtasM['Venta diaria']*controlVtasM['Precio Litro']


presuCostos['UEN']=presuCostos['UEN'].str.strip()
# Concateno Tablas MBC Presupuestado
controlVtasM=controlVtasM.merge(presuCostos,on='UEN',how='outer')

## Creo Columnas

controlVtasM['MBC Presupuestado $'] = controlVtasM['Presupuesto Acumulado $']*controlVtasM['% Margen Presupuestado']
controlVtasM['MBC Acumulado $'] = controlVtasM['IMPORTE']+controlVtasM['Costo Producto']
controlVtasM['Desvio Presupuestado %']=(controlVtasM['IMPORTE']/controlVtasM['Presupuesto Acumulado $'])-1
controlVtasM['Desvio MBC %']=(controlVtasM['MBC Acumulado $']/controlVtasM['MBC Presupuestado $'])-1

controlVtasM=controlVtasM.rename({'IMPORTE':'Ventas Acumuladas $'},axis=1)


controlVtasM = controlVtasM.reindex(columns=['UEN','Presupuesto Acumulado $','Ventas Acumuladas $','Desvio Presupuestado %','MBC Presupuestado $','MBC Acumulado $','Desvio MBC %'])


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

#Creo Variable Para Margen Empresa
mbcLubricantes=controlVtasM

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
            ,"Desvio Presupuestado %"]

### APLICO EL FORMATO A LA TABLA
controlVtasM = _estiladorVtaTituloD(controlVtasM,numCols,percColsPen, "MBC Lubricantes")
#ubicacion = "C:/Informes/Margen_Playa/"
ubicacion= str(pathlib.Path(__file__).parent)+"\\"

nombreGNCproy = "MargenLubri.png"

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
        dfi.export(df, ubicacion+nombre,max_rows=-1)
    else:
        dfi.export(df, ubicacion+nombre,max_rows=-1)

df_to_image(controlVtasM, ubicacion, nombreGNCproy)



#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)