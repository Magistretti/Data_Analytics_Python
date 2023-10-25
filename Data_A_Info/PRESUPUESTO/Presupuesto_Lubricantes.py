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

from datetime import datetime
from datetime import timedelta
from datetime import date
from datetime import timedelta,datetime

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
egncTotal = egncTotal.rename({'Fecha':'FECHASQL', 'VENTAS':'Venta diaria'},axis=1)
egncTotal = egncTotal.reindex(columns=['UEN','FECHASQL','Venta diaria'])

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
	select count(DISTINCT m.id),sum(m.IMPORTE) as Importe,sum(m.CANTIDAD*p.Envase*-1) as CANTIDAD,m.UEN,m.FECHASQL
	from dbo.VMovDet as M
	left join dbo.PLProdUEN as P
	on M.CODPRODUCTO = P.CODIGO
	and m.UEN = p.UEN
	where  M.TIPOMOVIM = 3
	and M.FECHASQL >= @inicioMesActual
	and	M.FECHASQL < @hoy
	and p.AGRUPACION = 'lubricantes'
	group by m.UEN,M.FECHASQL
  '''      ,db_conex)
controlVtasM = controlVtasM.convert_dtypes()
presupuestoLubri = egncTotal
# Mes Actual
controlVtasM = controlVtasM.reindex(columns=['UEN','CANTIDAD','FECHASQL','Importe'])
controlVtasM = controlVtasM.sort_values(['UEN', 'FECHASQL'])
controlVtasM = controlVtasM.groupby(
        ["UEN","FECHASQL"]
        , as_index=False
    ).sum(numeric_only=True)

controlVtasM['UEN']=controlVtasM['UEN'].str.strip()

controlVtasM = controlVtasM.merge(presupuestoLubri,on=['UEN','FECHASQL'],how='outer')
controlVtasM = controlVtasM.loc[controlVtasM['FECHASQL'] < today ,:]
controlVtasM = controlVtasM.reindex(columns=['UEN','Importe','CANTIDAD','Venta diaria'])
controlVtasM = controlVtasM.groupby(
        ["UEN"]
        , as_index=False
    ).sum(numeric_only=True)
preciolitrolubri['UEN']=preciolitrolubri['UEN'].str.strip()

controlVtasM=controlVtasM.merge(preciolitrolubri,on='UEN',how='outer')
controlVtasM['Presupuesto Acumulado $']=controlVtasM['Venta diaria']*controlVtasM['Precio Litro']

## Creo Columnas

controlVtasM['Desvio Presupuestado L'] = controlVtasM['CANTIDAD']-controlVtasM['Venta diaria']
controlVtasM['Desvio Presupuestado L %'] = controlVtasM['Desvio Presupuestado L']/controlVtasM['Venta diaria']
controlVtasM['Desvio Presupuestado $']=controlVtasM['Importe']-controlVtasM['Presupuesto Acumulado $']
controlVtasM['Desvio Presupuestado $ %']=controlVtasM['Desvio Presupuestado $']/controlVtasM['Presupuesto Acumulado $']

controlVtasM=controlVtasM.rename({'Importe':'Ventas Acumuladas $','Venta diaria':'Presupuesto Acumulado L','CANTIDAD':'Volumen Total Vendido'},axis=1)


controlVtasM = controlVtasM.reindex(columns=['UEN','Presupuesto Acumulado L','Volumen Total Vendido','Desvio Presupuestado L'
                                             ,'Desvio Presupuestado L %','Presupuesto Acumulado $','Ventas Acumuladas $'
                                            ,'Desvio Presupuestado $','Desvio Presupuestado $ %'])


controlVtasM.loc["colTOTAL"]= pd.Series(
    controlVtasM.sum(numeric_only=True)
    , index=['Presupuesto Acumulado L','Volumen Total Vendido','Desvio Presupuestado L','Presupuesto Acumulado $','Ventas Acumuladas $'
                                            ,'Desvio Presupuestado $']
)
controlVtasM.fillna({"UEN":"TOTAL"}, inplace=True)
tasa = (controlVtasM.loc["colTOTAL",'Desvio Presupuestado L'] /
    controlVtasM.loc["colTOTAL",'Presupuesto Acumulado L'])
controlVtasM.fillna({"Desvio Presupuestado L %":tasa}, inplace=True)

tasa2 = (controlVtasM.loc["colTOTAL",'Desvio Presupuestado $'] /
    controlVtasM.loc["colTOTAL",'Presupuesto Acumulado $'])
controlVtasM.fillna({'Desvio Presupuestado $ %':tasa2}, inplace=True)

#Creo Variable Para Margen Empresa
mbcLubricantes=controlVtasM

def _estiladorVtaTituloD(df, list_Col_EUm,list_Col_litros, list_Col_Perc, titulo):
    """
This function will return a styled dataframe that must be assign to a variable.
ARGS:
    df: Dataframe that will be styled.
    list_Col_EUm: List of EUmeric columns that will be formatted with
    zero decimals and thousand separator.
    list_Col_Perc: List of EUmeric columns that will be formatted 
    as percentage.
    titulo: String for the table caption.
    """
    resultado = df.style \
        .format("$ {0:,.0f}", subset=list_Col_EUm) \
        .format("{:,.2%}", subset=list_Col_Perc) \
        .format("{:,.2f} L", subset=list_Col_litros) \
        .hide(axis=0) \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>") \
        .applymap(lambda x: 'color: blue' if x >= 0 else 'color: #FF0000', subset=pd.IndexSlice[:, ['Desvio Presupuestado L %']]) \
        .applymap(lambda x: 'color: blue' if x >= 0 else 'color: #FF0000', subset=pd.IndexSlice[:, ['Desvio Presupuestado L']]) \
        .applymap(lambda x: 'color: blue' if x >= 0 else 'color: #FF0000', subset=pd.IndexSlice[:, ['Desvio Presupuestado $ %']]) \
        .applymap(lambda x: 'color: blue' if x >= 0 else 'color: #FF0000', subset=pd.IndexSlice[:, ['Desvio Presupuestado $']]) \
        .set_properties(subset= list_Col_Perc + list_Col_EUm +list_Col_litros
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
EUmColspesos = [ 'Ventas Acumuladas $','Presupuesto Acumulado $','Desvio Presupuestado $']
EUmColslitros=['Presupuesto Acumulado L','Volumen Total Vendido','Desvio Presupuestado L']
# Columnas Porcentaje
percColsPen = ['Desvio Presupuestado L %','Desvio Presupuestado $ %'
]

### APLICO EL FORMATO A LA TABLA
controlVtasM = _estiladorVtaTituloD(controlVtasM,EUmColspesos,EUmColslitros,percColsPen, "Ejecucion Presupuestaria Lubricantes")


###### DEFINO NOMBRE Y UBICACION DE DONDE SE VA A GUARDAR LA IMAGEN

ubicacion= str(pathlib.Path(__file__).parent)+"\\"
nombrepng = "Presupuesto_Lubricantes.png"

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

df_to_image(controlVtasM, ubicacion, nombrepng)


#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)
