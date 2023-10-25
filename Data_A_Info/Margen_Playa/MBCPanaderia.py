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

##################
## PRESUPUESTO ##
##################

sheet_id='1ofJ38O2-dLfw7TZOxeTDYUzlWgl6hN2iMLZCMscTgGo'
hoja='Presupuesto'
gsheet_url_presupuesto = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id, hoja)
df_presupuesto=pd.read_csv(gsheet_url_presupuesto)
df_presupuesto = df_presupuesto.convert_dtypes()
df_presupuesto['FECHASQL'] = pd.to_datetime(df_presupuesto['FECHASQL'], format='%d/%m/%Y')
df_presupuesto= df_presupuesto.loc[(df_presupuesto["FECHASQL"] <= ayer.strftime('%Y-%m-%d'))& (df_presupuesto["FECHASQL"] >= primer_dia_mes.strftime('%Y-%m-%d')),:]
df_presupuesto_panaderia= df_presupuesto.loc[df_presupuesto['CODPRODUCTO']=='PANADERIA']

##########################################
# TRABAJO CON TABLA DE EXCEL    ################
##########################################
sheet_id2='1yJlZkGWDcYa5hdlXZxY5_xbi4s3Y0AqoS-L2QgDdFTQ'
sheet_name= 'CostoPanaderia'
gsheet_url_costoPan = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id2, sheet_name)
panCosto = pd.read_csv(gsheet_url_costoPan)
panCosto = panCosto.convert_dtypes()
panCosto['Costo']=panCosto['MEDIALUNAS']+panCosto['TORTAS RASPADAS']

egnctotales =df_presupuesto_panaderia

egncAC=egnctotales.loc[egnctotales['UEN']=='ADOLFO CALLE']
egncU=egnctotales.loc[egnctotales['UEN']=='URQUIZA']
egncVN=egnctotales.loc[egnctotales['UEN']=='VILLANUEVA']
egncLH=egnctotales.loc[egnctotales['UEN']=='LAS HERAS']
egncM=egnctotales.loc[egnctotales['UEN']=='MITRE']
egncS=egnctotales.loc[egnctotales['UEN']=='SARMIENTO']
egncM1=egnctotales.loc[egnctotales['UEN']=='MERC GUAYMALLEN']
egncM2=egnctotales.loc[egnctotales['UEN']=='MERCADO 2']
egncP1=egnctotales.loc[egnctotales['UEN']=='PERDRIEL']
egncSJ=egnctotales.loc[egnctotales['UEN']=='SAN JOSE']
egncL=egnctotales.loc[egnctotales['UEN']=='LAMADRID']
egncPO=egnctotales.loc[egnctotales['UEN']=='PUENTE OLIVE']
egncP2=egnctotales.loc[egnctotales['UEN']=='PERDRIEL2']
egncA=egnctotales.loc[egnctotales['UEN']=='AZCUENAGA']


egncTotal = pd.concat([egncAC,egncA,egncP2,egncPO,egncL,egncSJ,egncP1,egncM2,egncM1,egncM,egncS,egncLH,egncVN,egncU])
egncTotal = egncTotal.rename({'Fecha':'FECHASQL', 'VENTAS':'Venta diaria'},axis=1)
panPresupuesto = egncTotal.reindex(columns=['UEN','FECHASQL','Venta diaria'])


###### Informacion Sobre Ventas, costos y Margenes Acumuladas
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
    
	select a.UEN, (a.CANTIDAD) as CANTIDAD,a.PRECIO,a.FECHASQL from 
	(select  p.UEN, p.TURNO,p.FECHASQL, p.CANTIDAD as CANTIDAD,p.PRECIO,p.CODIGO
	from dbo.PanSalDe as p
	inner join dbo.PanSalGe as g on
	p.UEN = g.UEN
	and p.FECHASQL=g.FECHASQL
	and p.TURNO = g.TURNO
	and p.NROCOMP = g.NROCOMP
	where g.NROCLIENTE = '30'
	and p.PRECIO > '10'
	and (p.CODIGO = 12
	or p.CODIGO = 16
	or p.CODIGO = 11
	or p.CODIGO = 14
	or p.CODIGO = 15
    or p.CODIGO = 17)
	and p.FECHASQL >= @inicioMesActual
	and	p.FECHASQL < @hoy) AS a 
  '''      ,db_conex)
controlVtasM = controlVtasM.convert_dtypes()

### Acumulado Mensual
controlVtasM['Costo'] = panCosto.loc[1,'Costo']
controlVtasM = controlVtasM.reindex(columns=['UEN','FECHASQL','CANTIDAD','PRECIO','Costo'])
controlVtasM['Ventas Acumuladas $']=controlVtasM['CANTIDAD']*controlVtasM['PRECIO']
controlVtasM['Costo Acumulado']=controlVtasM['CANTIDAD']*controlVtasM['Costo']
controlVtasM['MBC Acumulado $']=controlVtasM['Ventas Acumuladas $']-controlVtasM['Costo Acumulado']

precio_ponderado = (controlVtasM['CANTIDAD'] * controlVtasM['PRECIO']).sum() / controlVtasM['CANTIDAD'].sum()

controlVtasM = controlVtasM.reindex(columns=['UEN','FECHASQL','CANTIDAD','Ventas Acumuladas $'
                                            ,'Costo Acumulado','MBC Acumulado $'])

controlVtasM = controlVtasM.groupby(
    ["UEN",'FECHASQL']
    , as_index=False
).sum(numeric_only=True)


controlVtasM['PRECIO']=precio_ponderado

controlVtasM['Costo'] = panCosto.loc[1,'Costo']

prueba=controlVtasM


### Presupuesto
controlVtasM['UEN']=controlVtasM['UEN'].str.strip()
controlVtasM = controlVtasM.merge(panPresupuesto,on=['UEN','FECHASQL'],how='left')

controlVtasM['Presupuesto Acumulado $']=controlVtasM['Venta diaria']*controlVtasM['PRECIO']

controlVtasM['Costo Acumulado Presupuestado']=controlVtasM['Venta diaria']*controlVtasM['Costo']

controlVtasM['MBC Presupuestado $']=controlVtasM['Presupuesto Acumulado $']-controlVtasM['Costo Acumulado Presupuestado']
CONTROL=controlVtasM

controlVtasM = controlVtasM.reindex(columns=['UEN','CANTIDAD','Presupuesto Acumulado $','Costo Acumulado Presupuestado'
                                             ,'MBC Presupuestado $','Ventas Acumuladas $','Costo Acumulado','MBC Acumulado $'])

controlVtasM = controlVtasM.groupby(
    ["UEN"]
    , as_index=False
).sum(numeric_only=True)

controlVtasM['Desvio Presupuestado %']=(controlVtasM['Ventas Acumuladas $']/controlVtasM['Presupuesto Acumulado $'])-1

controlVtasM['Desvio MBC %']=(controlVtasM['MBC Acumulado $']/controlVtasM['MBC Presupuestado $'])-1



### Creo columna (fila) TOTALES
controlVtasM.loc["colTOTAL"]= pd.Series(
    controlVtasM.sum(numeric_only=True)
    , index=['UEN','CANTIDAD','Presupuesto Acumulado $','Costo Acumulado Presupuestado'
            ,'MBC Presupuestado $','Ventas Acumuladas $','Costo Acumulado','MBC Acumulado $']
)
controlVtasM.fillna({"UEN":"TOTAL"}, inplace=True)


#Creo totales de PENETRACION DIARIA TURNO 1
tasa = (controlVtasM.loc["colTOTAL","MBC Acumulado $"] /
    controlVtasM.loc["colTOTAL","MBC Presupuestado $"])-1
controlVtasM.fillna({"Desvio MBC %":tasa}, inplace=True)
#Creo totales de PENETRACION DIARIA TURNO 2
tasa1 = (controlVtasM.loc["colTOTAL","Ventas Acumuladas $"] /
    controlVtasM.loc["colTOTAL","Presupuesto Acumulado $"])-1
controlVtasM.fillna({"Desvio Presupuestado %":tasa1}, inplace=True)


controlVtasM = controlVtasM.reindex(columns=['UEN','Presupuesto Acumulado $','Ventas Acumuladas $','Desvio Presupuestado %'
                                             ,'MBC Presupuestado $','MBC Acumulado $','Desvio MBC %'])

mbcPanaderia=controlVtasM 

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
controlVtasM = _estiladorVtaTituloD(controlVtasM,numCols,percColsPen, "MBC Panaderia")

### APLICO EL FORMATO A LA TABLA
ubicacion= str(pathlib.Path(__file__).parent)+"\\"
#ubicacion = "C:/Informes/Margen_Playa/"
nombre = "MBCPanaderia.png"





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

