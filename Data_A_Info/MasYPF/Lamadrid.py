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
from MasYPF.PenetracionUENAppYPF1 import df_VentasYPFAPPREPORTE
from calendar import monthrange
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
hoy = datetime.date.today() # Obtiene la fecha actual
primero_enero = datetime.date(hoy.year, 7, 1) # Crea un objeto de fecha para el 1 de enero del año actual
dias_pasados = (hoy - primero_enero).days # Calcula la diferencia de días

### PRESUPUESTO
##########################################
# TRABAJO CON TABLA DE EXCEL    ################
##########################################

ubicacion = 'C:/Informes/PRESUPUESTO'+"\\"
aux_semanal = "Presupuesto_Mix.xlsx"
presuMix =pd.read_excel(ubicacion+aux_semanal, sheet_name= 'Hoja1')
presuMix = presuMix.convert_dtypes()

presuMix = presuMix[presuMix['UEN']=='LAMADRID            ']
presuMix = presuMix.convert_dtypes()
##########################################
################ MENSUAL ACUMULADO  ######
##########################################

##########################   DESPACHOS ACUMULADOS MENSUAL LAMADRID

df_despachosM = pd.read_sql('''
	
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

  	select   UEN, SUM(VTATOTVOL) as 'DESPACHOS', CODPRODUCTO from EmpVenta 
	WHERE 
	FECHASQL >= '2023-01-07'
	and	FECHASQL < @hoy
	and CODPRODUCTO not like 'GNC'
        and UEN IN (
            'LAMADRID')
	group by UEN, CODPRODUCTO
	ORDER BY UEN
    ''' ,db_conex)
#### Filtro y creo columnas de Naftas Y gasoleos
df_despachosM = df_despachosM.convert_dtypes()
df_despachosM = df_despachosM.loc[:,df_despachosM.columns!="ID"]
turno1 = df_despachosM['CODPRODUCTO'] == "EU   "
turno1 = df_despachosM[turno1]
df_despachosM["EU"]= 1 * turno1["DESPACHOS"]

turno2 = df_despachosM['CODPRODUCTO'] == "GO   "
turno2 = df_despachosM[turno2]
df_despachosM["GO"]= 1 * turno2["DESPACHOS"]

turno3 = df_despachosM['CODPRODUCTO'] == "NS   "
turno3 = df_despachosM[turno3]
df_despachosM["NS"]= 1* turno3["DESPACHOS"]

turno4 = df_despachosM['CODPRODUCTO'] == "NU   "
turno4 = df_despachosM[turno4]
df_despachosM["NU"]= 1* turno4["DESPACHOS"]

df_despachosM = df_despachosM.groupby(
        ["UEN"]
        , as_index=False
    ).sum()

df_despachosM['Mix Infinia Nafta']= (df_despachosM['NU']/(df_despachosM['NS']+df_despachosM['NU'])) 
df_despachosM['Mix Infinia Diesel']= (df_despachosM['EU']/(df_despachosM['GO']+df_despachosM['EU']))
df_despachosM['Proyectado Naftas']=(df_despachosM['NS']+df_despachosM['NU'])/dias_pasados*90
df_despachosM['Proyectado Diesel']=(df_despachosM['EU'])/dias_pasados*90
## Creo el DataFrame
p1 = pd.DataFrame()
p1['Objetivo'] = None
p1['Ejecucion'] = None
p1['Puntos'] = None
p1['Responsables'] = None
p1 = p1.convert_dtypes()
############################################# MIX #############################################
#### Creo fila de Mix Nafta
p1.loc["Mix Infinia Nafta"]= pd.Series()
x1=float(presuMix['Presupuesto Nafta'])
p1.fillna({"Objetivo":x1}, inplace=True)
x2=float(df_despachosM['Mix Infinia Nafta'])
p1.fillna({"Ejecucion":x2}, inplace=True)
p1.fillna({"Puntos":75}, inplace=True)
p1.fillna({"Responsables":'Marchan'}, inplace=True)
#### Creo fila de Mix Diesel
p1.loc["Mix Infinia Diesel"]= pd.Series() 
t1 = float(presuMix['Presupuesto Gas Oil'])
p1.fillna({"Objetivo":t1}, inplace=True)
t2 =float(df_despachosM['Mix Infinia Diesel'])
p1.fillna({"Ejecucion":t2}, inplace=True)
p1.fillna({"Puntos":75}, inplace=True)
p1.fillna({"Responsables":'Marchan'}, inplace=True)
############################################# VOLUMEN #############################################
#### Creo fila de Volumen Naftas
p1.loc["Volumen Naftas"]= pd.Series() 
x3 = float(698)
p1.fillna({"Objetivo":x3}, inplace=True)
x4=float(df_despachosM['Proyectado Naftas'])
p1.fillna({"Ejecucion":x4/1000}, inplace=True)
p1.fillna({"Puntos":90}, inplace=True)
p1.fillna({"Responsables":'Marchan'}, inplace=True)
#### Creo fila de Volumen Diesel
p1.loc["Volumen Infinia Diesel"]= pd.Series() 
t3 = float(172)
p1.fillna({"Objetivo":t3}, inplace=True)
t4=float(df_despachosM['Proyectado Diesel'])
p1.fillna({"Ejecucion":t4/1000}, inplace=True)
p1.fillna({"Puntos":90}, inplace=True)
p1.fillna({"Responsables":'Marchan'}, inplace=True)
############################################# CONTRIBUCION BRUTA #############################################

df_ContribucionB = pd.read_sql('''
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

		SELECT E.UEN,SUM(E.IMPORTE) AS 'Ventas Diarias',SUM(e.PreCostoImpIncl) AS 'Costo Diario', ( SUM(E.IMPORTE)-SUM(e.PreCostoImpIncl)) AS 'Margen Diario', (( SUM(E.IMPORTE)-SUM(e.PreCostoImpIncl))/SUM(E.IMPORTE)) as '% Margen Diario'
    FROM [Rumaos].[dbo].[SCEgreso] as  e
	left join dbo.SCprodUEN as P on
	E.UEN = P.UEN
	AND E.CODIGO = P.CODIGO

	where
  	E.FECHASQL >= '2023-01-07'
	and	E.FECHASQL < @hoy
	
	and p.AGRUPACION  Not like '%INSUMO%'
	and p.AGRUPACION not like '%FRANQUICIA%'
    AND P.AGRUPACION NOT LIKE 'REGAL%'
	and p.AGRUPACION not like 'Premios%'
	and p.AGRUPACION not like 'CIGARRILLOS%'
	AND E.IMPORTE > 1
	and E.UEN = 'LAMADRID'
    and e.PreCostoImpIncl > 1
	group by E.UEN
    ''' ,db_conex)
df_ContribucionB = df_ContribucionB.convert_dtypes()


#### Creo fila de Volumen Diesel
p1.loc["Contribución Bruta Full"]= pd.Series() 
t5 = int(4585345)
p1.fillna({"Objetivo":t5}, inplace=True)
t6=float(df_ContribucionB['Margen Diario'])
t6= t6/(1+0.21)
p1.fillna({"Ejecucion":t6/dias_pasados*90}, inplace=True)
p1.fillna({"Puntos":40}, inplace=True)
p1.fillna({"Responsables":'Salinas'}, inplace=True)
############################################# PENETRACION APP YPF PLAYA #############################################

df_VentasYPF = df_VentasYPFAPPREPORTE[df_VentasYPFAPPREPORTE['UEN']=='LAMADRID            ']

#### Creo fila de Volumen Diesel
p1.loc["Penetración App YPF de Playa"]= pd.Series() 
t7 = float(0.19)
p1.fillna({"Objetivo":t7}, inplace=True)
t8=float(df_VentasYPF['Penetracion App YPF'])
p1.fillna({"Ejecucion":t8}, inplace=True)
p1.fillna({"Puntos":50}, inplace=True)
p1.fillna({"Responsables":'Marchan'}, inplace=True)
## Creo columna Desvios
p1['Desvio']=(p1['Ejecucion']-p1['Objetivo'])/p1['Objetivo']
p1=p1.reset_index()
p1=p1.rename(columns={'index':'Lamadrid'})
def condicion_fila(fila):
    if fila['Desvio'] >= 0:
        return fila['Puntos']
    else:
        return (1+fila['Desvio'])*fila['Puntos']

p1['Premio'] = p1.apply(condicion_fila, axis=1)
p1 = p1.drop(p1[p1['Lamadrid'] == 'Volumen Naftas'].index)
p1 = p1.drop(p1[p1['Lamadrid'] == 'Volumen Infinia Diesel'].index)

p1.loc["colTOTAL"]= pd.Series(
    p1.sum()
    , index=['Premio']
)
p1.fillna({"Lamadrid":"Total"}, inplace=True)
p1.loc["% de Ejecución"]= pd.Series()
p1.fillna({"Lamadrid":"% de Ejecución"}, inplace=True)
optimo=(p1.loc['colTOTAL','Premio']/250)
p1.fillna({"Premio":optimo}, inplace=True)
p1=p1.fillna(' ')
p1=p1.reindex(columns=['Lamadrid','Responsables','Objetivo','Ejecucion','Desvio','Premio'])
p1 = p1.astype(str)
def formato_celda(celda):
    try:
        valor = pd.to_numeric(celda)
        if valor < 1:
            return '{:.2%}'.format(valor)
        else:
            return '{:,.0f}'.format(valor)
    except:
        return celda
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
        .format(formato_celda) \
        .hide_index() \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>") \
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
        , axis=1) \
        .apply(lambda x: ["background: black" if x.name == "% de Ejecución" 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == "% de Ejecución" 
            else "" for i in x]
        , axis=1) \
        .apply(lambda x: ["background: black" if x.name in [
            df.columns[0]
        ]
            else "" for i in x]
            , axis=0)\
        .apply(lambda x: ["color: white" if x.name in [
            df.columns[0]
        ]
            else "" for i in x]
            , axis=0) \
        .apply(lambda x: ["font-size: 15px" if x.name in [
            df.columns[0]
        ]
            else "" for i in x]
            , axis=0)
    evitarTotales = df.index.get_level_values(0)
    resultado = resultado.background_gradient(
        cmap="RdYlGn" # Red->Yellow->Green
        ,vmin=-0.01
        ,vmax=0.01
        ,subset=pd.IndexSlice[evitarTotales[:-2],"Desvio"]
    )
    return resultado
p1['Objetivo'] = p1['Objetivo'].replace(',', '.', regex=True)
p1['Ejecucion'] = p1['Ejecucion'].replace(',', '.', regex=True)
#  columnas sin decimales

numCols = [ 'Responsables'
         ]
         
# Columnas Porcentaje
percColsPen = ['Objetivo','Ejecucion','Desvio','Premio'
]


### Aplico la modificacion al Dataframe
p1 = _estiladorVtaTituloD(p1,numCols,percColsPen, "Programa +YPF Lamadrid")










ubicacion = "C:/Informes/MasYPF/"
nombrePen = "+YPFLamadrid.png"
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

df_to_image(p1, ubicacion, nombrePen)



#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)
