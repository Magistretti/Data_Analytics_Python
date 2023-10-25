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

hoy = datetime.now()
ayer = hoy - timedelta(days=1)
primer_dia_mes = hoy.replace(day=1)



    ##########################################
    # TRABAJO CON TABLA DE EXCEL    ################
    ##########################################

### Leo Hoja por hoja del excel y traigo los datos
sheet_id='1ofJ38O2-dLfw7TZOxeTDYUzlWgl6hN2iMLZCMscTgGo'
hoja='Presupuesto'
gsheet_url = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id, hoja)
df_presupuesto=pd.read_csv(gsheet_url)
df_presupuesto = df_presupuesto.convert_dtypes()
df_presupuesto['FECHASQL'] = pd.to_datetime(df_presupuesto['FECHASQL'], format='%d/%m/%Y')
df_presupuesto= df_presupuesto.loc[(df_presupuesto["FECHASQL"] <= ayer.strftime('%Y-%m-%d'))& (df_presupuesto["FECHASQL"] >= primer_dia_mes.strftime('%Y-%m-%d')),:]

df_presupuesto=df_presupuesto.loc[df_presupuesto['CODPRODUCTO']=='SALON']

egncM1=df_presupuesto.loc[df_presupuesto['UEN']=='XPRESS']
egncM2=df_presupuesto.loc[df_presupuesto['UEN']=='MERCADO 2']
egncP1=df_presupuesto.loc[df_presupuesto['UEN']=='PERDRIEL']
egncSJ=df_presupuesto.loc[df_presupuesto['UEN']=='SAN JOSE']
egncL=df_presupuesto.loc[df_presupuesto['UEN']=='LAMADRID']
egncPO=df_presupuesto.loc[df_presupuesto['UEN']=='PUENTE OLIVE']
egncP2=df_presupuesto.loc[df_presupuesto['UEN']=='PERDRIEL2']
egncA=df_presupuesto.loc[df_presupuesto['UEN']=='AZCUENAGA']


#Concateno todos los datos de cada hoja en una sola tabla

Total = pd.concat([egncA,egncP2,egncPO,egncL,egncSJ,egncP1,egncM2,egncM1])
Total = Total.rename({'FECHASQL':'Fecha', 'VENTAS':'Venta diaria'}, axis=1)



### Defino el dia de ayer
hoy = datetime.now()
ayer = hoy - timedelta(days=1)

### Volumen Acumulado proyectado Filtro datos acumulados hasta el dia de ayer

acumulado= Total["Fecha"] <=  ayer.strftime('%Y-%m-%d')
volacuProy = Total[acumulado]


ac = volacuProy['UEN'] == 'AZCUENAGA'
ac = volacuProy[ac]
azcuenaga = sum(ac['Venta diaria'])


ac = volacuProy['UEN'] == 'LAMADRID'
ac = volacuProy[ac]
lamadrid = sum(ac['Venta diaria'])


ac = volacuProy['UEN'] == 'XPRESS'
ac = volacuProy[ac]
merc_guay = sum(ac['Venta diaria'])


ac = volacuProy['UEN'] == 'MERCADO 2'
ac = volacuProy[ac]
merc_2 = sum(ac['Venta diaria'])


ac = volacuProy['UEN'] == 'PERDRIEL'
ac = volacuProy[ac]
pedriel_1 = sum(ac['Venta diaria'])


ac = volacuProy['UEN'] == 'PERDRIEL2'
ac = volacuProy[ac]
pedriel_2 = sum(ac['Venta diaria'])


ac = volacuProy['UEN'] == 'PUENTE OLIVE'
ac = volacuProy[ac]
puente_o = sum(ac['Venta diaria'])

ac = volacuProy['UEN'] == 'SAN JOSE'
ac = volacuProy[ac]
san_jose = sum(ac['Venta diaria'])


volaccdicc={}
volaccdicc={
    
    'AZCUENAGA': azcuenaga,
    'LAMADRID': lamadrid,
    'XPRESS':merc_guay,
    'MERCADO 2':merc_2,
    'PERDRIEL':pedriel_1,
    'PERDRIEL2':pedriel_2,
    'PUENTE OLIVE':puente_o,
    'SAN JOSE':san_jose,

}
volumProy = pd.DataFrame([[key, volaccdicc[key]] for key in volaccdicc.keys()], columns=['UEN', 'PRESUPUESTO ACUMULADO'])


####################    VENTAS PRESUPUESTADAS SALON Diarias  ################################

fecha = Total['Fecha']== ayer.strftime('%Y-%m-%d')
diario = Total[fecha]

diario = diario.loc[:,diario.columns!="FECHA"]
diario = diario.loc[:,diario.columns!="Día"]
diario = diario.loc[:,diario.columns!="Porcentaje"]

pvtassalon = diario.merge(volumProy, on= 'UEN', how= 'outer')
pvtassalon = pvtassalon.loc[:,pvtassalon.columns!="Fecha"]
pvtassalon = pvtassalon.rename({"UEN":"UEN","Venta Diaria":"Presupuesto Diario","PRESUPUESTO ACUMULADO":"Presupuesto Acumulado"}, axis=1)


###############################################################
##################  VENTAS REALES SALON DIARIAS  ##############


vtasSalon = pd.read_sql('''
	
    SELECT A.UEN, COUNT(DISTINCT A.DESPACHOS) AS ID, SUM(A.IMPORTE) AS 'TOTAL VENDIDO' FROM(
    SELECT ID AS DESPACHOS, UEN, IMPORTE
    FROM [Rumaos].[dbo].[SCEgreso] where
    FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
    ) AS A GROUP BY  UEN

    ''' ,db_conex)


vtasSalon = vtasSalon.convert_dtypes()
vtasSalon = vtasSalon.reset_index()


####################### VENTAS REALES SALON ACUMULADAS ##############3}

vtasSalonM = pd.read_sql('''
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
	SELECT A.UEN, COUNT(DISTINCT A.DESPACHOS) AS ID, SUM(A.IMPORTE) AS 'TOTAL VENDIDO' FROM(
	SELECT ID AS DESPACHOS, UEN, IMPORTE
    FROM [Rumaos].[dbo].[SCEgreso] where
  	FECHASQL >= @inicioMesActual
	and	FECHASQL < @hoy
	and UEN not like 'MERC GUAYMALLEN'
    ) AS A GROUP BY  UEN

    ''' ,db_conex)
vtasSalonM = vtasSalonM.convert_dtypes()
vtasSalonM = vtasSalonM.reset_index()

vtasSalonM = vtasSalonM.loc[:,vtasSalonM.columns!="index"]
vtasSalon = vtasSalon.loc[:,vtasSalon.columns!="index"]

### Concateno Tablas Diarias y Mensuales

vtasSalonT = vtasSalon.merge(vtasSalonM, on='UEN',how='outer')

vtasSalonT['UEN']=vtasSalonT['UEN'].str.strip()

presusalon = pvtassalon.merge(vtasSalonT, on='UEN', how='outer')
#presusalon = presusalon.fillna(0)
presusalon = presusalon.groupby(
        ["UEN"]
        , as_index=False
    ).sum(numeric_only=True)

presusalon = presusalon.loc[:,presusalon.columns!="ID_x"]
presusalon = presusalon.loc[:,presusalon.columns!="ID_y"]

presusalon = presusalon.assign(DesvioD=presusalon['TOTAL VENDIDO_x'] - presusalon['Venta diaria'])
presusalon = presusalon.assign(DesvioM=presusalon['TOTAL VENDIDO_y'] - presusalon['Presupuesto Acumulado'])
presusalon = presusalon.assign(PorcentualD=presusalon['DesvioD'] / presusalon['Venta diaria'])
presusalon = presusalon.assign(PorcentualM=presusalon['DesvioM'] / presusalon['Presupuesto Acumulado'])

## Creo Totales

presusalon.loc["colTOTAL"]= pd.Series(
    presusalon.sum(numeric_only=True)
    , index=["Venta diaria","Presupuesto Acumulado","TOTAL VENDIDO_x","TOTAL VENDIDO_y"]
)
presusalon.fillna({"UEN":"TOTAL"}, inplace=True)

tasa7 = (presusalon.loc["colTOTAL","TOTAL VENDIDO_x"] -
 presusalon.loc["colTOTAL","Venta diaria"])
presusalon.fillna({"DesvioD":tasa7}, inplace=True)

tasa1 = (presusalon.loc["colTOTAL","TOTAL VENDIDO_y"] -
 presusalon.loc["colTOTAL","Presupuesto Acumulado"])
presusalon.fillna({"DesvioM":tasa1}, inplace=True)

tasa2 = (presusalon.loc["colTOTAL","DesvioD"] / presusalon.loc["colTOTAL","Venta diaria"])
presusalon.fillna({"PorcentualD":tasa2}, inplace=True)

tasa3 = (presusalon.loc["colTOTAL","DesvioM"] / presusalon.loc["colTOTAL","Presupuesto Acumulado"])
presusalon.fillna({"PorcentualM":tasa3}, inplace=True)

## Renombro columnas


presusalon = presusalon.rename({"UEN":"UEN","Venta diaria":"Presupuesto Diario"
,"Presupuesto Acumulado":"Presupuesto Acumulado","TOTAL VENDIDO_x":"Ventas Diario","TOTAL VENDIDO_y":"Ventas Acumulado"
,"DesvioD":"Desvio Diario","DesvioM":"Desvio Acumulado","PorcentualD":"Porcentual Diario","PorcentualM":"Porcentual Acumulado"}, axis=1)

presusalon = presusalon.reindex(columns= ["UEN","Presupuesto Diario","Ventas Diario","Desvio Diario","Porcentual Diario"
,"Presupuesto Acumulado","Ventas Acumulado","Desvio Acumulado","Porcentual Acumulado"])
presusalon = presusalon[presusalon["UEN"] != 'MERCADO 2']

#### Creo funcion que Le de formato y color a la Tabla

def _estiladorVtaTituloP(df,list_Col_Num0, list_Col_Num, list_Col_Perc, titulo):
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
        .format("{0:,.0f}", subset=list_Col_Num0) \
        .format("{0:,.2f}", subset=list_Col_Num) \
        .format("{:,.1%}", subset=list_Col_Perc) \
        .hide(axis=0) \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>") \
        .set_properties(subset= list_Col_Perc + list_Col_Num + list_Col_Num0
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
        ,vmax=0.01
        ,subset=pd.IndexSlice["Porcentual Diario"]
    )
    resultado = resultado.background_gradient(
        cmap="RdYlGn" # Red->Yellow->Green
        ,vmin=-0.05
        ,vmax=0.01
        ,subset=pd.IndexSlice["Porcentual Acumulado"]
    )
    return resultado

#Columnas Sin decimales
numCols0 = ["Presupuesto Diario"
        ,"Ventas Diario"
        ,"Desvio Diario"
        ,"Presupuesto Acumulado"
        ,"Ventas Acumulado"
        ,"Desvio Acumulado"

]

##Columnas con decimales

numCols = [ 
         ]

## Columnas porcentajes
percColsPen = [ "Porcentual Diario"
,"Porcentual Acumulado"
]

######### Aplico a la tabla el formato
presusalon = _estiladorVtaTituloP(presusalon,numCols0,numCols,percColsPen, "INFO DESVIO PRESUPUESTARIO VENTAS SALON")


####### Defino el nombre de la imagen y creo la imagen
ubicacion= str(pathlib.Path(__file__).parent)+"\\"
nombreGNC = "presupuesto.png"
nombreGNCproy = "PresupuestadoSalon.png"
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

df_to_image(presusalon, ubicacion, nombreGNCproy)



#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)







