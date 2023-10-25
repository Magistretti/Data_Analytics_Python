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
    # TRABAJO CON TABLA DE EXCEL    ################
    ##########################################

ubicacion = str(pathlib.Path(__file__).parent)+"\\"
aux_semanal = "PresupuestoVtasSalon.xlsx"
totales =pd.read_excel(ubicacion+aux_semanal, sheet_name= 'Hoja1')
totales = totales.convert_dtypes()

### Leo Hoja por hoja del excel y traigo los datos

egncM1=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"XPRESS"
)
egncM1 = egncM1.convert_dtypes()
egncM2=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"Mercado 2"
)
egncM2 = egncM2.convert_dtypes()
egncP1=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"Perdriel 1"
)
egncP1 = egncP1.convert_dtypes()
egncSJ=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"San José"
)
egncSJ = egncSJ.convert_dtypes()
egncL=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"Lamadrid"
)
egncL = egncL.convert_dtypes()
egncPO=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"Puente Olive"
)
egncPO = egncPO.convert_dtypes()
egncP2=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"Perdriel 2"
)
egncP2 = egncP2.convert_dtypes()
egncA=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"Azcuenaga"
)
egncA = egncA.convert_dtypes()

#Concateno todos los datos de cada hoja en una sola tabla

Total = pd.concat([egncA,egncP2,egncPO,egncL,egncSJ,egncP1,egncM2,egncM1])

### Defino el dia de ayer
hoy = datetime.now()
ayer = hoy + timedelta(days=5)

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

#################################################  
############# VENTAS COCINA ACUMULADAS
#################################################   

vtasCocinaM = pd.read_sql(   
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

	SELECT DISTINCT(E.ID) AS DESPACHOS, E.UEN, IMPORTE, E.CANTIDAD, P.AGRUPACION, P.ENVASE, P.PREVENTA, P.DESCRIPCION
    FROM [Rumaos].[dbo].[SCEgreso] as  e
	left join dbo.SCprodUEN as P on
	E.UEN = P.UEN
	AND E.CODIGO = P.CODIGO

	where
  	E.FECHASQL >= @inicioMesActual
	and	E.FECHASQL < @hoy
	and E.UEN not like 'MERC GUAYMALLEN'
	and (P.AGRUPACION = 'Cocina' or P.AGRUPACION = 'Cocina Promos')

  '''      ,db_conex)
vtasCocinaM = vtasCocinaM.convert_dtypes()
vtasCocinaM = vtasCocinaM.reset_index()


vtasCocinaM = vtasCocinaM.assign(PromedioM= vtasCocinaM['TOTAL VENDIDO'] / vtasCocinaM['ID'])
################################################ 
############## VENTAS COCINA DIARIO #################
#################################################

vtasCocina = pd.read_sql(   
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

	SELECT DISTINCT(E.ID) AS DESPACHOS, E.UEN, IMPORTE, E.CANTIDAD, P.AGRUPACION, P.ENVASE, P.PREVENTA, P.DESCRIPCION
    FROM [Rumaos].[dbo].[SCEgreso] as  e
	left join dbo.SCprodUEN as P on
	E.UEN = P.UEN
	AND E.CODIGO = P.CODIGO

	where
  	E.FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
	and E.UEN not like 'MERC GUAYMALLEN'
	and (P.AGRUPACION = 'Cocina' or P.AGRUPACION = 'Cocina Promos')

  '''      ,db_conex)
vtasCocina = vtasCocina.convert_dtypes()

vtasCocina = vtasCocina.assign(Promedio= vtasCocina['TOTAL VENDIDO'] / vtasCocina['ID'])
vtasCocina = vtasCocina.loc[:,vtasCocina.columns!="index"]
vtasCocinaM = vtasCocinaM.loc[:,vtasCocinaM.columns!="index"]









#Columnas Sin decimales
numCols0 = ["Total Vendido Diario $"
        ,"Total Vendido Acumulado $"
        ,"Tickets Diario"
        ,"Tickets Acumulado"

]

##Columnas con decimales

numCols = [ "Promedio Ticket Diario $"
        , "Promedio Ticket Acumulado $"
         ]

## Columnas porcentajes
percColsPen = [
]



##############
# PRINTING dataframe as an image
##############


ubicacion = "C:/Users/mmagistretti/Desktop/Informes/Penetracion Salon/"
nombreGNC = "Info_Presupuesto_GNC.png"
nombreGNCproy = "prueba.png"
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

#df_to_image(egncFinal, ubicacion, nombreGNCproy)



#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)







































