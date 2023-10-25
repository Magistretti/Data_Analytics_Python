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

###########################################################################
##############################   GNC ######################################
###########################################################################
### Listado de tarjetas creadas Los ultimos 14 dias
tarjetascreadasGNCsemanapasada = pd.read_sql(''' 
	select DISTINCT E.tarjeta, E.FECHA FROM (SELECT
        DISTINCT TARJETA, MIN(FECHASQL) AS FECHA
    FROM Rumaos.dbo.Despapro
    WHERE 
        TARJETA like 'i%'
        AND FECHASQL >= '2022-02-01'
        and FECHASQL < DATEADD(DAY,0,CAST(GETDATE() AS date))
        AND VOLUMEN > '0'
        AND CODPRODUCTO = 'GNC'
        and TARJETA NOT LIKE 'cc%'
        and TARJETA NOT LIKE 'ig99999%'
		GROUP BY TARJETA) as e 
		WHERE E.FECHA >= DATEADD(DAY,-7,CAST(GETDATE() AS date))
        AND E.FECHA < DATEADD(DAY,0,CAST(GETDATE() AS date)) GROUP BY E.TARJETA,E.FECHA
''' 
  ,db_conex)
tarjetascreadasGNCsemanapasada = tarjetascreadasGNCsemanapasada.convert_dtypes()
listaTarjetasCreadasGNCS = tarjetascreadasGNCsemanapasada["tarjeta"].tolist()
tarjcreadasS= len(listaTarjetasCreadasGNCS)

### Cantidad de Tarjetas Activas (usadas en los ultimos 14 dias)  a partir de ayer
cantiadadtarjetasActivasGNC = pd.read_sql(''' 
    SELECT
        Count(DISTINCT TARJETA) AS 'Tarjetas Activas'
		,CODPRODUCTO
    FROM Rumaos.dbo.Despapro
    WHERE 
        TARJETA like 'i%'
        and FECHASQL >= DATEADD(DAY,-21,CAST(GETDATE() AS date))
		and FECHASQL < DATEADD(DAY,0,CAST(GETDATE() AS date))
        AND VOLUMEN > '0'
        AND CODPRODUCTO = 'GNC'
        and TARJETA NOT LIKE 'cc%'
        and TARJETA NOT LIKE 'ig99999%'
        group by CODPRODUCTO
''' 
  ,db_conex)
cantiadadtarjetasActivasGNC = cantiadadtarjetasActivasGNC.convert_dtypes()



### Cantidad de Tarjetas Activas (usadas en los ultimos 14 dias)  a partir de ayer
tarjetasActivasGNC = pd.read_sql(''' 
    select e.[Tarjetas Activas],e.fecha from (SELECT
        DISTINCT TARJETA AS 'Tarjetas Activas'
		,MAX(FECHASQL) as fecha
    FROM Rumaos.dbo.Despapro
    WHERE 
        TARJETA like 'i%'
        and FECHASQL >= '2022-02-01'
		and FECHASQL < DATEADD(DAY,0,CAST(GETDATE() AS date))
        AND VOLUMEN > '0'
        AND CODPRODUCTO = 'GNC'
        and TARJETA NOT LIKE 'cc%'
        and TARJETA NOT LIKE 'ig99999%'
        group by TARJETA) as e 
		where e.fecha >= DATEADD(DAY,-14,CAST(GETDATE() AS date))
		and e.fecha < DATEADD(DAY,0,CAST(GETDATE() AS date))
		order by [fecha]
''' 
  ,db_conex)
tarjetasActivasGNC = tarjetasActivasGNC.convert_dtypes()
listaTarjetasActivasGNC = tarjetasActivasGNC['Tarjetas Activas'].tolist()
activas=len(tarjetasActivasGNC)

### Cantidad de Tarjetas Activas (usadas en los ultimos 14 dias)  a partir de ayer
tarjetasActivasGNCantsem = pd.read_sql('''
    select e.[Tarjetas Activas],e.fecha from (SELECT
        DISTINCT TARJETA AS 'Tarjetas Activas'
		,MAX(FECHASQL) as fecha
    FROM Rumaos.dbo.Despapro
    WHERE 
        TARJETA like 'i%'
        and FECHASQL >= '2022-02-01'
		and FECHASQL < DATEADD(DAY,-6,CAST(GETDATE() AS date))
        AND VOLUMEN > '0'
        AND CODPRODUCTO = 'GNC'
        and TARJETA NOT LIKE 'cc%'
        and TARJETA NOT LIKE 'ig99999%'
        group by TARJETA) as e 
		where e.fecha >= DATEADD(DAY,-20,CAST(GETDATE() AS date))
		and e.fecha < DATEADD(DAY,-6,CAST(GETDATE() AS date))
		order by [fecha]
          
''' 
  ,db_conex)
tarjetasActivasGNCantsem = tarjetasActivasGNCantsem.convert_dtypes()
listaTarjetasActivasGNCsempasada = tarjetasActivasGNCantsem['Tarjetas Activas'].tolist()
activassempasada=len(tarjetasActivasGNCantsem)

### Cantidad de tarjetas pasivas
tarjetasPasivasGNChoy = pd.read_sql(''' 
    select DISTINCT A.[Tarjetas Pasivas] AS 'Tarjetas Pasivas',A.fecha as fecha
		,A.CODPRODUCTO from (SELECT
        DISTINCT TARJETA AS 'Tarjetas Pasivas',MAX(FECHASQL) as fecha
		,CODPRODUCTO
    FROM Rumaos.dbo.Despapro
    WHERE 
        TARJETA like 'i%'
		and FECHASQL >= '2022-02-01'
		and FECHASQL < DATEADD(DAY,0,CAST(GETDATE() AS date))
        AND VOLUMEN > '0'
        AND CODPRODUCTO = 'GNC'
        and TARJETA NOT LIKE 'cc%'
        and TARJETA NOT LIKE 'ig99999%'
		GROUP BY TARJETA,CODPRODUCTO) as A where A.fecha < DATEADD(DAY,-14,CAST(GETDATE() AS date))
				order by fecha

''' 
  ,db_conex)
tarjetasPasivasGNChoy = tarjetasPasivasGNChoy.convert_dtypes()
listaTarjetasPasivasGNC = tarjetasPasivasGNChoy['Tarjetas Pasivas'].tolist()
### Cantidad de tarjetas pasivas semana Pasada
tarjetasPasivasGNCsem = pd.read_sql(''' 
    select DISTINCT A.[Tarjetas Pasivas] AS 'Tarjetas Pasivas',A.fecha as fecha
		,A.CODPRODUCTO from (SELECT
        DISTINCT TARJETA AS 'Tarjetas Pasivas',MAX(FECHASQL) as fecha
		,CODPRODUCTO
    FROM Rumaos.dbo.Despapro
    WHERE 
        TARJETA like 'i%'
		and FECHASQL >= '2022-02-01'
		and FECHASQL < DATEADD(DAY,-6,CAST(GETDATE() AS date))
        AND VOLUMEN > '0'
        AND CODPRODUCTO = 'GNC'
        and TARJETA NOT LIKE 'cc%'
        and TARJETA NOT LIKE 'ig99999%'
		GROUP BY TARJETA,CODPRODUCTO) as A where A.fecha < DATEADD(DAY,-20,CAST(GETDATE() AS date))
				order by fecha

''' 
  ,db_conex)
tarjetasPasivasGNCsem = tarjetasPasivasGNCsem.convert_dtypes()
listaTarjetasPasivasGNCsem = tarjetasPasivasGNCsem['Tarjetas Pasivas'].tolist()
### Cantidad de tarjetas pasivas Inicio Semana Pasada
tarjetas = pd.read_sql(''' 
    SELECT
    DISTINCT TARJETA,FECHASQL
    FROM Rumaos.dbo.Despapro
    WHERE 
        TARJETA like 'i%'
		and FECHASQL >= '2022-02-01'
		and FECHASQL < DATEADD(DAY,0,CAST(GETDATE() AS date))
        AND VOLUMEN > '0'
        AND CODPRODUCTO = 'GNC'
        and TARJETA NOT LIKE 'cc%'
        and TARJETA NOT LIKE 'ig99999%'

''' 
  ,db_conex)
tarjetas = tarjetas.convert_dtypes()


recaptadas=0
for elem in listaTarjetasActivasGNC:
    if elem not in listaTarjetasActivasGNCsempasada:
            recaptadas += 1



nuevopasiva = 0
for elem in listaTarjetasActivasGNCsempasada:
    if elem in listaTarjetasPasivasGNC:
        nuevopasiva += 1

pasivassempasada = len(listaTarjetasPasivasGNCsem)
pasivas= len(listaTarjetasPasivasGNC)
#### Creo logartimo para determinar Cuantas Tarjetas nuevas o recaptadas hay

recaptadasS= recaptadas
nuevosclientesS = tarjcreadasS
nuevosactivos = recaptadasS
#### Creo columnas del informe 
cantiadadtarjetasActivasGNC['Clientes Recaptados (Ultimos 7 dias)']= recaptadasS-nuevosclientesS
cantiadadtarjetasActivasGNC['Nuevos Clientes (ultimos 7 dias)']= nuevosclientesS
cantiadadtarjetasActivasGNC['TOTAL Pasivos']= pasivas
cantiadadtarjetasActivasGNC['Nuevos Clientes Pasivos (ultimos 7 dias)']= nuevopasiva
cantiadadtarjetasActivasGNC['Nuevos Clientes Activos(ultimos 7 dias)']= nuevosactivos
cantiadadtarjetasActivasGNC['TOTAL Pasivos semana pasada']=pasivassempasada
cantiadadtarjetasActivasGNC['TOTAL Activos semana pasada']=activassempasada
cantiadadtarjetasActivasGNC['TOTAL Activos']=activas

cantiadadtarjetasActivasGNC = cantiadadtarjetasActivasGNC.reindex(columns=['CODPRODUCTO','TOTAL Pasivos semana pasada','TOTAL Activos semana pasada','Nuevos Clientes (ultimos 7 dias)','Clientes Recaptados (Ultimos 7 dias)','Nuevos Clientes Pasivos (ultimos 7 dias)','TOTAL Pasivos','Nuevos Clientes Activos(ultimos 7 dias)','TOTAL Activos'])


###########################################################################
##############################   GO ######################################
###########################################################################
### Listado de tarjetas creadas Los ultimos 14 dias
tarjetascreadasGOsemanapasada = pd.read_sql(''' 
	select DISTINCT E.tarjeta, E.FECHA FROM (SELECT
        DISTINCT TARJETA, MIN(FECHASQL) AS FECHA
    FROM Rumaos.dbo.Despapro
    WHERE 
        TARJETA like 'i%'
        AND FECHASQL >= '2022-02-01'
        and FECHASQL < DATEADD(DAY,0,CAST(GETDATE() AS date))
        AND VOLUMEN > '0'
        AND CODPRODUCTO = 'GO'
        and TARJETA NOT LIKE 'cc%'
        and TARJETA NOT LIKE 'ig99999%'
		GROUP BY TARJETA) as e 
		WHERE E.FECHA >= DATEADD(DAY,-7,CAST(GETDATE() AS date))
        AND E.FECHA < DATEADD(DAY,0,CAST(GETDATE() AS date)) GROUP BY E.TARJETA,E.FECHA
''' 
  ,db_conex)
tarjetascreadasGOsemanapasada = tarjetascreadasGOsemanapasada.convert_dtypes()
listaTarjetasCreadasGOS = tarjetascreadasGOsemanapasada["tarjeta"].tolist()
tarjcreadasS= len(listaTarjetasCreadasGOS)

### Cantidad de Tarjetas Activas (usadas en los ultimos 14 dias)  a partir de ayer
cantiadadtarjetasActivasGO = pd.read_sql(''' 
    SELECT
        Count(DISTINCT TARJETA) AS 'Tarjetas Activas'
		,CODPRODUCTO
    FROM Rumaos.dbo.Despapro
    WHERE 
        TARJETA like 'i%'
        and FECHASQL >= DATEADD(DAY,-21,CAST(GETDATE() AS date))
		and FECHASQL < DATEADD(DAY,0,CAST(GETDATE() AS date))
        AND VOLUMEN > '0'
        AND CODPRODUCTO = 'GO'
        and TARJETA NOT LIKE 'cc%'
        and TARJETA NOT LIKE 'ig99999%'
        group by CODPRODUCTO
''' 
  ,db_conex)
cantiadadtarjetasActivasGO = cantiadadtarjetasActivasGO.convert_dtypes()



### Cantidad de Tarjetas Activas (usadas en los ultimos 14 dias)  a partir de ayer
tarjetasActivasGO = pd.read_sql(''' 
    select e.[Tarjetas Activas],e.fecha from (SELECT
        DISTINCT TARJETA AS 'Tarjetas Activas'
		,MAX(FECHASQL) as fecha
    FROM Rumaos.dbo.Despapro
    WHERE 
        TARJETA like 'i%'
        and FECHASQL >= '2022-02-01'
		and FECHASQL < DATEADD(DAY,0,CAST(GETDATE() AS date))
        AND VOLUMEN > '0'
        AND CODPRODUCTO = 'GO'
        and TARJETA NOT LIKE 'cc%'
        and TARJETA NOT LIKE 'ig99999%'
        group by TARJETA) as e 
		where e.fecha >= DATEADD(DAY,-14,CAST(GETDATE() AS date))
		and e.fecha < DATEADD(DAY,0,CAST(GETDATE() AS date))
		order by [fecha]
''' 
  ,db_conex)
tarjetasActivasGO = tarjetasActivasGO.convert_dtypes()
listaTarjetasActivasGO = tarjetasActivasGO['Tarjetas Activas'].tolist()
activas=len(tarjetasActivasGO)

### Cantidad de Tarjetas Activas (usadas en los ultimos 14 dias)  a partir de ayer
tarjetasActivasGOantsem = pd.read_sql('''
    select e.[Tarjetas Activas],e.fecha from (SELECT
        DISTINCT TARJETA AS 'Tarjetas Activas'
		,MAX(FECHASQL) as fecha
    FROM Rumaos.dbo.Despapro
    WHERE 
        TARJETA like 'i%'
        and FECHASQL >= '2022-02-01'
		and FECHASQL < DATEADD(DAY,-6,CAST(GETDATE() AS date))
        AND VOLUMEN > '0'
        AND CODPRODUCTO = 'GO'
        and TARJETA NOT LIKE 'cc%'
        and TARJETA NOT LIKE 'ig99999%'
        group by TARJETA) as e 
		where e.fecha >= DATEADD(DAY,-20,CAST(GETDATE() AS date))
		and e.fecha < DATEADD(DAY,-6,CAST(GETDATE() AS date))
		order by [fecha]
          
''' 
  ,db_conex)
tarjetasActivasGOantsem = tarjetasActivasGOantsem.convert_dtypes()
listaTarjetasActivasGOsempasada = tarjetasActivasGOantsem['Tarjetas Activas'].tolist()
activassempasada=len(tarjetasActivasGOantsem)

### Cantidad de tarjetas pasivas
tarjetasPasivasGOhoy = pd.read_sql(''' 
    select DISTINCT A.[Tarjetas Pasivas] AS 'Tarjetas Pasivas',A.fecha as fecha
		,A.CODPRODUCTO from (SELECT
        DISTINCT TARJETA AS 'Tarjetas Pasivas',MAX(FECHASQL) as fecha
		,CODPRODUCTO
    FROM Rumaos.dbo.Despapro
    WHERE 
        TARJETA like 'i%'
		and FECHASQL >= '2022-02-01'
		and FECHASQL < DATEADD(DAY,0,CAST(GETDATE() AS date))
        AND VOLUMEN > '0'
        AND CODPRODUCTO = 'GO'
        and TARJETA NOT LIKE 'cc%'
        and TARJETA NOT LIKE 'ig99999%'
		GROUP BY TARJETA,CODPRODUCTO) as A where A.fecha < DATEADD(DAY,-14,CAST(GETDATE() AS date))
				order by fecha

''' 
  ,db_conex)
tarjetasPasivasGOhoy = tarjetasPasivasGOhoy.convert_dtypes()
listaTarjetasPasivasGO = tarjetasPasivasGOhoy['Tarjetas Pasivas'].tolist()

### Cantidad de tarjetas pasivas semana Pasada
tarjetasPasivasgosem = pd.read_sql(''' 
    select DISTINCT A.[Tarjetas Pasivas] AS 'Tarjetas Pasivas',A.fecha as fecha
		,A.CODPRODUCTO from (SELECT
        DISTINCT TARJETA AS 'Tarjetas Pasivas',MAX(FECHASQL) as fecha
		,CODPRODUCTO
    FROM Rumaos.dbo.Despapro
    WHERE 
        TARJETA like 'i%'
		and FECHASQL >= '2022-02-01'
		and FECHASQL < DATEADD(DAY,-6,CAST(GETDATE() AS date))
        AND VOLUMEN > '0'
        AND CODPRODUCTO = 'GO'
        and TARJETA NOT LIKE 'cc%'
        and TARJETA NOT LIKE 'ig99999%'
		GROUP BY TARJETA,CODPRODUCTO) as A where A.fecha < DATEADD(DAY,-20,CAST(GETDATE() AS date))
				order by fecha

''' 
  ,db_conex)
tarjetasPasivasgosem = tarjetasPasivasgosem.convert_dtypes()
listaTarjetasPasivasGOsem = tarjetasPasivasgosem['Tarjetas Pasivas'].tolist()
### Cantidad de tarjetas pasivas Inicio Semana Pasada
tarjetas = pd.read_sql(''' 
    SELECT
    DISTINCT TARJETA,FECHASQL
    FROM Rumaos.dbo.Despapro
    WHERE 
        TARJETA like 'i%'
		and FECHASQL >= '2022-02-01'
		and FECHASQL < DATEADD(DAY,0,CAST(GETDATE() AS date))
        AND VOLUMEN > '0'
        AND CODPRODUCTO = 'GO'
        and TARJETA NOT LIKE 'cc%'
        and TARJETA NOT LIKE 'ig99999%'

''' 
  ,db_conex)
tarjetas = tarjetas.convert_dtypes()

recaptadas=0
for elem in listaTarjetasActivasGO:
    if elem not in listaTarjetasActivasGOsempasada:
            recaptadas += 1



nuevopasiva = 0
for elem in listaTarjetasActivasGOsempasada:
    if elem in listaTarjetasPasivasGO:
        nuevopasiva += 1

pasivassempasada = len(listaTarjetasPasivasGOsem)
pasivas= len(listaTarjetasPasivasGO)
#### Creo logartimo para determinar Cuantas Tarjetas nuevas o recaptadas hay

recaptadasS= recaptadas
nuevosclientesS = tarjcreadasS
nuevosactivos = recaptadasS
#### Creo columnas del informe 
cantiadadtarjetasActivasGO['Clientes Recaptados (Ultimos 7 dias)']= recaptadasS-nuevosclientesS
cantiadadtarjetasActivasGO['Nuevos Clientes (ultimos 7 dias)']= nuevosclientesS
cantiadadtarjetasActivasGO['TOTAL Pasivos']= pasivas
cantiadadtarjetasActivasGO['Nuevos Clientes Pasivos (ultimos 7 dias)']= nuevopasiva
cantiadadtarjetasActivasGO['Nuevos Clientes Activos(ultimos 7 dias)']= nuevosactivos
cantiadadtarjetasActivasGO['TOTAL Pasivos semana pasada']=pasivassempasada
cantiadadtarjetasActivasGO['TOTAL Activos semana pasada']=activassempasada
cantiadadtarjetasActivasGO['TOTAL Activos']=activas

cantiadadtarjetasActivasGO = cantiadadtarjetasActivasGO.reindex(columns=['CODPRODUCTO','TOTAL Pasivos semana pasada','TOTAL Activos semana pasada','Nuevos Clientes (ultimos 7 dias)','Clientes Recaptados (Ultimos 7 dias)','Nuevos Clientes Pasivos (ultimos 7 dias)','TOTAL Pasivos','Nuevos Clientes Activos(ultimos 7 dias)','TOTAL Activos'])

###########################################################################
##############################   EU ######################################
###########################################################################
### Listado de tarjetas creadas Los ultimos 14 dias
tarjetascreadasEUsemanapasada = pd.read_sql(''' 
	select DISTINCT E.tarjeta, E.FECHA FROM (SELECT
        DISTINCT TARJETA, MIN(FECHASQL) AS FECHA
    FROM Rumaos.dbo.Despapro
    WHERE 
        TARJETA like 'i%'
        AND FECHASQL >= '2022-02-01'
        and FECHASQL < DATEADD(DAY,0,CAST(GETDATE() AS date))
        AND VOLUMEN > '0'
        AND CODPRODUCTO = 'EU'
        and TARJETA NOT LIKE 'cc%'
        and TARJETA NOT LIKE 'ig99999%'
		GROUP BY TARJETA) as e 
		WHERE E.FECHA >= DATEADD(DAY,-7,CAST(GETDATE() AS date))
        AND E.FECHA < DATEADD(DAY,0,CAST(GETDATE() AS date)) GROUP BY E.TARJETA,E.FECHA
''' 
  ,db_conex)
tarjetascreadasEUsemanapasada = tarjetascreadasEUsemanapasada.convert_dtypes()
listaTarjetasCreadasEUS = tarjetascreadasEUsemanapasada["tarjeta"].tolist()
tarjcreadasS= len(listaTarjetasCreadasEUS)

### Cantidad de Tarjetas Activas (usadas en los ultimos 14 dias)  a partir de ayer
cantiadadtarjetasActivasEU = pd.read_sql(''' 
    SELECT
        Count(DISTINCT TARJETA) AS 'Tarjetas Activas'
		,CODPRODUCTO
    FROM Rumaos.dbo.Despapro
    WHERE 
        TARJETA like 'i%'
        and FECHASQL >= DATEADD(DAY,-14,CAST(GETDATE() AS date))
		and FECHASQL < DATEADD(DAY,0,CAST(GETDATE() AS date))
        AND VOLUMEN > '0'
        AND CODPRODUCTO = 'EU'
        and TARJETA NOT LIKE 'cc%'
        and TARJETA NOT LIKE 'ig99999%'
        group by CODPRODUCTO
''' 
  ,db_conex)
cantiadadtarjetasActivasEU = cantiadadtarjetasActivasEU.convert_dtypes()



### Cantidad de Tarjetas Activas (usadas en los ultimos 14 dias)  a partir de ayer
tarjetasActivasEU = pd.read_sql(''' 
    select e.[Tarjetas Activas],e.fecha from (SELECT
        DISTINCT TARJETA AS 'Tarjetas Activas'
		,MAX(FECHASQL) as fecha
    FROM Rumaos.dbo.Despapro
    WHERE 
        TARJETA like 'i%'
        and FECHASQL >= '2022-02-01'
		and FECHASQL < DATEADD(DAY,0,CAST(GETDATE() AS date))
        AND VOLUMEN > '0'
        AND CODPRODUCTO = 'EU'
        and TARJETA NOT LIKE 'cc%'
        and TARJETA NOT LIKE 'ig99999%'
        group by TARJETA) as e 
		where e.fecha >= DATEADD(DAY,-14,CAST(GETDATE() AS date))
		and e.fecha < DATEADD(DAY,0,CAST(GETDATE() AS date))
		order by [fecha]
''' 
  ,db_conex)
tarjetasActivasEU = tarjetasActivasEU.convert_dtypes()
listaTarjetasActivasEU = tarjetasActivasEU['Tarjetas Activas'].tolist()
activas=len(tarjetasActivasEU)

### Cantidad de Tarjetas Activas (usadas en los ultimos 14 dias)  a partir de ayer
tarjetasActivasEUantsem = pd.read_sql('''
    select e.[Tarjetas Activas],e.fecha from (SELECT
        DISTINCT TARJETA AS 'Tarjetas Activas'
		,MAX(FECHASQL) as fecha
    FROM Rumaos.dbo.Despapro
    WHERE 
        TARJETA like 'i%'
        and FECHASQL >= '2022-02-01'
		and FECHASQL < DATEADD(DAY,-6,CAST(GETDATE() AS date))
        AND VOLUMEN > '0'
        AND CODPRODUCTO = 'EU'
        and TARJETA NOT LIKE 'cc%'
        and TARJETA NOT LIKE 'ig99999%'
        group by TARJETA) as e 
		where e.fecha >= DATEADD(DAY,-20,CAST(GETDATE() AS date))
		and e.fecha < DATEADD(DAY,-6,CAST(GETDATE() AS date))
		order by [fecha]
          
''' 
  ,db_conex)
tarjetasActivasEUantsem = tarjetasActivasEUantsem.convert_dtypes()
listaTarjetasActivasEUsempasada = tarjetasActivasEUantsem['Tarjetas Activas'].tolist()
activassempasada=len(tarjetasActivasEUantsem)

### Cantidad de tarjetas pasivas
tarjetasPasivasEUhoy = pd.read_sql(''' 
    select DISTINCT A.[Tarjetas Pasivas] AS 'Tarjetas Pasivas',A.fecha as fecha
		,A.CODPRODUCTO from (SELECT
        DISTINCT TARJETA AS 'Tarjetas Pasivas',MAX(FECHASQL) as fecha
		,CODPRODUCTO
    FROM Rumaos.dbo.Despapro
    WHERE 
        TARJETA like 'i%'
		and FECHASQL >= '2022-02-01'
		and FECHASQL < DATEADD(DAY,0,CAST(GETDATE() AS date))
        AND VOLUMEN > '0'
        AND CODPRODUCTO = 'EU'
        and TARJETA NOT LIKE 'cc%'
        and TARJETA NOT LIKE 'ig99999%'
		GROUP BY TARJETA,CODPRODUCTO) as A where A.fecha < DATEADD(DAY,-14,CAST(GETDATE() AS date))
				order by fecha

''' 
  ,db_conex)
tarjetasPasivasEUhoy = tarjetasPasivasEUhoy.convert_dtypes()
listaTarjetasPasivasEU = tarjetasPasivasEUhoy['Tarjetas Pasivas'].tolist()

### Cantidad de tarjetas pasivas semana Pasada
tarjetasPasivasEUsem = pd.read_sql(''' 
    select DISTINCT A.[Tarjetas Pasivas] AS 'Tarjetas Pasivas',A.fecha as fecha
		,A.CODPRODUCTO from (SELECT
        DISTINCT TARJETA AS 'Tarjetas Pasivas',MAX(FECHASQL) as fecha
		,CODPRODUCTO
    FROM Rumaos.dbo.Despapro
    WHERE 
        TARJETA like 'i%'
		and FECHASQL >= '2022-02-01'
		and FECHASQL < DATEADD(DAY,-6,CAST(GETDATE() AS date))
        AND VOLUMEN > '0'
        AND CODPRODUCTO = 'EU'
        and TARJETA NOT LIKE 'cc%'
        and TARJETA NOT LIKE 'ig99999%'
		GROUP BY TARJETA,CODPRODUCTO) as A where A.fecha < DATEADD(DAY,-20,CAST(GETDATE() AS date))
				order by fecha

''' 
  ,db_conex)
tarjetasPasivasEUsem = tarjetasPasivasEUsem.convert_dtypes()
listaTarjetasPasivasEUsem = tarjetasPasivasEUsem['Tarjetas Pasivas'].tolist()
### Cantidad de tarjetas pasivas Inicio Semana Pasada
tarjetas = pd.read_sql(''' 
    SELECT
    DISTINCT TARJETA,FECHASQL
    FROM Rumaos.dbo.Despapro
    WHERE 
        TARJETA like 'i%'
		and FECHASQL >= '2022-02-01'
		and FECHASQL < DATEADD(DAY,0,CAST(GETDATE() AS date))
        AND VOLUMEN > '0'
        AND CODPRODUCTO = 'EU'
        and TARJETA NOT LIKE 'cc%'
        and TARJETA NOT LIKE 'ig99999%'

''' 
  ,db_conex)
tarjetas = tarjetas.convert_dtypes()

recaptadas=0
for elem in listaTarjetasActivasEU:
    if elem not in listaTarjetasActivasEUsempasada:
            recaptadas += 1


nuevopasiva = 0
for elem in listaTarjetasActivasEUsempasada:
    if elem in listaTarjetasPasivasEU:
        nuevopasiva += 1

pasivassempasada = len(listaTarjetasPasivasEUsem)
pasivas= len(listaTarjetasPasivasEU)
#### Creo logartimo para determinar Cuantas Tarjetas nuevas o recaptadas hay

recaptadasS= recaptadas
nuevosclientesS = tarjcreadasS
nuevosactivos = recaptadasS
#### Creo columnas del informe 
cantiadadtarjetasActivasEU['Clientes Recaptados (Ultimos 7 dias)']= recaptadasS-nuevosclientesS
cantiadadtarjetasActivasEU['Nuevos Clientes (ultimos 7 dias)']= nuevosclientesS
cantiadadtarjetasActivasEU['TOTAL Pasivos']= pasivas
cantiadadtarjetasActivasEU['Nuevos Clientes Pasivos (ultimos 7 dias)']= nuevopasiva
cantiadadtarjetasActivasEU['Nuevos Clientes Activos(ultimos 7 dias)']= nuevosactivos
cantiadadtarjetasActivasEU['TOTAL Pasivos semana pasada']=pasivassempasada
cantiadadtarjetasActivasEU['TOTAL Activos semana pasada']=activassempasada
cantiadadtarjetasActivasEU['TOTAL Activos']=activas

cantiadadtarjetasActivasEU = cantiadadtarjetasActivasEU.reindex(columns=['CODPRODUCTO','TOTAL Pasivos semana pasada','TOTAL Activos semana pasada','Nuevos Clientes (ultimos 7 dias)','Clientes Recaptados (Ultimos 7 dias)','Nuevos Clientes Pasivos (ultimos 7 dias)','TOTAL Pasivos','Nuevos Clientes Activos(ultimos 7 dias)','TOTAL Activos'])

cantidadTotal = cantiadadtarjetasActivasGNC.merge(cantiadadtarjetasActivasGO,on=['CODPRODUCTO','TOTAL Pasivos semana pasada','TOTAL Activos semana pasada','Nuevos Clientes (ultimos 7 dias)','Clientes Recaptados (Ultimos 7 dias)','Nuevos Clientes Pasivos (ultimos 7 dias)','TOTAL Pasivos','Nuevos Clientes Activos(ultimos 7 dias)','TOTAL Activos'],how='outer')

cantidadTotal = cantidadTotal.merge(cantiadadtarjetasActivasEU,on=['CODPRODUCTO','TOTAL Pasivos semana pasada','TOTAL Activos semana pasada','Nuevos Clientes (ultimos 7 dias)','Clientes Recaptados (Ultimos 7 dias)','Nuevos Clientes Pasivos (ultimos 7 dias)','TOTAL Pasivos','Nuevos Clientes Activos(ultimos 7 dias)','TOTAL Activos'],how='outer')

cantidadTotal=cantidadTotal.rename({'Nuevos Clientes (ultimos 7 dias)':'Nuevos Clientes','Clientes Recaptados (Ultimos 7 dias)':'Clientes Recaptados','Nuevos Clientes Pasivos (ultimos 7 dias)':'Nuevos Clientes Pasivos','Nuevos Clientes Activos(ultimos 7 dias)':'Nuevos Clientes Activos','TOTAL Activos':'TOTAL Activos Hoy','TOTAL Pasivos':'TOTAL Pasivos Hoy'},axis=1)
raitos=cantidadTotal

raitos['% de Recaptacion']= raitos['Clientes Recaptados']/raitos['TOTAL Activos Hoy']
raitos['% de Nuevos Clientes']=raitos['Nuevos Clientes']/raitos['TOTAL Activos Hoy']
raitos['Total Variacion C. Pasivos Semanal %']= -(raitos['Nuevos Clientes Pasivos']/raitos['TOTAL Activos Hoy'])
raitos['Total Variacion C. Activos Semanal %']= (raitos['Nuevos Clientes Activos']/raitos['TOTAL Activos Hoy'])
raitos['Variacion Clientes Semanal %']= (raitos['TOTAL Activos Hoy']/raitos['TOTAL Activos semana pasada'])-1

cantidadTotal = cantidadTotal.reindex(columns=['CODPRODUCTO','Nuevos Clientes','Clientes Recaptados','Nuevos Clientes Pasivos','Nuevos Clientes Activos','TOTAL Pasivos Hoy','TOTAL Activos Hoy'])

raitos = raitos.reindex(columns=['CODPRODUCTO','% de Nuevos Clientes','% de Recaptacion','Total Variacion C. Activos Semanal %','Total Variacion C. Pasivos Semanal %','Variacion Clientes Semanal %'])






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

def _estiladorVtaTitulo2(df, list_Col_Num, list_Col_Perc,colcaract, titulo):
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
        ])\
        .apply(lambda x: ["color: white" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1)
    resultado = resultado.background_gradient(
        cmap="Dark2" # Red->Yellow->Green
        ,vmin=0.001
        ,vmax=0.007
        ,subset=pd.IndexSlice["Total Variacion C. Activos Semanal %"]
    )
    resultado = resultado.background_gradient(
        cmap="RdYlGn" # Red->Yellow->Green
        ,vmin=-0.01
        ,vmax=0.01
        ,subset=pd.IndexSlice["Variacion Clientes Semanal %"]
    )

    return resultado
### COLUMNAS Con Numeros Enteros
numCols = ['Nuevos Clientes','Clientes Recaptados','Nuevos Clientes Pasivos','TOTAL Pasivos Hoy','Nuevos Clientes Activos','TOTAL Activos Hoy'
         ]
### COLUMNAS con Porcentajes
percColsPen = [
]

#### COLUMNAS Con caracteres
colcaract = [
]
###### Aplico el formato elegido a la imagen

cantidadTotal = _estiladorVtaTitulo(cantidadTotal,numCols,percColsPen,colcaract, "CRM Ultimos 7 dias")
numCols = []
percColsPen = ['% de Nuevos Clientes','% de Recaptacion','Total Variacion C. Activos Semanal %','Total Variacion C. Pasivos Semanal %','Variacion Clientes Semanal %']
raitos = _estiladorVtaTitulo2(raitos,numCols,percColsPen,colcaract,"")

###### DEFINO NOMBRE Y UBICACION DE DONDE SE VA A GUARDAR LA IMAGEN

def _append_images(listOfImages, direction='vertical',
                  bg_color=(255,255,255), alignment='center'):
    """
    Appends images in horizontal/vertical direction.

    Args:
        listOfImages: List of images with complete path
        direction: direction of concatenation, 'horizontal' or 'vertical'
        bg_color: Background color (default: white)
        alignment: alignment mode if images need padding;
           'left', 'right', 'top', 'bottom', or 'center'

    Returns:
        Concatenated image as a new PIL image object.
    """
    images = [Image.open(x) for x in listOfImages]
    widths, heights = zip(*(i.size for i in images))

    if direction=='horizontal':
        new_width = sum(widths)
        new_height = max(heights)
    else:
        new_width = max(widths)
        new_height = sum(heights)

    new_im = Image.new('RGB', (new_width, new_height), color=bg_color)

    offset = 0
    for im in images:
        if direction=='horizontal':
            y = 0
            if alignment == 'center':
                y = int((new_height - im.size[1])/2)
            elif alignment == 'bottom':
                y = new_height - im.size[1]
            new_im.paste(im, (offset, y))
            offset += im.size[0]
        else:
            x = 0
            if alignment == 'center':
                x = int((new_width - im.size[0])/2)
            elif alignment == 'right':
                x = new_width - im.size[0]
            new_im.paste(im, (x, offset))
            offset += im.size[1]

    return new_im

ubicacion = "C:/Informes/InfoLubri_y_RedMas/"
nombre = "RedMasClientes.png"
nombreratio='Ratio.png'
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

df_to_image(cantidadTotal, ubicacion, nombre)
df_to_image(raitos, ubicacion, nombreratio)
listaImg = [ubicacion + nombre, ubicacion + nombreratio]
# Merge DFs images vertically and save it as a .png
fusionImg = _append_images(listaImg, direction="vertical")
fusionImg.save(ubicacion + "CRM.png")
#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)




