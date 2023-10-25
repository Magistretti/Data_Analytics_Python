import os
import math
import re
import numpy as np
from DatosLogin import login
from Conectores import conectorMSSQL
from PIL import Image
from calendar import monthrange
import pandas as pd
from pandas.api.types import CategoricalDtype
import dataframe_image as dfi
import pyodbc #Library to connect to Microsoft SQL Server
from DatosLogin import login #Holds connection data to the SQL Server
import sys
import pathlib
from datetime import datetime
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

import datetime
hoy = datetime.datetime.now()
ayer = hoy - timedelta(days=1)

# Obtener la fecha actual
fecha_actual = datetime.date.today()

# Obtener el primer día del mes actual del año pasado
inicio_mes_pasado = fecha_actual.replace(year=fecha_actual.year - 1, month=fecha_actual.month, day=1)

# Obtener el último día del mes actual del año pasado
ultimo_dia_mes_pasado = inicio_mes_pasado.replace(day=1, month=inicio_mes_pasado.month % 12 + 1) - datetime.timedelta(days=1)

#### Creo datos para volumen Proyectado
diasdelmes=(tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d")
mes=(tiempoInicio-pd.to_timedelta(1,"days")).strftime("%m")
año=(tiempoInicio-pd.to_timedelta(1,"days")).strftime("%y")
diasdelmes = int(diasdelmes)
mes=int(mes)
año=int(año)
num_days = monthrange(año,mes)[1] # num_days = 31.
num_days=int(num_days)

######### LECTURA DE EXCEL DE COMISIONES Y TRAMOS NS
ubicacion = "C:/Informes/Margen_Playa/"
aux_semanal = "TRAMOSyCOSTOSnaftas.xlsx"
comisionNS =pd.read_excel(ubicacion+aux_semanal, sheet_name= 'Comision y Tramo NS')
comisionNS = comisionNS.convert_dtypes()

######### LECTURA DE EXCEL DE COMISIONES Y TRAMOS EU
ubicacion = "C:/Informes/Margen_Playa/"
aux_semanal = "TRAMOSyCOSTOSnaftas.xlsx"
comisionNU =pd.read_excel(ubicacion+aux_semanal, sheet_name= 'Comision y Tramo NU')
comisionNU = comisionNU.convert_dtypes()

######### LECTURA DE EXCEL DE COSTOS DAPSA EU
ubicacion = "C:/Informes/Margen_Playa/"
aux_semanal = "TRAMOSyCOSTOSnaftas.xlsx"
costoDapsaNS =pd.read_excel(ubicacion+aux_semanal, sheet_name= 'Costo Dapsa NS')
costoDapsaNS = costoDapsaNS.convert_dtypes()

######### LECTURA DE EXCEL DE COSTOS DAPSA EU
ubicacion = "C:/Informes/Margen_Playa/"
aux_semanal = "TRAMOSyCOSTOSnaftas.xlsx"
costoDapsaNU =pd.read_excel(ubicacion+aux_semanal, sheet_name= 'Costo Dapsa NU')
costoDapsaNU = costoDapsaNU.convert_dtypes()

######### PRECIO DOLAR ################

import requests
from bs4 import BeautifulSoup

url = "https://www.dolarhoy.com/cotizaciondolarblue"
response = requests.get(url)
html = response.content
soup = BeautifulSoup(html, "html.parser")

dolar_blue_HOY = soup.find("div", {"class": "value"})

dolar_blue = dolar_blue_HOY.text

dolar_blue = dolar_blue.replace("$", "").strip()

dolar_blue=float(dolar_blue)
################################################
############# Volumen diario GASOLEOS YPF 
################################################

df_naftasTotal = pd.read_sql('''
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
    SELECT UEN  
        ,[CODPRODUCTO]
        ,SUM([VTATOTVOL]) AS 'VENTA TOTAL VOLUMEN'
		,FECHASQL
		,sum(VTAPRECARTELVOL) AS 'Ventas Efectivo'
		,MAX(PRECARTEL) as 'Precio Cartel'
		,sum(VTACTACTEVOL + VTAADELVOL) AS 'Venta Cta Cte'
		,MAX(PREVTAADEL) AS 'Precio Cta Cte'
		,sum(VTAPROMOSVOL) AS 'ventas Promos'
    FROM [Rumaos].[dbo].[EmpVenta]
    WHERE FECHASQL >= @inicioMesActual
        AND FECHASQL <= @hoy
	    AND VTATOTVOL > '0'
		and (CODPRODUCTO = 'NS' OR CODPRODUCTO = 'NU')
		and UEN IN (
            'LAMADRID'
            ,'AZCUENAGA'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
            ,'SAN JOSE'
        )
		group BY FECHASQL,CODPRODUCTO,UEN
		order by CODPRODUCTO,UEN

  '''      ,db_conex)
df_naftasTotal = df_naftasTotal.convert_dtypes()

############################################
#####Volumen Diario GASOLEOS YPF PLAYA######
############################################

df_naftasPlaya = pd.read_sql('''
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
	SELECT emp.UEN,emp.FECHASQL,sum(emp.VOLUMEN) as 'Volumen YER',emp.CODPRODUCTO
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL >= @inicioMesActual
        AND FECHASQL <= @hoy
		and emp.UEN IN (
            'LAMADRID'
            ,'AZCUENAGA'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
            ,'SAN JOSE'
        )
		and (emp.CODPRODUCTO = 'NS' OR emp.CODPRODUCTO = 'NU')
        AND  P.[DESCRIPCION] like '%ruta%'
		group by emp.FECHASQL,emp.CODPRODUCTO,emp.UEN
		order by CODPRODUCTO,UEN
'''      ,db_conex)
df_naftasPlaya = df_naftasPlaya.convert_dtypes()


############################################
#####Volumen Diario GASOLEOS REDMAS APPYPF######
############################################

df_naftasREDMAS = pd.read_sql('''
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
	SELECT emp.UEN,emp.FECHASQL,sum(emp.VOLUMEN) as 'Volumen REDMAS',emp.CODPRODUCTO,MAX(emp.PRECIO) AS 'Precio Promos'
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL >= @inicioMesActual
        AND FECHASQL <= @hoy
		and emp.UEN IN (
            'LAMADRID'
            ,'AZCUENAGA'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
            ,'SAN JOSE'
        )
		and (emp.CODPRODUCTO = 'NS' OR emp.CODPRODUCTO = 'NU')
        AND  (P.[DESCRIPCION] like '%PROMO%' OR P.DESCRIPCION LIKE '%MERCO%' OR P.DESCRIPCION LIKE '%MAS%')
		group by emp.FECHASQL,emp.CODPRODUCTO,emp.UEN
		order by CODPRODUCTO,UEN
'''      ,db_conex)
df_naftasREDMAS = df_naftasREDMAS.convert_dtypes()

df_naftasTotal=df_naftasTotal.merge(df_naftasREDMAS,on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')
df_naftasTotal=df_naftasTotal.merge(df_naftasPlaya,on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')
df_naftasTotal = df_naftasTotal.fillna(0)

df_naftasTotal['Volumen Total Vendido']=(df_naftasTotal['Ventas Efectivo']+df_naftasTotal['Venta Cta Cte']
                                           +df_naftasTotal['Volumen REDMAS'])


df_naftasTotal = df_naftasTotal.reindex(columns=['UEN','FECHASQL','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Precio Cartel'])
df_naftasTotal = df_naftasTotal.fillna(0)
df_naftasTotal= df_naftasTotal.loc[df_naftasTotal["FECHASQL"] <= ayer.strftime('%Y-%m-%d'),:]

df_naftasTotal=df_naftasTotal.sort_values(['UEN','CODPRODUCTO','FECHASQL'])
costoNaftasYPF=df_naftasTotal

# Definir una función para aplicar la comisión acumulada según el tramo de ventas
def aplicar_comision_acumuladaNS(df,estacion):
    df=df.sort_values(['UEN','FECHASQL'])
    comi = comisionNS.loc[(comisionNS["UEN"] == estacion),:]
    comi = comi.reset_index()
    df['Ventas Acumuladas']=df['Volumen Total Vendido'].cumsum()
    for i in df.index:
        if df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO I NS']:
            df.loc[i,'Marcador'] = 1
        elif df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO II NS']:
            df.loc[i,'Marcador'] = 2
        elif df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO III NS']:
                df.loc[i,'Marcador'] = 3
        else:
            if df.loc[i,'Ventas Acumuladas'] > comi.loc[0,'TRAMO III NS']:
                df.loc[i,'Marcador'] = 4      

    for i in df.index:
        if df.loc[i,'Marcador'] == 1:
            df.loc[i,'COMISION']=comi.loc[0,'COMISION TRAMO I NS']
        elif df.loc[i,'Marcador'] == 2:
            if df.loc[i,'Marcador'] != df.loc[i-1,'Marcador']:
                tsig = (df.loc[i,'Ventas Acumuladas'] - comi.loc[0,'TRAMO I NS'])
                tant = (df.loc[i,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO I NS']*tant)+(comi.loc[0,'COMISION TRAMO II NS']*tsig)
                df.loc[i,'COMISION']= comis/df.loc[i,'Volumen Total Vendido']
            else:
                df.loc[i,'COMISION']=comi.loc[0,'COMISION TRAMO II NS']
        elif df.loc[i,'Marcador'] == 3:
            if df.loc[i,'Marcador'] != df.loc[i-1,'Marcador']:
                tsig = (df.loc[i,'Ventas Acumuladas'] - comi.loc[0,'TRAMO II NS'])
                tant = (df.loc[i,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO II NS']*tant)+(comi.loc[0,'COMISION TRAMO III NS']*tsig)
                df.loc[i,'COMISION']= comis/df.loc[i,'Volumen Total Vendido']
            else:
                df.loc[i,'COMISION']=comi.loc[0,'COMISION TRAMO III NS']
        elif df.loc[i,'Marcador'] == 4:
            if df.loc[i,'Marcador'] != df.loc[i-1,'Marcador']:
                tsig = (df.loc[i,'Ventas Acumuladas'] - comi.loc[0,'TRAMO III NS'])
                tant = (df.loc[i,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO III NS']*tant)+(comi.loc[0,'COMISION TRAMO IV NS']*tsig)
                df.loc[i,'COMISION']= comis/df.loc[i,'Volumen Total Vendido']
            else:
                df.loc[i,'COMISION']=comi.loc[0,'COMISION TRAMO IV NS']
    df['Comision USD']=   (df['Volumen Total Vendido']*(df['Precio Cartel']/dolar_blue)*df['COMISION'])+(df['Volumen YER']*(df['Precio Cartel']/dolar_blue)*comi.loc[0,'COMISION TRAMO I NS'])
    df['Total Vendido USD']=   (df['Volumen Total Vendido']*(df['Precio Cartel']/dolar_blue))+(df['Volumen YER']*(df['Precio Cartel']/dolar_blue))
    df = df.reindex(columns=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'])
    df = df.groupby(
        ["UEN",'CODPRODUCTO']
        , as_index=False
    ).sum()
    return df

### Aplico la comision a cada estacion para el producto NS
perdriel1= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'PERDRIEL            ') & (df_naftasTotal["CODPRODUCTO"] == 'NS   '),:]
perdriel1 = aplicar_comision_acumuladaNS(perdriel1,'PERDRIEL            ')
perdriel2= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'PERDRIEL2           ') & (df_naftasTotal["CODPRODUCTO"] == 'NS   '),:]
perdriel2 = aplicar_comision_acumuladaNS(perdriel2,'PERDRIEL2           ')
azcuenaga= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'AZCUENAGA           ') & (df_naftasTotal["CODPRODUCTO"] == 'NS   '),:]
azcuenaga = aplicar_comision_acumuladaNS(azcuenaga,'AZCUENAGA           ')
san_Jose= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'SAN JOSE            ') & (df_naftasTotal["CODPRODUCTO"] == 'NS   '),:]
san_Jose = aplicar_comision_acumuladaNS(san_Jose,'SAN JOSE            ')
puente_Olive= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'PUENTE OLIVE        ') & (df_naftasTotal["CODPRODUCTO"] == 'NS   '),:]
puente_Olive = aplicar_comision_acumuladaNS(puente_Olive,'PUENTE OLIVE        ')
lamadrid= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'LAMADRID            ') & (df_naftasTotal["CODPRODUCTO"] == 'NS   '),:]
lamadrid = aplicar_comision_acumuladaNS(lamadrid,'LAMADRID            ')

### Creo tabla de YPF NS
mbcNSYPF=perdriel1.merge(perdriel2,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')
mbcNSYPF=mbcNSYPF.merge(azcuenaga,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')
mbcNSYPF=mbcNSYPF.merge(san_Jose,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')
mbcNSYPF=mbcNSYPF.merge(puente_Olive,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')
mbcNSYPF=mbcNSYPF.merge(lamadrid,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')

def aplicar_comision_acumuladaNU(df,estacion):
    df=df.sort_values(['UEN','FECHASQL'])
    comi = comisionNU.loc[(comisionNU["UEN"] == estacion),:]
    comi = comi.reset_index()
    df['Ventas Acumuladas']=df['Volumen Total Vendido'].cumsum()
    for i in df.index:
        if df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO I NU']:
            df.loc[i,'Marcador'] = 1
        elif df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO II NU']:
            df.loc[i,'Marcador'] = 2
        elif df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO III NU']:
                df.loc[i,'Marcador'] = 3
        else:
            if df.loc[i,'Ventas Acumuladas'] > comi.loc[0,'TRAMO III NU']:
                df.loc[i,'Marcador'] = 4      

    for i in df.index:
        if df.loc[i,'Marcador'] == 1:
            df.loc[i,'COMISION']=comi.loc[0,'COMISION TRAMO I NU']
        elif df.loc[i,'Marcador'] == 2:
            if df.loc[i,'Marcador'] != df.loc[i-1,'Marcador']:
                tsig = (df.loc[i,'Ventas Acumuladas'] - comi.loc[0,'TRAMO I NU'])
                tant = (df.loc[i,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO I NU']*tant)+(comi.loc[0,'COMISION TRAMO II NU']*tsig)
                df.loc[i,'COMISION']= comis/df.loc[i,'Volumen Total Vendido']
            else:
                df.loc[i,'COMISION']=comi.loc[0,'COMISION TRAMO II NU']
        elif df.loc[i,'Marcador'] == 3:
            if df.loc[i,'Marcador'] != df.loc[i-1,'Marcador']:
                tsig = (df.loc[i,'Ventas Acumuladas'] - comi.loc[0,'TRAMO II NU'])
                tant = (df.loc[i,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO II NU']*tant)+(comi.loc[0,'COMISION TRAMO III NU']*tsig)
                df.loc[i,'COMISION']= comis/df.loc[i,'Volumen Total Vendido']
            else:
                df.loc[i,'COMISION']=comi.loc[0,'COMISION TRAMO III NU']
        elif df.loc[i,'Marcador'] == 4:
            if df.loc[i,'Marcador'] != df.loc[i-1,'Marcador']:
                tsig = (df.loc[i,'Ventas Acumuladas'] - comi.loc[0,'TRAMO III NU'])
                tant = (df.loc[i,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO III NU']*tant)+(comi.loc[0,'COMISION TRAMO IV NU']*tsig)
                df.loc[i,'COMISION']= comis/df.loc[i,'Volumen Total Vendido']
            else:
                df.loc[i,'COMISION']=comi.loc[0,'COMISION TRAMO IV NU']
    df['Comision USD']=   (df['Volumen Total Vendido']*(df['Precio Cartel']/dolar_blue)*df['COMISION'])+(df['Volumen YER']*(df['Precio Cartel']/dolar_blue)*comi.loc[0,'COMISION TRAMO I NU'])
    df['Total Vendido USD']=   (df['Volumen Total Vendido']*(df['Precio Cartel']/dolar_blue))+(df['Volumen YER']*(df['Precio Cartel']/dolar_blue))
    df = df.reindex(columns=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'])
    df = df.groupby(
        ["UEN",'CODPRODUCTO']
        , as_index=False
    ).sum()
    return df



### Aplico la comision a cada estacion para el producto EU
perdriel1= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'PERDRIEL            ') & (df_naftasTotal["CODPRODUCTO"] == 'NU   '),:]
perdriel1 = aplicar_comision_acumuladaNU(perdriel1,'PERDRIEL            ')
perdriel2= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'PERDRIEL2           ') & (df_naftasTotal["CODPRODUCTO"] == 'NU   '),:]
perdriel2 = aplicar_comision_acumuladaNU(perdriel2,'PERDRIEL2           ')
azcuenaga= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'AZCUENAGA           ') & (df_naftasTotal["CODPRODUCTO"] == 'NU   '),:]
azcuenaga = aplicar_comision_acumuladaNU(azcuenaga,'AZCUENAGA           ')
san_Jose= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'SAN JOSE            ') & (df_naftasTotal["CODPRODUCTO"] == 'NU   '),:]
san_Jose = aplicar_comision_acumuladaNU(san_Jose,'SAN JOSE            ')
puente_Olive= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'PUENTE OLIVE        ') & (df_naftasTotal["CODPRODUCTO"] == 'NU   '),:]
puente_Olive = aplicar_comision_acumuladaNU(puente_Olive,'PUENTE OLIVE        ')
lamadrid= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'LAMADRID            ') & (df_naftasTotal["CODPRODUCTO"] == 'NU   '),:]
lamadrid = aplicar_comision_acumuladaNU(lamadrid,'LAMADRID            ')

### Creo tabla de YPF EU
mbcNUYPF=perdriel1.merge(perdriel2,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')
mbcNUYPF=mbcNUYPF.merge(azcuenaga,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')
mbcNUYPF=mbcNUYPF.merge(san_Jose,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')
mbcNUYPF=mbcNUYPF.merge(puente_Olive,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')
mbcNUYPF=mbcNUYPF.merge(lamadrid,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')

### Concateno Tabla De YPF NS con YPF EU
mbcYPF=mbcNSYPF.merge(mbcNUYPF,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')

##################################################
####### VENTAS DAPSA #############################
##################################################

# Obtener la fecha actual
fecha_actual = datetime.datetime.now()
# Crear una nueva fecha con el primer día del mes actual
primer_dia_mes = datetime.datetime(fecha_actual.year, fecha_actual.month, 1)

####  Volumen diario GASOLEOS YPF 
df_naftasdapsa = pd.read_sql(''' 
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
    SELECT UEN  
        ,[CODPRODUCTO]
        ,SUM([VTATOTVOL]) AS 'VENTA TOTAL VOLUMEN'
		,FECHASQL
		,sum(VTAPRECARTELVOL) AS 'Ventas Efectivo'
		,MAX(PRECARTEL) as 'Precio Cartel'
		,sum(VTACTACTEVOL + VTAADELVOL) AS 'Venta Cta Cte'
		,MAX(PREVTAADEL) AS 'Precio Cta Cte'
		,sum(VTAPROMOSVOL) AS 'ventas Promos'
    FROM [Rumaos].[dbo].[EmpVenta]
    WHERE FECHASQL >= @inicioMesActual
        AND FECHASQL <= @hoy
	    AND VTATOTVOL > '0'
		and (CODPRODUCTO = 'NS' OR CODPRODUCTO = 'NU')
		and UEN IN (
            'SARMIENTO'
            ,'URQUIZA'
            ,'MITRE'
            ,'MERC GUAYMALLEN'
			,'ADOLFO CALLE'
			,'VILLANUEVA'
			,'MERCADO 2'
			,'LAS HERAS'
        )
		group BY FECHASQL,CODPRODUCTO,UEN
		order by CODPRODUCTO,UEN

   '''  ,db_conex)
df_naftasdapsa = df_naftasdapsa.convert_dtypes()
df_naftasdapsa =df_naftasdapsa.fillna(0)
### Descuentos
df_naftadapsaDesc = pd.read_sql('''

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
	SELECT emp.UEN,EmP.FECHASQL
        ,EmP.[CODPRODUCTO]
        ,SUM(-EmP.[VOLUMEN]) as 'Descuentos'
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL >= @inicioMesActual
        AND FECHASQL <= @hoy
		and emp.UEN IN (
            'SARMIENTO'
            ,'URQUIZA'
            ,'MITRE'
            ,'MERC GUAYMALLEN'
			,'ADOLFO CALLE'
			,'VILLANUEVA'
			,'MERCADO 2'
			,'LAS HERAS'
        )
		and (emp.CODPRODUCTO = 'NS' OR emp.CODPRODUCTO = 'NU')
        AND (EmP.[CODPROMO] = '30'
            OR P.[DESCRIPCION] like '%PRUEBA%')
		GROUP BY emp.UEN,EMP.FECHASQL,EMP.CODPRODUCTO

  '''      ,db_conex)
df_naftadapsaDesc = df_naftadapsaDesc.convert_dtypes()
### CONCATENO TABLA DE VOLUMEN TOTAL VENDIDO CON LA TABLA DE LOS DESCUENTOS
df_naftasdapsa = df_naftasdapsa.merge(df_naftadapsaDesc, on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')
df_naftasdapsa =df_naftasdapsa.fillna(0)





df_naftasdapsa['Total Vendido $'] = ((df_naftasdapsa['Ventas Efectivo']*df_naftasdapsa['Precio Cartel'])
                                       +(df_naftasdapsa['Venta Cta Cte']*df_naftasdapsa['Precio Cta Cte'])
                                      +((df_naftasdapsa['ventas Promos']+df_naftasdapsa['Descuentos'])*df_naftasdapsa['Precio Cartel']))

df_naftasdapsa['Volumen Total Vendido']=(df_naftasdapsa['Ventas Efectivo']+df_naftasdapsa['Venta Cta Cte']
                                           +(df_naftasdapsa['ventas Promos']+df_naftasdapsa['Descuentos']))



df_naftasdapsa = df_naftasdapsa.reindex(columns=['UEN','FECHASQL','CODPRODUCTO','Volumen Total Vendido','Total Vendido $','Total Vendido YER $','Precio Cartel'])
df_naftasdapsa = df_naftasdapsa.fillna(0)
df_naftasdapsa= df_naftasdapsa.loc[df_naftasdapsa["FECHASQL"] <= ayer.strftime('%Y-%m-%d'),:]

df_naftasdapsa=df_naftasdapsa.sort_values(['UEN','CODPRODUCTO','FECHASQL'])

costoDapsaNS= costoDapsaNS.loc[(costoDapsaNS["FECHASQL"] <= ayer.strftime('%Y-%m-%d')) & (costoDapsaNS["FECHASQL"] >= primer_dia_mes.strftime('%Y-%m-%d')),:]

costoDapsaNU= costoDapsaNU.loc[(costoDapsaNU["FECHASQL"] <= ayer.strftime('%Y-%m-%d')) & (costoDapsaNU["FECHASQL"] >= primer_dia_mes.strftime('%Y-%m-%d')),:]
## FUNCION PARA APLICAR COMISION NS

def aplicar_comision_acumuladaDAPSA(df):
    df=df.sort_values(['UEN','FECHASQL'])
    df = df.merge(costoDapsaNS,on=['FECHASQL','CODPRODUCTO'],how='outer')

    df['Comision']= (df['Precio Cartel']-df['COSTO NS DAP'])/dolar_blue
    df['Comision USD']= df['Comision']*df['Volumen Total Vendido']
    df['Total Vendido USD'] = df['Volumen Total Vendido']*df['Precio Cartel']/dolar_blue
    df = df.reindex(columns=['UEN','CODPRODUCTO','Volumen Total Vendido','Comision USD','Total Vendido USD'])
    df = df.groupby(
        ["UEN",'CODPRODUCTO']
        , as_index=False
    ).sum()
    return df

### Aplico la comision a cada estacion para el producto NS
las_heras= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'LAS HERAS           ') & (df_naftasdapsa["CODPRODUCTO"] == 'NS   '),:]
las_heras = aplicar_comision_acumuladaDAPSA(las_heras)
mercado1= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'MERC GUAYMALLEN     ') & (df_naftasdapsa["CODPRODUCTO"] == 'NS   '),:]
mercado1 = aplicar_comision_acumuladaDAPSA(mercado1)
mercado2= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'MERCADO 2           ') & (df_naftasdapsa["CODPRODUCTO"] == 'NS   '),:]
mercado2 = aplicar_comision_acumuladaDAPSA(mercado2)
sarmiento= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'SARMIENTO           ') & (df_naftasdapsa["CODPRODUCTO"] == 'NS   '),:]
sarmiento = aplicar_comision_acumuladaDAPSA(sarmiento)
villa_nueva= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'VILLANUEVA          ') & (df_naftasdapsa["CODPRODUCTO"] == 'NS   '),:]
villa_nueva = aplicar_comision_acumuladaDAPSA(villa_nueva)
adolfo_Calle= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'ADOLFO CALLE        ') & (df_naftasdapsa["CODPRODUCTO"] == 'NS   '),:]
adolfo_Calle = aplicar_comision_acumuladaDAPSA(adolfo_Calle)
mitre= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'MITRE               ') & (df_naftasdapsa["CODPRODUCTO"] == 'NS   '),:]
mitre = aplicar_comision_acumuladaDAPSA(mitre)
urquiza= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'URQUIZA             ') & (df_naftasdapsa["CODPRODUCTO"] == 'NS   '),:]
urquiza = aplicar_comision_acumuladaDAPSA(urquiza)

### Creo tabla de YPF NS
mbcNSDAPSA=las_heras.merge(mercado1,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Total Vendido USD','Comision USD'],how='outer')
mbcNSDAPSA=mbcNSDAPSA.merge(mercado2,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Total Vendido USD','Comision USD'],how='outer')
mbcNSDAPSA=mbcNSDAPSA.merge(sarmiento,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Total Vendido USD','Comision USD'],how='outer')
mbcNSDAPSA=mbcNSDAPSA.merge(villa_nueva,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Total Vendido USD','Comision USD'],how='outer')
mbcNSDAPSA=mbcNSDAPSA.merge(adolfo_Calle,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Total Vendido USD','Comision USD'],how='outer')
mbcNSDAPSA=mbcNSDAPSA.merge(mitre,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Total Vendido USD','Comision USD'],how='outer')
mbcNSDAPSA=mbcNSDAPSA.merge(urquiza,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Total Vendido USD','Comision USD'],how='outer')

### Concateno Tablas de Dapsa NS y EU


### Concateno tablas de YPF y Dapsa TOTALES
### Concateno tablas de YPF y Dapsa TOTALES
mbcTOTAL = mbcYPF.merge(mbcNSDAPSA,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Total Vendido USD','Comision USD'],how='outer')


## Creo Dataframe de NS
ns = mbcTOTAL['CODPRODUCTO'] == "NS   "
mbcTOTALNS = mbcTOTAL[ns]
mbcTOTALNS = mbcTOTALNS.reindex(columns=['UEN','Volumen Total Vendido','Volumen YER','Total Vendido USD','Comision USD'])
## Creo Dataframe de EU
nu = mbcTOTAL['CODPRODUCTO'] == "NU   "
mbcTotalNU = mbcTOTAL[nu]
mbcTotalNU=mbcTotalNU.reindex(columns=['UEN','Volumen Total Vendido','Volumen YER','Total Vendido USD','Comision USD'])



#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)




