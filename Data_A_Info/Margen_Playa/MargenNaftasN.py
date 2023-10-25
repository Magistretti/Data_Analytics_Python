import os
import math
import re
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

hoy = datetime.datetime.now()
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



### TRAMOS Y COSTOS 
sheet_id2='1yJlZkGWDcYa5hdlXZxY5_xbi4s3Y0AqoS-L2QgDdFTQ'

######### LECTURA DE EXCEL DE COSTOS DAPSA EU
sheet_name3= 'CostoDapsaNS'
gsheet_url_costoNS = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id2, sheet_name3)
costoDapsaNS =pd.read_csv(gsheet_url_costoNS)
costoDapsaNS['FECHASQL'] = pd.to_datetime(costoDapsaNS['FECHASQL'], format='%d/%m/%Y')


######### LECTURA DE EXCEL DE COSTOS DAPSA EU
sheet_name4= 'CostoDapsaNU'
gsheet_url_costoNU = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id2, sheet_name4)
costoDapsaNU =pd.read_csv(gsheet_url_costoNU)
costoDapsaNU = costoDapsaNU.convert_dtypes()
costoDapsaNU['FECHASQL'] = pd.to_datetime(costoDapsaNU['FECHASQL'], format='%d/%m/%Y')


######### LECTURA DE EXCEL DE COMISIONES Y TRAMOS NS
sheet_name1= 'ComisionyTramoNS'
gsheet_url_comisionNS = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id2, sheet_name1)
comisionNS =pd.read_csv(gsheet_url_comisionNS)
comisionNS = comisionNS.convert_dtypes()
comisionNS['Fecha'] = pd.to_datetime(comisionNS['Fecha'], format='%d/%m/%Y')
comisionNS= comisionNS.loc[comisionNS["Fecha"] == primer_dia_mes.strftime('%Y-%m-%d'),:]


######### LECTURA DE EXCEL DE COMISIONES Y TRAMOS EU
sheet_name2= 'ComisionyTramoNU'
gsheet_url_comisionNU = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id2, sheet_name2)
comisionNU =pd.read_csv(gsheet_url_comisionNU)
comisionNU = comisionNU.convert_dtypes()
comisionNU['Fecha'] = pd.to_datetime(comisionNU['Fecha'], format='%d/%m/%Y')
comisionNU= comisionNU.loc[comisionNU["Fecha"] == primer_dia_mes.strftime('%Y-%m-%d'),:]

df_presupuesto = df_presupuesto.reindex(columns=['UEN','FECHASQL','CODPRODUCTO','VENTAS'])


##########################################
# TRABAJO CON TABLA DE EXCEL    ################
##########################################

df_presupuestoNAFTA=df_presupuesto.loc[(df_presupuesto['CODPRODUCTO']=='NS') | (df_presupuesto['CODPRODUCTO']=='NU')]
egnctotales =df_presupuesto

egncAC=df_presupuestoNAFTA.loc[df_presupuesto['UEN']=='ADOLFO CALLE']
egncAC = egncAC.convert_dtypes()

egncU=df_presupuestoNAFTA.loc[df_presupuesto['UEN']=='URQUIZA']
egncU = egncU.convert_dtypes()

egncVN=df_presupuestoNAFTA.loc[df_presupuesto['UEN']=='VILLANUEVA']
egncVN = egncVN.convert_dtypes()

egncLH=df_presupuestoNAFTA.loc[df_presupuesto['UEN']=='LAS HERAS']
egncLH = egncLH.convert_dtypes()

egncM=df_presupuestoNAFTA.loc[df_presupuesto['UEN']=='MITRE']
egncM = egncM.convert_dtypes()

egncS=df_presupuestoNAFTA.loc[df_presupuesto['UEN']=='SARMIENTO']
egncS = egncS.convert_dtypes()

egncM1=df_presupuestoNAFTA.loc[df_presupuesto['UEN']=='MERC GUAYMALLEN']
egncM1 = egncM1.convert_dtypes()

egncM2=df_presupuestoNAFTA.loc[df_presupuesto['UEN']=='MERCADO 2']
egncM2 = egncM2.convert_dtypes()

egncP1=df_presupuestoNAFTA.loc[df_presupuesto['UEN']=='PERDRIEL']
egncP1 = egncP1.convert_dtypes()

egncSJ=df_presupuestoNAFTA.loc[df_presupuesto['UEN']=='SAN JOSE']
egncSJ = egncSJ.convert_dtypes()

egncL=df_presupuestoNAFTA.loc[df_presupuesto['UEN']=='LAMADRID']
egncL = egncL.convert_dtypes()

egncPO=df_presupuestoNAFTA.loc[df_presupuesto['UEN']=='PUENTE OLIVE']
egncPO = egncPO.convert_dtypes()

egncP2=df_presupuestoNAFTA.loc[df_presupuesto['UEN']=='PERDRIEL2']
egncP2 = egncP2.convert_dtypes()

egncA=df_presupuestoNAFTA.loc[df_presupuesto['UEN']=='AZCUENAGA']
egncA = egncA.convert_dtypes()


egncTotal = pd.concat([egncAC,egncA,egncP2,egncPO,egncL,egncSJ,egncP1,egncM2,egncM1,egncM,egncS,egncLH,egncVN,egncU])
egncTotal = egncTotal.rename({'Fecha':'FECHASQL','VENTAS':'Presupuesto Diario'},axis=1)
egncTotal = egncTotal.reindex(columns=['UEN','FECHASQL','CODPRODUCTO','Presupuesto Diario'])

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
        AND FECHASQL < @hoy
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
	SELECT emp.UEN,emp.FECHASQL,sum(emp.VOLUMEN) as 'Volumen YER',emp.CODPRODUCTO
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL >= @inicioMesActual
        AND FECHASQL < @hoy
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
	SELECT emp.UEN,emp.FECHASQL,sum(emp.VOLUMEN) as 'Volumen REDMAS',emp.CODPRODUCTO,MAX(emp.PRECIO) AS 'Precio Promos'
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL >= @inicioMesActual
        AND FECHASQL < @hoy
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
df_naftasTotal['Total Vendido $'] = ((df_naftasTotal['Ventas Efectivo']*df_naftasTotal['Precio Cartel'])
                                       +(df_naftasTotal['Venta Cta Cte']*df_naftasTotal['Precio Cta Cte'])
                                      +(df_naftasTotal['Volumen REDMAS']*df_naftasTotal['Precio Cartel']))
df_naftasTotal['Volumen Total Vendido']=(df_naftasTotal['Ventas Efectivo']+df_naftasTotal['Venta Cta Cte']
                                           +df_naftasTotal['Volumen REDMAS'])

df_naftasTotal['Total Vendido YER $']=df_naftasTotal['Volumen YER']*df_naftasTotal['Precio Cartel']

presupuestoYPF = egncTotal.loc[(egncTotal['UEN'] == 'LAMADRID') | (egncTotal['UEN'] == 'AZCUENAGA') | (egncTotal['UEN'] == 'PERDRIEL')
                               | (egncTotal['UEN'] == 'PERDRIEL2') | (egncTotal['UEN'] == 'SAN JOSE') | (egncTotal['UEN'] == 'PUENTE OLIVE'),:]

df_naftasTotal['UEN']=df_naftasTotal['UEN'].str.strip()
df_naftasTotal['CODPRODUCTO']=df_naftasTotal['CODPRODUCTO'].str.strip()

df_naftasTotal = df_naftasTotal.merge(presupuestoYPF,on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')

df_naftasTotal=df_naftasTotal.sort_values(['CODPRODUCTO','UEN','FECHASQL'])
# convertir la columna 'Precio Cartel' a un tipo de datos numérico
df_naftasTotal['Precio Cartel'] = pd.to_numeric(df_naftasTotal['Precio Cartel'], errors='coerce')

# reemplazar los valores 0 en la columna 'Precio Cartel' con valores nulos (NaN)
df_naftasTotal['Precio Cartel'] = df_naftasTotal['Precio Cartel'].replace(0, np.nan)
# llenar los valores nulos utilizando la función fillna de Pandas con el argumento method='ffill'
df_naftasTotal['Precio Cartel'] = df_naftasTotal['Precio Cartel'].fillna(method='ffill')
df_naftasTotal['Precio Cartel'] = df_naftasTotal['Precio Cartel'].fillna(method='bfill')

df_naftasTotal['Presupuesto Diario $']=df_naftasTotal['Presupuesto Diario']*df_naftasTotal['Precio Cartel']

df_naftasTotal = df_naftasTotal.reindex(columns=['UEN','FECHASQL','CODPRODUCTO','Volumen Total Vendido','Total Vendido $','Total Vendido YER $','Presupuesto Diario','Presupuesto Diario $','Precio Cartel'])
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
    t1=comi.loc[0,'COMISION TRAMO I NS']
    df['Presu Acumu']=df['Volumen Total Vendido'].cumsum()
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
        if df.loc[i,'Presu Acumu'] <= comi.loc[0,'TRAMO I NS']:
            df.loc[i,'Marcador presu'] = 1
        elif df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO II NS']:
            df.loc[i,'Marcador presu'] = 2
        elif df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO III NS']:
                df.loc[i,'Marcador presu'] = 3
        else:
            if df.loc[i,'Ventas Acumuladas'] > comi.loc[0,'TRAMO III NS']:
                df.loc[i,'Marcador presu'] = 4

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

    for i in df.index:
        if df.loc[i,'Marcador presu'] == 1:
            df.loc[i,'COMISION presu']=comi.loc[0,'COMISION TRAMO I NS']
        elif df.loc[i,'Marcador presu'] == 2:
            if df.loc[i,'Marcador presu'] != df.loc[i-1,'Marcador presu']:
                tsig = (df.loc[i,'Presu Acumu'] - comi.loc[0,'TRAMO I NS'])
                tant = (df.loc[i,'Presupuesto Diario $']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO I NS']*tant)+(comi.loc[0,'COMISION TRAMO II NS']*tsig)
                df.loc[i,'COMISION presu']= comis/df.loc[i,'Presupuesto Diario']
            else:
                df.loc[i,'COMISION presu']=comi.loc[0,'COMISION TRAMO II NS']
        elif df.loc[i,'Marcador presu'] == 3:
            if df.loc[i,'Marcador presu'] != df.loc[i-1,'Marcador presu']:
                tsig = (df.loc[i,'Ventas Acumuladas'] - comi.loc[0,'TRAMO II NS'])
                tant = (df.loc[i,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO II NS']*tant)+(comi.loc[0,'COMISION TRAMO III NS']*tsig)
                df.loc[i,'COMISION presu']= comis/df.loc[i,'Presupuesto Diario']
            else:
                df.loc[i,'COMISION presu']=comi.loc[0,'COMISION TRAMO III NS']
        elif df.loc[i,'Marcador presu'] == 4:
            if df.loc[i,'Marcador presu'] != df.loc[i-1,'Marcador presu']:
                tsig = (df.loc[i,'Ventas Acumuladas'] - comi.loc[0,'TRAMO III NS'])
                tant = (df.loc[i,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO III NS']*tant)+(comi.loc[0,'COMISION TRAMO IV NS']*tsig)
                df.loc[i,'COMISION presu']= comis/df.loc[i,'Presupuesto Diario']
            else:
                df.loc[i,'COMISION presu']=comi.loc[0,'COMISION TRAMO IV NS']        
    df['Comision $']= ((df['COMISION']*df['Total Vendido $'])+(df['Total Vendido YER $']*t1))*1.21/0.97
    df['Comision Presupuestada $']= df['COMISION presu']*df['Presupuesto Diario $']*1.21/0.97
    df['Ventas Acumuladas $'] = df['Total Vendido $']+df['Total Vendido YER $']
    df = df.reindex(columns=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'])
    df = df.groupby(
        ["UEN",'CODPRODUCTO']
        , as_index=False
    ).sum(numeric_only=True)
    return df


df_naftasTotal['UEN']=df_naftasTotal['UEN'].str.strip()
df_naftasTotal['CODPRODUCTO']=df_naftasTotal['CODPRODUCTO'].str.strip()

### Aplico la comision a cada estacion para el producto NS
perdriel1= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'PERDRIEL') & (df_naftasTotal["CODPRODUCTO"] == 'NS'),:]
perdriel1 = aplicar_comision_acumuladaNS(perdriel1,'PERDRIEL')
perdriel2= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'PERDRIEL2') & (df_naftasTotal["CODPRODUCTO"] == 'NS'),:]
perdriel2 = aplicar_comision_acumuladaNS(perdriel2,'PERDRIEL2')
azcuenaga= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'AZCUENAGA') & (df_naftasTotal["CODPRODUCTO"] == 'NS'),:]
azcuenaga = aplicar_comision_acumuladaNS(azcuenaga,'AZCUENAGA')
san_Jose= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'SAN JOSE') & (df_naftasTotal["CODPRODUCTO"] == 'NS'),:]
san_Jose = aplicar_comision_acumuladaNS(san_Jose,'SAN JOSE')
puente_Olive= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'PUENTE OLIVE') & (df_naftasTotal["CODPRODUCTO"] == 'NS'),:]
puente_Olive = aplicar_comision_acumuladaNS(puente_Olive,'PUENTE OLIVE')
lamadrid= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'LAMADRID') & (df_naftasTotal["CODPRODUCTO"] == 'NS'),:]
lamadrid = aplicar_comision_acumuladaNS(lamadrid,'LAMADRID')


### Creo tabla de YPF NS
mbcNSYPF=perdriel1.merge(perdriel2,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcNSYPF=mbcNSYPF.merge(azcuenaga,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcNSYPF=mbcNSYPF.merge(san_Jose,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcNSYPF=mbcNSYPF.merge(puente_Olive,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcNSYPF=mbcNSYPF.merge(lamadrid,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')

def aplicar_comision_acumuladaNU(df,estacion):
    df=df.sort_values(['UEN','FECHASQL'])
    comi = comisionNU.loc[(comisionNU["UEN"] == estacion),:]
    comi = comi.reset_index()
    df['Ventas Acumuladas']=df['Volumen Total Vendido'].cumsum()
    df['Presu Acumu']=df['Volumen Total Vendido'].cumsum()
    t1=comi.loc[0,'COMISION TRAMO I NU']
    for i in df.index:
        if df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO I NU']:
            df.loc[i,'Marcador'] = 1
        elif df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO II NU']:
            df.loc[i,'Marcador'] = 2
        elif df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO III NU']:
                df.loc[i,'Marcador'] = 3
        else:
            if df.loc[i,'Ventas Acumuladas'] > comi.loc[0,'TRAMO IV NU']:
                df.loc[i,'Marcador'] = 4        
    for q in df.index:
        if df.loc[q,'Presu Acumu'] <= comi.loc[0,'TRAMO I NU']:
            df.loc[q,'Marcador presu'] = 1
        elif df.loc[q,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO II NU']:
            df.loc[q,'Marcador presu'] = 2
        elif df.loc[q,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO III NU']:
                df.loc[q,'Marcador presu'] = 3
        else:
            if df.loc[q,'Ventas Acumuladas'] > comi.loc[0,'TRAMO IV NU']:
                df.loc[q,'Marcador presu'] = 4



    for r in df.index:
        if df.loc[r,'Marcador'] == 1:
            df.loc[r,'COMISION']=comi.loc[0,'COMISION TRAMO I NU']
        elif df.loc[r,'Marcador'] == 2:
            if df.loc[r,'Marcador'] != df.loc[r-1,'Marcador']:
                tsig = (df.loc[r,'Ventas Acumuladas'] - comi.loc[0,'TRAMO I NU'])
                tant = (df.loc[r,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO I NU']*tant)+(comi.loc[0,'COMISION TRAMO II NU']*tsig)
                df.loc[r,'COMISION']= comis/df.loc[r,'Volumen Total Vendido']
            else:
                df.loc[r,'COMISION']=comi.loc[0,'COMISION TRAMO II NU']
        elif df.loc[r,'Marcador'] == 3:
            if df.loc[r,'Marcador'] != df.loc[r-1,'Marcador']:
                tsig = (df.loc[r,'Ventas Acumuladas'] - comi.loc[0,'TRAMO II NU'])
                tant = (df.loc[r,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO II NU']*tant)+(comi.loc[0,'COMISION TRAMO III NU']*tsig)
                df.loc[r,'COMISION']= comis/df.loc[r,'Volumen Total Vendido']
            else:
                df.loc[r,'COMISION']=comi.loc[0,'COMISION TRAMO III NU']
        elif df.loc[r,'Marcador'] == 4:
            if df.loc[r,'Marcador'] != df.loc[r-1,'Marcador']:
                tsig = (df.loc[r,'Ventas Acumuladas'] - comi.loc[0,'TRAMO III NU'])
                tant = (df.loc[r,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO III NU']*tant)+(comi.loc[0,'COMISION TRAMO IV NU']*tsig)
                df.loc[r,'COMISION']= comis/df.loc[r,'Volumen Total Vendido']
            else:
                df.loc[r,'COMISION']=comi.loc[0,'COMISION TRAMO IV NU']
                
                

    for h in df.index:
        if df.loc[h,'Marcador presu'] == 1:
            df.loc[h,'COMISION presu']=comi.loc[0,'COMISION TRAMO I NU']
        elif df.loc[h,'Marcador presu'] == 2:
            if df.loc[h,'Marcador presu'] != df.loc[h-1,'Marcador presu']:
                tsig = (df.loc[h,'Presu Acumu'] - comi.loc[0,'TRAMO I NU'])
                tant = (df.loc[h,'Presupuesto Diario']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO I NU']*tant)+(comi.loc[0,'COMISION TRAMO II NU']*tsig)
                df.loc[h,'COMISION presu']= comis/df.loc[h,'Presupuesto Diario']
            else:
                df.loc[h,'COMISION presu']=comi.loc[0,'COMISION TRAMO II NU']
        elif df.loc[h,'Marcador presu'] == 3:
            if df.loc[h,'Marcador presu'] != df.loc[h-1,'Marcador presu']:
                tsig = (df.loc[h,'Presu Acumu'] - comi.loc[0,'TRAMO II NU'])
                tant = (df.loc[h,'Presupuesto Diario']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO II NU']*tant)+(comi.loc[0,'COMISION TRAMO III NU']*tsig)
                df.loc[h,'COMISION presu']= comis/df.loc[h,'Presupuesto Diario']
            else:
                df.loc[h,'COMISION presu']=comi.loc[0,'COMISION TRAMO III NU']
        elif df.loc[h,'Marcador presu'] == 4:
            if df.loc[h,'Marcador presu'] != df.loc[h-1,'Marcador presu']:
                tsig = (df.loc[h,'Presu Acumu'] - comi.loc[0,'TRAMO III NU'])
                tant = (df.loc[h,'Presupuesto Diario']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO III NU']*tant)+(comi.loc[0,'COMISION TRAMO IV NU']*tsig)
                df.loc[h,'COMISION presu']= comis/df.loc[h,'Presupuesto Diario']
            else:
                df.loc[h,'COMISION presu']=comi.loc[0,'COMISION TRAMO IV NU']        
                
    df['Comision $']= ((df['COMISION']*df['Total Vendido $'])+(df['Total Vendido YER $']*t1))*1.21/0.97
    df['Comision Presupuestada $']= (df['COMISION presu']*df['Presupuesto Diario $'])*1.21/0.97
    df['Ventas Acumuladas $'] = df['Total Vendido $']+df['Total Vendido YER $']
    df = df.reindex(columns=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'])
    df = df.groupby(
        ["UEN",'CODPRODUCTO']
        , as_index=False
    ).sum(numeric_only=True)
    return df



### Aplico la comision a cada estacion para el producto EU
perdriel1= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'PERDRIEL') & (df_naftasTotal["CODPRODUCTO"] == 'NU'),:]
perdriel1 = aplicar_comision_acumuladaNU(perdriel1,'PERDRIEL')
perdriel2= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'PERDRIEL2') & (df_naftasTotal["CODPRODUCTO"] == 'NU'),:]
perdriel2 = aplicar_comision_acumuladaNU(perdriel2,'PERDRIEL2')
azcuenaga= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'AZCUENAGA') & (df_naftasTotal["CODPRODUCTO"] == 'NU'),:]
azcuenaga = aplicar_comision_acumuladaNU(azcuenaga,'AZCUENAGA')
san_Jose= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'SAN JOSE') & (df_naftasTotal["CODPRODUCTO"] == 'NU'),:]
san_Jose = aplicar_comision_acumuladaNU(san_Jose,'SAN JOSE')
puente_Olive= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'PUENTE OLIVE') & (df_naftasTotal["CODPRODUCTO"] == 'NU'),:]
puente_Olive = aplicar_comision_acumuladaNU(puente_Olive,'PUENTE OLIVE')
lamadrid= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'LAMADRID') & (df_naftasTotal["CODPRODUCTO"] == 'NU'),:]
lamadrid = aplicar_comision_acumuladaNU(lamadrid,'LAMADRID')
### Creo tabla de YPF EU
mbcNUYPF=perdriel1.merge(perdriel2,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcNUYPF=mbcNUYPF.merge(azcuenaga,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcNUYPF=mbcNUYPF.merge(san_Jose,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcNUYPF=mbcNUYPF.merge(puente_Olive,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcNUYPF=mbcNUYPF.merge(lamadrid,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')

### Concateno Tabla De YPF NS con YPF EU
mbcYPF=mbcNSYPF.merge(mbcNUYPF,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')


##################################################
####### VENTAS DAPSA #############################
##################################################

# Obtener la fecha actual
#fecha_actual = datetime.datetime.now()
# Crear una nueva fecha con el primer día del mes actual
#primer_dia_mes = datetime.datetime(fecha_actual.year, fecha_actual.month, 1)

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
        AND FECHASQL < @hoy
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
'''
     ,db_conex)
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
	SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 1, CURRENT_TIMESTAMP), 0)
	SELECT emp.UEN,EmP.FECHASQL
        ,EmP.[CODPRODUCTO]
        ,SUM(-EmP.[VOLUMEN]) as 'Descuentos'
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL >= @inicioMesActual
        AND FECHASQL < @hoy
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


presupuestoDAPSA = egncTotal.loc[(egncTotal['UEN'] != 'LAMADRID') | (egncTotal['UEN'] != 'AZCUENAGA') | (egncTotal['UEN'] != 'PERDRIEL')
                               | (egncTotal['UEN'] != 'PERDRIEL2') | (egncTotal['UEN'] != 'SAN JOSE') | (egncTotal['UEN'] != 'PUENTE OLIVE'),:]

df_naftasdapsa['UEN']=df_naftasdapsa['UEN'].str.strip()
df_naftasdapsa['CODPRODUCTO']=df_naftasdapsa['CODPRODUCTO'].str.strip()
df_naftasdapsa = df_naftasdapsa.merge(presupuestoDAPSA,on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')

df_naftasdapsa=df_naftasdapsa.sort_values(['CODPRODUCTO','UEN','FECHASQL'])
# reemplazar los valores 0 en la columna 'Precio Cartel' con valores nulos (NaN)
df_naftasdapsa['Precio Cartel'] = df_naftasdapsa['Precio Cartel'].replace(0, np.nan)
# llenar los valores nulos utilizando la función fillna de Pandas con el argumento method='ffill'
df_naftasdapsa['Precio Cartel'] = df_naftasdapsa['Precio Cartel'].fillna(method='ffill')
df_naftasdapsa['Precio Cartel'] = df_naftasdapsa['Precio Cartel'].fillna(method='bfill')

df_naftasdapsa['Presupuesto Diario $']=df_naftasdapsa['Presupuesto Diario']*df_naftasdapsa['Precio Cartel']

df_naftasdapsa = df_naftasdapsa.reindex(columns=['UEN','FECHASQL','CODPRODUCTO','Volumen Total Vendido','Total Vendido $','Total Vendido YER $','Presupuesto Diario','Presupuesto Diario $','Precio Cartel'])
df_naftasdapsa = df_naftasdapsa.fillna(0)
df_naftasdapsa= df_naftasdapsa.loc[df_naftasdapsa["FECHASQL"] <= ayer.strftime('%Y-%m-%d'),:]

df_naftasdapsa=df_naftasdapsa.sort_values(['UEN','CODPRODUCTO','FECHASQL'])

costoDapsaNS= costoDapsaNS.loc[(costoDapsaNS["FECHASQL"] <= ayer.strftime('%Y-%m-%d')) & (costoDapsaNS["FECHASQL"] >= primer_dia_mes.strftime('%Y-%m-%d')),:]

costoDapsaNU= costoDapsaNU.loc[(costoDapsaNU["FECHASQL"] <= ayer.strftime('%Y-%m-%d')) & (costoDapsaNU["FECHASQL"] >= primer_dia_mes.strftime('%Y-%m-%d')),:]
## FUNCION PARA APLICAR COMISION NS

def aplicar_comision_acumuladaDAPSA(df):
    df=df.sort_values(['UEN','FECHASQL'])
    df = df.merge(costoDapsaNS,on=['FECHASQL','CODPRODUCTO'],how='outer')

    
    # convertir la columna 'Precio Cartel' a un tipo de datos numérico
    df['Precio Cartel'] = pd.to_numeric(df['Precio Cartel'], errors='coerce')

    # reemplazar los valores 0 en la columna 'Precio Cartel' con valores nulos (NaN)
    df['Precio Cartel'] = df['Precio Cartel'].replace(0, np.nan)
    # llenar los valores nulos utilizando la función fillna de Pandas con el argumento method='ffill'
    df['Precio Cartel'] = df['Precio Cartel'].fillna(method='bfill')
    
    df['Comision']= df['Precio Cartel']-df['COSTO NS DAP']
    df['Comision $']= df['Comision']*df['Volumen Total Vendido']
    df['Comision Presupuestada $']= df['Comision']*df['Presupuesto Diario']
    df['Ventas Acumuladas $'] = df['Volumen Total Vendido']*df['Precio Cartel']
    df = df.reindex(columns=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'])
    df = df.groupby(
        ["UEN",'CODPRODUCTO']
        , as_index=False
    ).sum(numeric_only=True)
    return df

df_naftasdapsa['UEN']=df_naftasdapsa['UEN'].str.strip()
df_naftasdapsa['CODPRODUCTO']=df_naftasdapsa['CODPRODUCTO'].str.strip()

### Aplico la comision a cada estacion para el producto NS
las_heras= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'LAS HERAS') & (df_naftasdapsa["CODPRODUCTO"] == 'NS'),:]
las_heras = aplicar_comision_acumuladaDAPSA(las_heras)
mercado1= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'MERC GUAYMALLEN') & (df_naftasdapsa["CODPRODUCTO"] == 'NS'),:]
mercado1 = aplicar_comision_acumuladaDAPSA(mercado1)
mercado2= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'MERCADO 2') & (df_naftasdapsa["CODPRODUCTO"] == 'NS'),:]
mercado2 = aplicar_comision_acumuladaDAPSA(mercado2)
sarmiento= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'SARMIENTO') & (df_naftasdapsa["CODPRODUCTO"] == 'NS'),:]
sarmiento = aplicar_comision_acumuladaDAPSA(sarmiento)
villa_nueva= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'VILLANUEVA') & (df_naftasdapsa["CODPRODUCTO"] == 'NS'),:]
villa_nueva = aplicar_comision_acumuladaDAPSA(villa_nueva)
adolfo_Calle= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'ADOLFO CALLE') & (df_naftasdapsa["CODPRODUCTO"] == 'NS'),:]
adolfo_Calle = aplicar_comision_acumuladaDAPSA(adolfo_Calle)
mitre= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'MITRE') & (df_naftasdapsa["CODPRODUCTO"] == 'NS'),:]
mitre = aplicar_comision_acumuladaDAPSA(mitre)
urquiza= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'URQUIZA') & (df_naftasdapsa["CODPRODUCTO"] == 'NS'),:]
urquiza = aplicar_comision_acumuladaDAPSA(urquiza)

### Creo tabla de YPF NS
mbcNSDAPSA=las_heras.merge(mercado1,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcNSDAPSA=mbcNSDAPSA.merge(mercado2,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcNSDAPSA=mbcNSDAPSA.merge(sarmiento,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcNSDAPSA=mbcNSDAPSA.merge(villa_nueva,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcNSDAPSA=mbcNSDAPSA.merge(adolfo_Calle,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcNSDAPSA=mbcNSDAPSA.merge(mitre,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcNSDAPSA=mbcNSDAPSA.merge(urquiza,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')

### Concateno Tablas de Dapsa NS y EU


### Concateno tablas de YPF y Dapsa TOTALES
### Concateno tablas de YPF y Dapsa TOTALES
mbcTOTAL = mbcYPF.merge(mbcNSDAPSA,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')





## Creo Dataframe de NS
ns = mbcTOTAL['CODPRODUCTO'] == "NS"
mbcTOTALNS = mbcTOTAL[ns]
mbcTOTALNS = mbcTOTALNS.rename({'Presupuesto Diario $':'Presupuesto Acumulado $','Comision Presupuestada $':'MBC Presupuestado $','Comision $':'MBC Acumulado $'},axis=1)
mbcTOTALNS = mbcTOTALNS.reindex(columns=['UEN','Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $'])
## Creo Dataframe de EU
nu = mbcTOTAL['CODPRODUCTO'] == "NU"
mbcTotalNU = mbcTOTAL[nu]
mbcTotalNU = mbcTotalNU.rename({'Presupuesto Diario $':'Presupuesto Acumulado $','Comision Presupuestada $':'MBC Presupuestado $','Comision $':'MBC Acumulado $'},axis=1)
mbcTotalNU=mbcTotalNU.reindex(columns=['UEN','Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $'])

###### Columnas de Desvio y Totales NS
mbcTOTALNS['Desvio Presupuestado %']=(mbcTOTALNS['Ventas Acumuladas $']/mbcTOTALNS['Presupuesto Acumulado $'])-1
mbcTOTALNS['Desvio MBC %']=(mbcTOTALNS['MBC Acumulado $']/mbcTOTALNS['MBC Presupuestado $'])-1

mbcTOTALNS.loc["colTOTAL"]= pd.Series(
    mbcTOTALNS.sum(numeric_only=True)
    , index=['Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $']
)
mbcTOTALNS.fillna({"UEN":"TOTAL"}, inplace=True)

tasa = (mbcTOTALNS.loc["colTOTAL",'Ventas Acumuladas $'] /
    mbcTOTALNS.loc["colTOTAL",'Presupuesto Acumulado $'])-1
mbcTOTALNS.fillna({"Desvio Presupuestado %":tasa}, inplace=True)

tasa2 = (mbcTOTALNS.loc["colTOTAL",'MBC Acumulado $'] /
    mbcTOTALNS.loc["colTOTAL",'MBC Presupuestado $'])-1
mbcTOTALNS.fillna({'Desvio MBC %':tasa2}, inplace=True)
mbcTOTALNS=mbcTOTALNS.reindex(columns=['UEN','Presupuesto Acumulado $','Ventas Acumuladas $','Desvio Presupuestado %','MBC Presupuestado $','MBC Acumulado $','Desvio MBC %'])

###### Columnas de Desvio y Totales EU
mbcTotalNU['Desvio Presupuestado %']=(mbcTotalNU['Ventas Acumuladas $']/mbcTotalNU['Presupuesto Acumulado $'])-1
mbcTotalNU['Desvio MBC %']=(mbcTotalNU['MBC Acumulado $']/mbcTotalNU['MBC Presupuestado $'])-1

mbcTotalNU.loc["colTOTAL"]= pd.Series(
    mbcTotalNU.sum(numeric_only=True)
    , index=['Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $']
)
mbcTotalNU.fillna({"UEN":"TOTAL"}, inplace=True)

tasa = (mbcTotalNU.loc["colTOTAL",'Ventas Acumuladas $'] /
    mbcTotalNU.loc["colTOTAL",'Presupuesto Acumulado $'])-1
mbcTotalNU.fillna({"Desvio Presupuestado %":tasa}, inplace=True)

tasa2 = (mbcTotalNU.loc["colTOTAL",'MBC Acumulado $'] /
    mbcTotalNU.loc["colTOTAL",'MBC Presupuestado $'])-1
mbcTotalNU.fillna({'Desvio MBC %':tasa2}, inplace=True)
mbcTotalNU=mbcTotalNU.reindex(columns=['UEN','Presupuesto Acumulado $','Ventas Acumuladas $','Desvio Presupuestado %','MBC Presupuestado $','MBC Acumulado $','Desvio MBC %'])

mbcNaftasNS=mbcTOTALNS

mbcNaftasNU=mbcTotalNU


######### LE DOY FORMATO AL DATAFRAME
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
        .format("$ {0:,.0f}", subset=list_Col_Num) \
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
mbcTOTALNS = _estiladorVtaTituloD(mbcTOTALNS,numCols,percColsPen, "MBC Nafta Super")
mbcTotalNU = _estiladorVtaTituloD(mbcTotalNU,numCols,percColsPen, "MBC Infinia Nafta")


#### DEFINO EL DESTINO DONDE SE GUARDARA LA IMAGEN Y EL NOMBRE
ubicacion = str(pathlib.Path(__file__).parent)+"\\"
nombrePen = "MBCNaftasNS.png"
nombrePenDiario = "MBCNaftasNU.png"
### IMPRIMO LA IMAGEN
def df_to_image(df, ubicacion, nombre):
    """
    Esta función usa las biblioteca "dataframe_Image as dfi" y "os" para 
    generar un archivo .png de un dataframe. Si el archivo ya existe, este será
    reemplazado por el nuevo archivo.

    Args:
        df: dataframe a convertir
         ubicacion: ubicacion local donde se quiere grabar el archivo
          nombre: nombre del archivo incluyendo extegoión .png (ej: "hello.png")

    """
    if os.path.exists(ubicacion+nombre):
        os.remove(ubicacion+nombre)
        dfi.export(df, ubicacion+nombre,max_rows=-1)
    else:
        dfi.export(df, ubicacion+nombre,max_rows=-1)

df_to_image(mbcTOTALNS, ubicacion, nombrePen)
df_to_image(mbcTotalNU,ubicacion,nombrePenDiario)


#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)



