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




###COSTO
sheet_id2='1yJlZkGWDcYa5hdlXZxY5_xbi4s3Y0AqoS-L2QgDdFTQ'
hoja2='CostoDapsaGO'
gsheet_url_costos = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id2, hoja2)
costoDapsaGO=pd.read_csv(gsheet_url_costos)
costoDapsaGO = costoDapsaGO.convert_dtypes()
costoDapsaGO['FECHASQL'] = pd.to_datetime(costoDapsaGO['FECHASQL'], format='%d/%m/%Y')


hoja3='CostoDapsaEU'
gsheet_url_costos = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id2, hoja3)
costoDapsaEU=pd.read_csv(gsheet_url_costos)
costoDapsaEU = costoDapsaEU.convert_dtypes()
costoDapsaEU['FECHASQL'] = pd.to_datetime(costoDapsaEU['FECHASQL'], format='%d/%m/%Y')

###COMISION
hoja4='ComisionyTramoGO'
gsheet_url_costos = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id2, hoja4)
comisionGO=pd.read_csv(gsheet_url_costos)
comisionGO = comisionGO.convert_dtypes()
comisionGO['Fecha'] = pd.to_datetime(comisionGO['Fecha'], format='%d/%m/%Y')
comisionGO= comisionGO.loc[comisionGO["Fecha"] == primer_dia_mes.strftime('%Y-%m-%d'),:]



hoja5='ComisionyTramoEU'
gsheet_url_costos = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id2, hoja5)
comisionEU=pd.read_csv(gsheet_url_costos)
comisionEU = comisionEU.convert_dtypes()
comisionEU['Fecha'] = pd.to_datetime(comisionEU['Fecha'], format='%d/%m/%Y')
comisionEU= comisionEU.loc[comisionEU["Fecha"] == primer_dia_mes.strftime('%Y-%m-%d'),:]




df_presupuesto_gasoleos=df_presupuesto.loc[(df_presupuesto['CODPRODUCTO']=='GO') | (df_presupuesto['CODPRODUCTO']=='EU')]

egnctotales=df_presupuesto_gasoleos

egncAC=df_presupuesto_gasoleos.loc[df_presupuesto['UEN']=='ADOLFO CALLE']
egncAC = egncAC.convert_dtypes()

egncU=df_presupuesto_gasoleos.loc[df_presupuesto['UEN']=='URQUIZA']
egncU = egncU.convert_dtypes()

egncVN=df_presupuesto_gasoleos.loc[df_presupuesto['UEN']=='VILLANUEVA']
egncVN = egncVN.convert_dtypes()

egncLH=df_presupuesto_gasoleos.loc[df_presupuesto['UEN']=='LAS HERAS']
egncLH = egncLH.convert_dtypes()

egncM=df_presupuesto_gasoleos.loc[df_presupuesto['UEN']=='MITRE']
egncM = egncM.convert_dtypes()

egncS=df_presupuesto_gasoleos.loc[df_presupuesto['UEN']=='SARMIENTO']
egncS = egncS.convert_dtypes()

egncM1=df_presupuesto_gasoleos.loc[df_presupuesto['UEN']=='MERC GUAYMALLEN']
egncM1 = egncM1.convert_dtypes()

egncM2=df_presupuesto_gasoleos.loc[df_presupuesto['UEN']=='MERCADO 2']
egncM2 = egncM2.convert_dtypes()

egncP1=df_presupuesto_gasoleos.loc[df_presupuesto['UEN']=='PERDRIEL']
egncP1 = egncP1.convert_dtypes()

egncSJ=df_presupuesto_gasoleos.loc[df_presupuesto['UEN']=='SAN JOSE']
egncSJ = egncSJ.convert_dtypes()

egncL=df_presupuesto_gasoleos.loc[df_presupuesto['UEN']=='LAMADRID']
egncL = egncL.convert_dtypes()

egncPO=df_presupuesto_gasoleos.loc[df_presupuesto['UEN']=='PUENTE OLIVE']
egncPO = egncPO.convert_dtypes()

egncP2=df_presupuesto_gasoleos.loc[df_presupuesto['UEN']=='PERDRIEL2']
egncP2 = egncP2.convert_dtypes()

egncA=df_presupuesto_gasoleos.loc[df_presupuesto['UEN']=='AZCUENAGA']
egncA = egncA.convert_dtypes()


egncTotal = pd.concat([egncAC,egncA,egncP2,egncPO,egncL,egncSJ,egncP1,egncM2,egncM1,egncM,egncS,egncLH,egncVN,egncU])


egncTotal = egncTotal.rename({'Fecha':'FECHASQL', 'VENTAS':'Presupuesto Diario'},axis=1)

egncTotal = egncTotal.reindex(columns=['UEN','FECHASQL','CODPRODUCTO', 'Presupuesto Diario'])


##
################################################
############# Volumen diario GASOLEOS YPF
################################################

df_gasoleosTotal = pd.read_sql('''
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
		and (CODPRODUCTO = 'GO' OR CODPRODUCTO = 'EU')
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
df_gasoleosTotal = df_gasoleosTotal.convert_dtypes()

############################################
#####Volumen Diario GASOLEOS YPF PLAYA######
############################################

df_gasoleosPlaya = pd.read_sql('''
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
		and (emp.CODPRODUCTO = 'GO' OR emp.CODPRODUCTO = 'EU')
        AND  (P.[DESCRIPCION] like '%ruta%' OR P.[DESCRIPCION] like '%YER%')
		group by emp.FECHASQL,emp.CODPRODUCTO,emp.UEN
		order by CODPRODUCTO,UEN
'''      ,db_conex)
df_gasoleosPlaya = df_gasoleosPlaya.convert_dtypes()


############################################
#####Volumen Diario GASOLEOS REDMAS APPYPF######
############################################

df_gasoleosREDMAS = pd.read_sql('''
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
		and (emp.CODPRODUCTO = 'GO' OR emp.CODPRODUCTO = 'EU')
		AND  (P.[DESCRIPCION] not like '%PRUEBA%' AND P.DESCRIPCION 
		not LIKE '%RUTA%' AND P.DESCRIPCION not LIKE '%YER%')
		group by emp.FECHASQL,emp.CODPRODUCTO,emp.UEN
		order by CODPRODUCTO,UEN
'''      ,db_conex)
df_gasoleosREDMAS = df_gasoleosREDMAS.convert_dtypes()


df_gasoleosTotal=df_gasoleosTotal.merge(df_gasoleosREDMAS,on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')
df_gasoleosTotal=df_gasoleosTotal.merge(df_gasoleosPlaya,on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')
df_gasoleosTotal = df_gasoleosTotal.fillna(0)
df_gasoleosTotal['Total Vendido $'] = ((df_gasoleosTotal['Ventas Efectivo']*df_gasoleosTotal['Precio Cartel'])
                                       +(df_gasoleosTotal['Venta Cta Cte']*df_gasoleosTotal['Precio Cta Cte'])
                                       +(df_gasoleosTotal['Volumen REDMAS']*df_gasoleosTotal['Precio Cartel']))
df_gasoleosTotal['Volumen Total Vendido']=(df_gasoleosTotal['Ventas Efectivo']+df_gasoleosTotal['Venta Cta Cte']
                                           +df_gasoleosTotal['Volumen REDMAS'])

df_gasoleosTotal['Total Vendido YER $']=df_gasoleosTotal['Volumen YER']*df_gasoleosTotal['Precio Cartel']

df_gasoleosTotal['UEN']=df_gasoleosTotal['UEN'].str.strip()
df_gasoleosTotal['CODPRODUCTO']=df_gasoleosTotal['CODPRODUCTO'].str.strip()

presupuestoYPF = egncTotal.loc[(egncTotal['UEN'] == 'LAMADRID') | (egncTotal['UEN'] == 'AZCUENAGA') | (egncTotal['UEN'] == 'PERDRIEL')
                               | (egncTotal['UEN'] == 'PERDRIEL2') | (egncTotal['UEN'] == 'SAN JOSE') | (egncTotal['UEN'] == 'PUENTE OLIVE'),:]

df_gasoleosTotal = df_gasoleosTotal.merge(presupuestoYPF,on=['CODPRODUCTO','UEN','FECHASQL'],how='outer')
df_gasoleosTotal=df_gasoleosTotal.sort_values(['UEN','FECHASQL','CODPRODUCTO'])
# reemplazar los valores 0 en la columna 'Precio Cartel' con valores nulos (NaN)
df_gasoleosTotal['Precio Cartel'] = df_gasoleosTotal['Precio Cartel'].replace(0, np.nan)
# llenar los valores nulos utilizando la función fillna de Pandas con el argumento method='ffill'
df_gasoleosTotal['Precio Cartel'] = df_gasoleosTotal['Precio Cartel'].fillna(method='ffill')
df_gasoleosTotal['Precio Cartel'] = df_gasoleosTotal['Precio Cartel'].fillna(method='bfill')
df_gasoleosTotal['Presupuesto Diario $']=df_gasoleosTotal['Presupuesto Diario']*df_gasoleosTotal['Precio Cartel']

df_gasoleosTotal = df_gasoleosTotal.reindex(columns=['UEN','FECHASQL','CODPRODUCTO','Volumen Total Vendido','Total Vendido $','Total Vendido YER $','Presupuesto Diario','Presupuesto Diario $','Precio Cartel'])
df_gasoleosTotal = df_gasoleosTotal.fillna(0)
df_gasoleosTotal= df_gasoleosTotal.loc[df_gasoleosTotal["FECHASQL"] <= ayer.strftime('%Y-%m-%d'),:]

df_gasoleosTotal=df_gasoleosTotal.sort_values(['UEN','CODPRODUCTO','FECHASQL'])

# Definir una función para aplicar la comisión acumulada según el tramo de ventas
def aplicar_comision_acumulada(df,estacion):
    df=df.sort_values(['UEN','FECHASQL'])
    comi = comisionGO.loc[(comisionGO["UEN"] == estacion),:]
    comi = comi.reset_index()
    df['Ventas Acumuladas']=df['Volumen Total Vendido'].cumsum()
    df['Presu Acumu']=df['Presupuesto Diario'].cumsum()
    df= df.reset_index()
    for i in df.index:
        if df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO I GO']:
            df.loc[i,'Marcador'] = 1
        elif df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO II GO']:
            df.loc[i,'Marcador'] = 2
        elif df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO III GO']:
            df.loc[i,'Marcador'] = 3
        else:
            df.loc[i,'Marcador'] = 4


    for e in df.index:
        if df.loc[e,'Presu Acumu'] <= comi.loc[0,'TRAMO I GO']:
            df.loc[e,'Marcador presu'] = 1
        elif df.loc[e,'Presu Acumu'] <= comi.loc[0,'TRAMO II GO']:
            df.loc[e,'Marcador presu'] = 2
        elif df.loc[e,'Presu Acumu'] <= comi.loc[0,'TRAMO III GO']:
            df.loc[e,'Marcador presu'] = 3
        else:
            df.loc[e,'Marcador presu'] = 4
            

    for r in df.index:
        if df.loc[r,'Marcador'] == 1:
            df.loc[r,'COMISION']=comi.loc[0,'COMISION TRAMO I GO']
        elif df.loc[r,'Marcador'] == 2:
            if df.loc[r,'Marcador'] != df.loc[r-1,'Marcador']:
                tsig = (df.loc[r,'Ventas Acumuladas'] - comi.loc[0,'TRAMO I GO'])
                tant = (df.loc[r,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO I GO']*tant)+(comi.loc[0,'COMISION TRAMO II GO']*tsig)
                df.loc[r,'COMISION']= comis/df.loc[r,'Volumen Total Vendido']
            else:
                df.loc[r,'COMISION']=comi.loc[0,'COMISION TRAMO II GO']
        elif df.loc[r,'Marcador'] == 3:
            if df.loc[r,'Marcador'] != df.loc[r-1,'Marcador']:
                tsig = (df.loc[r,'Ventas Acumuladas'] - comi.loc[0,'TRAMO II GO'])
                tant = (df.loc[r,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO II GO']*tant)+(comi.loc[0,'COMISION TRAMO III GO']*tsig)
                df.loc[r,'COMISION']= comis/df.loc[r,'Volumen Total Vendido']
            else:
                df.loc[r,'COMISION']=comi.loc[0,'COMISION TRAMO III GO']
        elif df.loc[r,'Marcador'] == 4:
            if df.loc[r,'Marcador'] != df.loc[r-1,'Marcador']:
                tsig = (df.loc[r,'Ventas Acumuladas'] - comi.loc[0,'TRAMO III GO'])
                tant = (df.loc[r,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO III GO']*tant)+(comi.loc[0,'COMISION TRAMO IV GO']*tsig)
                df.loc[r,'COMISION']= comis/df.loc[r,'Volumen Total Vendido']
            else:
                df.loc[r,'COMISION']=comi.loc[0,'COMISION TRAMO IV GO']

    for h in df.index:
        if df.loc[h,'Marcador presu'] == 1:
            df.loc[h,'COMISION presu']=comi.loc[0,'COMISION TRAMO I GO']
        elif df.loc[h,'Marcador presu'] == 2:
            if df.loc[h,'Marcador presu'] != df.loc[h-1,'Marcador presu']:
                tsig = (df.loc[h,'Presu Acumu'] - comi.loc[0,'TRAMO I GO'])
                tant = (df.loc[h,'Presupuesto Diario']-tsig)
                comis2=(comi.loc[0,'COMISION TRAMO I GO']*tant)+(comi.loc[0,'COMISION TRAMO II GO']*tsig)
                df.loc[h,'COMISION presu']= comis2/df.loc[h,'Presupuesto Diario']
            else:
                df.loc[h,'COMISION presu']=comi.loc[0,'COMISION TRAMO II GO']
        elif df.loc[h,'Marcador presu'] == 3:
            if df.loc[h,'Marcador presu'] != df.loc[h-1,'Marcador presu']:
                tsig = (df.loc[h,'Presu Acumu'] - comi.loc[0,'TRAMO II GO'])
                tant = (df.loc[h,'Presupuesto Diario']-tsig)
                comis2=(comi.loc[0,'COMISION TRAMO II GO']*tant)+(comi.loc[0,'COMISION TRAMO III GO']*tsig)
                df.loc[h,'COMISION presu']= comis2/df.loc[h,'Presupuesto Diario']
            else:
                df.loc[h,'COMISION presu']=comi.loc[0,'COMISION TRAMO III GO']

        elif df.loc[h,'Marcador presu'] == 4:
            if df.loc[h,'Marcador presu'] != df.loc[h-1,'Marcador presu']:
                tsig = (df.loc[h,'Presu Acumu'] - comi.loc[0,'TRAMO III GO'])
                tant = (df.loc[h,'Presupuesto Diario']-tsig)
                comis2=(comi.loc[0,'COMISION TRAMO III GO']*tant)+(comi.loc[0,'COMISION TRAMO IV GO']*tsig)
                df.loc[h,'COMISION presu']= comis2/df.loc[h,'Presupuesto Diario']
            else:
                df.loc[h,'COMISION presu']=comi.loc[0,'COMISION TRAMO IV GO']

    df['Comision $']= ((df['COMISION']*df['Total Vendido $'])+(df['Total Vendido YER $']*comi.loc[0,'COMISION TRAMO I GO']))*1.21/0.97
    df['Comision Presupuestada $']= df['COMISION']*df['Presupuesto Diario $']*1.21/0.97
    df['Ventas Acumuladas $'] = df['Total Vendido $']+df['Total Vendido YER $']
    df = df.reindex(columns=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'])
    df = df.groupby(
        ["UEN",'CODPRODUCTO']
        , as_index=False
    ).sum(numeric_only=True)
    return df

### Aplico la comision a cada estacion para el producto GO
aperdriel1= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'PERDRIEL') & (df_gasoleosTotal["CODPRODUCTO"] == 'GO'),:]
aperdriel1 = aplicar_comision_acumulada(aperdriel1,'PERDRIEL')
aperdriel2= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'PERDRIEL2') & (df_gasoleosTotal["CODPRODUCTO"] == 'GO'),:]
aperdriel2 = aplicar_comision_acumulada(aperdriel2,'PERDRIEL2')
aazcuenaga= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'AZCUENAGA') & (df_gasoleosTotal["CODPRODUCTO"] == 'GO'),:]
aazcuenaga = aplicar_comision_acumulada(aazcuenaga,'AZCUENAGA')
asan_Jose= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'SAN JOSE') & (df_gasoleosTotal["CODPRODUCTO"] == 'GO'),:]
asan_Jose = aplicar_comision_acumulada(asan_Jose,'SAN JOSE')
apuente_Olive= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'PUENTE OLIVE') & (df_gasoleosTotal["CODPRODUCTO"] == 'GO'),:]
apuente_Olive = aplicar_comision_acumulada(apuente_Olive,'PUENTE OLIVE')
alamadrid= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'LAMADRID') & (df_gasoleosTotal["CODPRODUCTO"] == 'GO'),:]
alamadrid = aplicar_comision_acumulada(alamadrid,'LAMADRID')

### Creo tabla de YPF GO
mbcGOYPF=aperdriel1.merge(aperdriel2,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcGOYPF=mbcGOYPF.merge(aazcuenaga,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcGOYPF=mbcGOYPF.merge(asan_Jose,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcGOYPF=mbcGOYPF.merge(apuente_Olive,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcGOYPF=mbcGOYPF.merge(alamadrid,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')

def aplicar_comision_acumuladaEU(df,estacion):
    df=df.sort_values(['UEN','FECHASQL'])
    comi = comisionEU.loc[(comisionEU["UEN"] == estacion),:]
    comi = comi.reset_index()
    df['Ventas Acumuladas']=df['Volumen Total Vendido'].cumsum()
    df['Presu Acumu']=df['Presupuesto Diario'].cumsum()
    for i in df.index:
        if df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO I EU']:
            df.loc[i,'Marcador'] = 1
        elif df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO II EU']:
            df.loc[i,'Marcador'] = 2
        elif df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO III EU']:
            df.loc[i,'Marcador'] = 3
        else:
            df.loc[i,'Marcador'] = 4

    for e in df.index:
        if df.loc[e,'Presu Acumu'] <= comi.loc[0,'TRAMO I EU']:
            df.loc[e,'Marcador presu'] = 1
        elif df.loc[e,'Presu Acumu'] <= comi.loc[0,'TRAMO II EU']:
            df.loc[e,'Marcador presu'] = 2
        elif df.loc[e,'Presu Acumu'] <= comi.loc[0,'TRAMO III EU']:
            df.loc[e,'Marcador presu'] = 3
        else:
            df.loc[e,'Marcador presu'] = 4

    for r in df.index:
        if df.loc[r,'Marcador'] == 1:
            df.loc[r,'COMISION']=comi.loc[0,'COMISION TRAMO I EU']
        elif df.loc[r,'Marcador'] == 2:
            if df.loc[r,'Marcador'] != df.loc[r-1,'Marcador']:
                tsig = (df.loc[r,'Ventas Acumuladas'] - comi.loc[0,'TRAMO I EU'])
                tant = (df.loc[r,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO I EU']*tant)+(comi.loc[0,'COMISION TRAMO II EU']*tsig)
                df.loc[r,'COMISION']= comis/df.loc[r,'Volumen Total Vendido']
            else:
                df.loc[r,'COMISION']=comi.loc[0,'COMISION TRAMO II EU']
        elif df.loc[r,'Marcador'] == 3:
            if df.loc[r,'Marcador'] != df.loc[r-1,'Marcador']:
                tsig = (df.loc[r,'Ventas Acumuladas'] - comi.loc[0,'TRAMO II EU'])
                tant = (df.loc[r,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO II EU']*tant)+(comi.loc[0,'COMISION TRAMO III EU']*tsig)
                df.loc[r,'COMISION']= comis/df.loc[r,'Volumen Total Vendido']
            else:
                df.loc[r,'COMISION']=comi.loc[0,'COMISION TRAMO III EU']
        elif df.loc[r,'Marcador'] == 4:
            if df.loc[r,'Marcador'] != df.loc[r-1,'Marcador']:
                tsig = (df.loc[r,'Ventas Acumuladas'] - comi.loc[0,'TRAMO III EU'])
                tant = (df.loc[r,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO III EU']*tant)+(comi.loc[0,'COMISION TRAMO IV EU']*tsig)
                df.loc[r,'COMISION']= comis/df.loc[r,'Volumen Total Vendido']
            else:
                df.loc[r,'COMISION']=comi.loc[0,'COMISION TRAMO IV EU']

    for h in df.index:
        if df.loc[h,'Marcador presu'] == 1:
            df.loc[h,'COMISION presu']=comi.loc[0,'COMISION TRAMO I EU']
        elif df.loc[h,'Marcador presu'] == 2:
            if df.loc[h,'Marcador presu'] != df.loc[h-1,'Marcador presu']:
                tsig = (df.loc[h,'Presu Acumu'] - comi.loc[0,'TRAMO I EU'])
                tant = (df.loc[h,'Presupuesto Diario']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO I EU']*tant)+(comi.loc[0,'COMISION TRAMO II EU']*tsig)
                df.loc[h,'COMISION presu']= comis/df.loc[h,'Presupuesto Diario']
            else:
                df.loc[h,'COMISION presu']=comi.loc[0,'COMISION TRAMO II EU']
        elif df.loc[h,'Marcador presu'] == 3:
            if df.loc[h,'Marcador presu'] != df.loc[h-1,'Marcador presu']:
                tsig = (df.loc[h,'Presu Acumu'] - comi.loc[0,'TRAMO II EU'])
                tant = (df.loc[h,'Presupuesto Diario']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO II EU']*tant)+(comi.loc[0,'COMISION TRAMO III EU']*tsig)
                df.loc[h,'COMISION presu']= comis/df.loc[h,'Presupuesto Diario']
            else:
                df.loc[h,'COMISION presu']=comi.loc[0,'COMISION TRAMO III EU']

        elif df.loc[h,'Marcador presu'] == 4:
            if df.loc[h,'Marcador presu'] != df.loc[h-1,'Marcador presu']:
                tsig = (df.loc[h,'Presu Acumu'] - comi.loc[0,'TRAMO III EU'])
                tant = (df.loc[h,'Presupuesto Diario']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO III EU']*tant)+(comi.loc[0,'COMISION TRAMO IV EU']*tsig)
                df.loc[h,'COMISION presu']= comis/df.loc[h,'Presupuesto Diario']
            else:
                df.loc[h,'COMISION presu']=comi.loc[0,'COMISION TRAMO IV EU']


    df['Comision $']= ((df['COMISION']*df['Total Vendido $'])+(df['Total Vendido YER $']*comi.loc[0,'COMISION TRAMO I EU']))*1.21/0.97
    df['Comision Presupuestada $']= df['COMISION']*df['Presupuesto Diario $']*1.21/0.97
    df['Ventas Acumuladas $'] = df['Total Vendido $']+df['Total Vendido YER $']
    df = df.reindex(columns=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'])
    df = df.groupby(
        ["UEN",'CODPRODUCTO']
        , as_index=False
    ).sum(numeric_only=True)
    return df
### Aplico la comision a cada estacion para el producto EU
perdriel1= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'PERDRIEL') & (df_gasoleosTotal["CODPRODUCTO"] == 'EU'),:]
perdriel1 = aplicar_comision_acumuladaEU(perdriel1,'PERDRIEL')
perdriel2= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'PERDRIEL2') & (df_gasoleosTotal["CODPRODUCTO"] == 'EU'),:]
perdriel2 = aplicar_comision_acumuladaEU(perdriel2,'PERDRIEL2')
azcuenaga= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'AZCUENAGA') & (df_gasoleosTotal["CODPRODUCTO"] == 'EU'),:]
azcuenaga = aplicar_comision_acumuladaEU(azcuenaga,'AZCUENAGA')
san_Jose= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'SAN JOSE') & (df_gasoleosTotal["CODPRODUCTO"] == 'EU'),:]
san_Jose = aplicar_comision_acumuladaEU(san_Jose,'SAN JOSE')
puente_Olive= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'PUENTE OLIVE') & (df_gasoleosTotal["CODPRODUCTO"] == 'EU'),:]
puente_Olive = aplicar_comision_acumuladaEU(puente_Olive,'PUENTE OLIVE')
lamadrid= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'LAMADRID') & (df_gasoleosTotal["CODPRODUCTO"] == 'EU'),:]
lamadrid = aplicar_comision_acumuladaEU(lamadrid,'LAMADRID')
### Creo tabla de YPF EU
mbcEUYPF=perdriel1.merge(perdriel2,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcEUYPF=mbcEUYPF.merge(azcuenaga,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcEUYPF=mbcEUYPF.merge(san_Jose,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcEUYPF=mbcEUYPF.merge(puente_Olive,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcEUYPF=mbcEUYPF.merge(lamadrid,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')

### Concateno Tabla De YPF GO con YPF EU
mbcYPF=mbcGOYPF.merge(mbcEUYPF,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')




##################################################
####### VENTAS DAPSA #############################
##################################################



####  Volumen diario GASOLEOS YPF
df_gasoleosdapsa = pd.read_sql(''' 
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
		and (CODPRODUCTO = 'GO' OR CODPRODUCTO = 'EU')
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
df_gasoleosdapsa = df_gasoleosdapsa.convert_dtypes()
df_gasoleosdapsa =df_gasoleosdapsa.fillna(0)
### Descuentos
df_gasoleosdapsaDesc = pd.read_sql('''

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
		and (emp.CODPRODUCTO = 'GO' OR emp.CODPRODUCTO = 'EU')
        AND (EmP.[CODPROMO] = '30'
            OR P.[DESCRIPCION] like '%PRUEBA%')
		GROUP BY emp.UEN,EMP.FECHASQL,EMP.CODPRODUCTO

  '''      ,db_conex)
df_gasoleosdapsaDesc = df_gasoleosdapsaDesc.convert_dtypes()
### CONCATENO TABLA DE VOLUMEN TOTAL VENDIDO CON LA TABLA DE LOS DESCUENTOS
df_gasoleosdapsa = df_gasoleosdapsa.merge(df_gasoleosdapsaDesc, on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')
df_gasoleosdapsa =df_gasoleosdapsa.fillna(0)



df_gasoleosdapsa['Total Vendido $'] = ((df_gasoleosdapsa['Ventas Efectivo']*df_gasoleosdapsa['Precio Cartel'])
                                       +(df_gasoleosdapsa['Venta Cta Cte']*df_gasoleosdapsa['Precio Cta Cte'])
                                       +((df_gasoleosdapsa['ventas Promos']+df_gasoleosdapsa['Descuentos'])*df_gasoleosdapsa['Precio Cartel']))

df_gasoleosdapsa['Volumen Total Vendido']=(df_gasoleosdapsa['Ventas Efectivo']+df_gasoleosdapsa['Venta Cta Cte']
                                           +(df_gasoleosdapsa['ventas Promos']+df_gasoleosdapsa['Descuentos']))

presupuestoDAPSA = egncTotal.loc[(egncTotal['UEN'] != 'LAMADRID') | (egncTotal['UEN'] != 'AZCUENAGA') | (egncTotal['UEN'] != 'PERDRIEL')
                                 | (egncTotal['UEN'] != 'PERDRIEL2') | (egncTotal['UEN'] != 'SAN JOSE') | (egncTotal['UEN'] != 'PUENTE OLIVE'),:]

df_gasoleosdapsa['UEN']=df_gasoleosdapsa['UEN'].str.strip()
df_gasoleosdapsa['CODPRODUCTO']=df_gasoleosdapsa['CODPRODUCTO'].str.strip()

df_gasoleosdapsa = df_gasoleosdapsa.merge(presupuestoDAPSA,on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')


df_gasoleosdapsaGO = df_gasoleosdapsa.loc[(df_gasoleosdapsa["CODPRODUCTO"] == 'GO'),:]
df_gasoleosdapsaEU = df_gasoleosdapsa.loc[(df_gasoleosdapsa["CODPRODUCTO"] == 'EU'),:]

####### GO
df_gasoleosdapsaGO=df_gasoleosdapsaGO.sort_values(['UEN','FECHASQL'])
# reemplazar los valores 0 en la columna 'Precio Cartel' con valores nulos (NaN)
df_gasoleosdapsaGO['Precio Cartel'] = df_gasoleosdapsaGO['Precio Cartel'].replace(0, np.nan)
# llenar los valores nulos utilizando la función fillna de Pandas con el argumento method='ffill'
df_gasoleosdapsaGO['Precio Cartel'] = df_gasoleosdapsaGO['Precio Cartel'].fillna(method='ffill')
df_gasoleosdapsaGO['Precio Cartel'] = df_gasoleosdapsaGO['Precio Cartel'].fillna(method='bfill')

df_gasoleosdapsaGO['Presupuesto Diario $']=df_gasoleosdapsaGO['Presupuesto Diario']*df_gasoleosdapsaGO['Precio Cartel']

df_gasoleosdapsaGO = df_gasoleosdapsaGO.reindex(columns=['UEN','FECHASQL','CODPRODUCTO','Volumen Total Vendido','Total Vendido $','Total Vendido YER $','Presupuesto Diario','Presupuesto Diario $','Precio Cartel'])
df_gasoleosdapsaGO = df_gasoleosdapsaGO.fillna(0)
df_gasoleosdapsaGO= df_gasoleosdapsaGO.loc[df_gasoleosdapsaGO["FECHASQL"] <= ayer.strftime('%Y-%m-%d'),:]

df_gasoleosdapsaGO=df_gasoleosdapsaGO.sort_values(['UEN','CODPRODUCTO','FECHASQL'])

costoDapsaGO= costoDapsaGO.loc[(costoDapsaGO["FECHASQL"] <= ayer.strftime('%Y-%m-%d')) & (costoDapsaGO["FECHASQL"] >= primer_dia_mes.strftime('%Y-%m-%d')),:]


########## EU
df_gasoleosdapsaEU=df_gasoleosdapsaEU.sort_values(['UEN','FECHASQL'])
# reemplazar los valores 0 en la columna 'Precio Cartel' con valores nulos (NaN)
df_gasoleosdapsaEU['Precio Cartel'] = df_gasoleosdapsaEU['Precio Cartel'].replace(0, np.nan)
# llenar los valores nulos utilizando la función fillna de Pandas con el argumento method='ffill'
df_gasoleosdapsaEU['Precio Cartel'] = df_gasoleosdapsaEU['Precio Cartel'].fillna(method='ffill')
df_gasoleosdapsaEU['Precio Cartel'] = df_gasoleosdapsaEU['Precio Cartel'].fillna(method='bfill')

df_gasoleosdapsaEU['Presupuesto Diario $']=df_gasoleosdapsaEU['Presupuesto Diario']*df_gasoleosdapsaEU['Precio Cartel']

df_gasoleosdapsaEU = df_gasoleosdapsaEU.reindex(columns=['UEN','FECHASQL','CODPRODUCTO','Volumen Total Vendido','Total Vendido $','Total Vendido YER $','Presupuesto Diario','Presupuesto Diario $','Precio Cartel'])
df_gasoleosdapsaEU = df_gasoleosdapsaEU.fillna(0)
df_gasoleosdapsaEU= df_gasoleosdapsaEU.loc[df_gasoleosdapsaEU["FECHASQL"] <= ayer.strftime('%Y-%m-%d'),:]

df_gasoleosdapsaEU=df_gasoleosdapsaEU.sort_values(['UEN','CODPRODUCTO','FECHASQL'])

costoDapsaEU= costoDapsaEU.loc[(costoDapsaEU["FECHASQL"] <= ayer.strftime('%Y-%m-%d')) & (costoDapsaEU["FECHASQL"] >= primer_dia_mes.strftime('%Y-%m-%d')),:]

## FUNCION PARA APLICAR COMISION GO

def aplicar_comision_acumuladaDAPSA(df):
    df=df.sort_values(['UEN','FECHASQL'])
    df = df.merge(costoDapsaGO,on=['FECHASQL','CODPRODUCTO'],how='outer')
    # convertir la columna 'Precio Cartel' a un tipo de datos numérico
    df['Precio Cartel'] = pd.to_numeric(df['Precio Cartel'], errors='coerce')

    # reemplazar los valores 0 en la columna 'Precio Cartel' con valores nulos (NaN)
    df['Precio Cartel'] = df['Precio Cartel'].replace(0, np.nan)
    # llenar los valores nulos utilizando la función fillna de Pandas con el argumento method='ffill'
    df['Precio Cartel'] = df['Precio Cartel'].fillna(method='ffill')

    df['Comision']= df['Precio Cartel']-df['COSTO GO DAP']
    df['Comision $']= df['Comision']*df['Volumen Total Vendido']
    df['Comision Presupuestada $']= df['Comision']*df['Presupuesto Diario']
    df['Ventas Acumuladas $'] = df['Volumen Total Vendido']*df['Precio Cartel']
    df = df.reindex(columns=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'])
    df = df.groupby(
        ["UEN",'CODPRODUCTO']
        , as_index=False
    ).sum(numeric_only=True)
    return df

### Aplico la comision a cada estacion para el producto GO
las_heras= df_gasoleosdapsaGO.loc[(df_gasoleosdapsaGO["UEN"] == 'LAS HERAS') & (df_gasoleosdapsaGO["CODPRODUCTO"] == 'GO'),:]
las_heras = aplicar_comision_acumuladaDAPSA(las_heras)
amercado1= df_gasoleosdapsaGO.loc[(df_gasoleosdapsaGO["UEN"] == 'MERC GUAYMALLEN') & (df_gasoleosdapsaGO["CODPRODUCTO"] == 'GO'),:]
amercado1 = aplicar_comision_acumuladaDAPSA(amercado1)
amercado2= df_gasoleosdapsaGO.loc[(df_gasoleosdapsaGO["UEN"] == 'MERCADO 2') & (df_gasoleosdapsaGO["CODPRODUCTO"] == 'GO'),:]
amercado2 = aplicar_comision_acumuladaDAPSA(amercado2)
sarmiento= df_gasoleosdapsaGO.loc[(df_gasoleosdapsaGO["UEN"] == 'SARMIENTO') & (df_gasoleosdapsaGO["CODPRODUCTO"] == 'GO'),:]
sarmiento = aplicar_comision_acumuladaDAPSA(sarmiento)
villa_nueva= df_gasoleosdapsaGO.loc[(df_gasoleosdapsaGO["UEN"] == 'VILLANUEVA') & (df_gasoleosdapsaGO["CODPRODUCTO"] == 'GO'),:]
villa_nueva = aplicar_comision_acumuladaDAPSA(villa_nueva)
adolfo_Calle= df_gasoleosdapsaGO.loc[(df_gasoleosdapsaGO["UEN"] == 'ADOLFO CALLE') & (df_gasoleosdapsaGO["CODPRODUCTO"] == 'GO'),:]
adolfo_Calle = aplicar_comision_acumuladaDAPSA(adolfo_Calle)
mitre= df_gasoleosdapsaGO.loc[(df_gasoleosdapsaGO["UEN"] == 'MITRE') & (df_gasoleosdapsaGO["CODPRODUCTO"] == 'GO'),:]
mitre = aplicar_comision_acumuladaDAPSA(mitre)
urquiza= df_gasoleosdapsaGO.loc[(df_gasoleosdapsaGO["UEN"] == 'URQUIZA') & (df_gasoleosdapsaGO["CODPRODUCTO"] == 'GO'),:]
urquiza = aplicar_comision_acumuladaDAPSA(urquiza)

### Creo tabla de YPF GO
mbcGODAPSA=las_heras.merge(amercado1,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcGODAPSA=mbcGODAPSA.merge(amercado2,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcGODAPSA=mbcGODAPSA.merge(sarmiento,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcGODAPSA=mbcGODAPSA.merge(villa_nueva,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcGODAPSA=mbcGODAPSA.merge(adolfo_Calle,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcGODAPSA=mbcGODAPSA.merge(mitre,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
mbcGODAPSA=mbcGODAPSA.merge(urquiza,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')

## FUNCION PARA APLICAR COMISION EU
def aplicar_comision_acumuladaDAPSAEU(df):
    df=df.sort_values(['UEN','FECHASQL'])
    df = df.merge(costoDapsaEU,on=['FECHASQL','CODPRODUCTO'],how='outer')
    # convertir la columna 'Precio Cartel' a un tipo de datos numérico
    df['Precio Cartel'] = pd.to_numeric(df['Precio Cartel'], errors='coerce')

    # reemplazar los valores 0 en la columna 'Precio Cartel' con valores nulos (NaN)
    df['Precio Cartel'] = df['Precio Cartel'].replace(0, np.nan)
    # llenar los valores nulos utilizando la función fillna de Pandas con el argumento method='ffill'
    df['Precio Cartel'] = df['Precio Cartel'].fillna(method='ffill')

    df['Comision']= df['Precio Cartel']-df['COSTO EU DAP']
    df['Comision $']= df['Comision']*df['Volumen Total Vendido']
    df['Comision Presupuestada $']= df['Comision']*df['Presupuesto Diario']
    df['Ventas Acumuladas $'] = df['Volumen Total Vendido']*df['Precio Cartel']
    df = df.reindex(columns=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'])
    df = df.groupby(
        ["UEN",'CODPRODUCTO']
        , as_index=False
    ).sum(numeric_only=True)
    return df

### Aplico la comision a cada estacion para el producto EU

mercado1= df_gasoleosdapsaEU.loc[(df_gasoleosdapsaEU["UEN"] == 'MERC GUAYMALLEN') & (df_gasoleosdapsaEU["CODPRODUCTO"] == 'EU'),:]
mercado1 = aplicar_comision_acumuladaDAPSAEU(mercado1)
mercado2= df_gasoleosdapsaEU.loc[(df_gasoleosdapsaEU["UEN"] == 'MERCADO 2') & (df_gasoleosdapsaEU["CODPRODUCTO"] == 'EU'),:]
mercado2 = aplicar_comision_acumuladaDAPSAEU(mercado2)

### Creo tabla de DAPSA EU
mbcGODAPSAEU=mercado1.merge(mercado2,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')
### Concateno Tablas de Dapsa GO y EU
mbcDapsa=mbcGODAPSA.merge(mbcGODAPSAEU,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')

### Concateno tablas de YPF y Dapsa TOTALES
mbcTOTAL = mbcYPF.merge(mbcDapsa,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Presupuesto Diario $','Comision Presupuestada $','Comision $'],how='outer')



## Creo Dataframe de GO
go = mbcTOTAL['CODPRODUCTO'] == "GO"
mbcTOTALGO = mbcTOTAL[go]
mbcTOTALGO = mbcTOTALGO.rename({'Presupuesto Diario $':'Presupuesto Acumulado $','Comision Presupuestada $':'MBC Presupuestado $','Comision $':'MBC Acumulado $'},axis=1)
mbcTOTALGO = mbcTOTALGO.reindex(columns=['UEN','Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $'])
## Creo Dataframe de EU
eu = mbcTOTAL['CODPRODUCTO'] == "EU"
mbcTotalEU = mbcTOTAL[eu]
mbcTotalEU = mbcTotalEU.rename({'Presupuesto Diario $':'Presupuesto Acumulado $','Comision Presupuestada $':'MBC Presupuestado $','Comision $':'MBC Acumulado $'},axis=1)
mbcTotalEU=mbcTotalEU.reindex(columns=['UEN','Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $'])

###### Columnas de Desvio y Totales GO
mbcTOTALGO['Desvio Presupuestado %']=(mbcTOTALGO['Ventas Acumuladas $']/mbcTOTALGO['Presupuesto Acumulado $'])-1
mbcTOTALGO['Desvio MBC %']=(mbcTOTALGO['MBC Acumulado $']/mbcTOTALGO['MBC Presupuestado $'])-1

mbcTOTALGO.loc["colTOTAL"]= pd.Series(
    mbcTOTALGO.sum(numeric_only=True)
    , index=['Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $']
)
mbcTOTALGO.fillna({"UEN":"TOTAL"}, inplace=True)

tasa = (mbcTOTALGO.loc["colTOTAL",'Ventas Acumuladas $'] /
        mbcTOTALGO.loc["colTOTAL",'Presupuesto Acumulado $'])-1
mbcTOTALGO.fillna({"Desvio Presupuestado %":tasa}, inplace=True)

tasa2 = (mbcTOTALGO.loc["colTOTAL",'MBC Acumulado $'] /
         mbcTOTALGO.loc["colTOTAL",'MBC Presupuestado $'])-1
mbcTOTALGO.fillna({'Desvio MBC %':tasa2}, inplace=True)
mbcTOTALGO=mbcTOTALGO.reindex(columns=['UEN','Presupuesto Acumulado $','Ventas Acumuladas $','Desvio Presupuestado %','MBC Presupuestado $','MBC Acumulado $','Desvio MBC %'])

###### Columnas de Desvio y Totales EU
mbcTotalEU['Desvio Presupuestado %']=(mbcTotalEU['Ventas Acumuladas $']/mbcTotalEU['Presupuesto Acumulado $'])-1
mbcTotalEU['Desvio MBC %']=(mbcTotalEU['MBC Acumulado $']/mbcTotalEU['MBC Presupuestado $'])-1

mbcTotalEU.loc["colTOTAL"]= pd.Series(
    mbcTotalEU.sum(numeric_only=True)
    , index=['Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $']
)
mbcTotalEU.fillna({"UEN":"TOTAL"}, inplace=True)

tasa = (mbcTotalEU.loc["colTOTAL",'Ventas Acumuladas $'] /
        mbcTotalEU.loc["colTOTAL",'Presupuesto Acumulado $'])-1
mbcTotalEU.fillna({"Desvio Presupuestado %":tasa}, inplace=True)

tasa2 = (mbcTotalEU.loc["colTOTAL",'MBC Acumulado $'] /
         mbcTotalEU.loc["colTOTAL",'MBC Presupuestado $'])-1
mbcTotalEU.fillna({'Desvio MBC %':tasa2}, inplace=True)
mbcTotalEU=mbcTotalEU.reindex(columns=['UEN','Presupuesto Acumulado $','Ventas Acumuladas $','Desvio Presupuestado %','MBC Presupuestado $','MBC Acumulado $','Desvio MBC %'])


# Varialbes para reporte general

mbcGasoleosGO=mbcTOTALGO

mbcGasoleosEU=mbcTotalEU




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
mbcTOTALGO = _estiladorVtaTituloD(mbcTOTALGO,numCols,percColsPen, "MBC Gasoleos Ultra Diesel")
mbcTotalEU = _estiladorVtaTituloD(mbcTotalEU,numCols,percColsPen, "MBC Gasoleos Infinia Diesel")

#### DEFINO EL DESTINO DONDE SE GUARDARA LA IMAGEN Y EL NOMBRE
#ubicacion = "C:/Users/gmartinez/Desktop/new/Informes/Margen_Playa/"
ubicacion = "C:/Informes/Margen_Playa/"
nombrePen = "MBCGasoleosGO.png"
nombrePenDiario = "MBCGasoleosEU.png"
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

df_to_image(mbcTOTALGO, ubicacion, nombrePen)
df_to_image(mbcTotalEU,ubicacion,nombrePenDiario)


#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)






