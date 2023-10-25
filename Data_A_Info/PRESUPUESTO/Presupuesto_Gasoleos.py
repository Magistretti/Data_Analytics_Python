import os
import math
import numpy as np
from DatosLogin import login
#from Conectores import conectorMSSQL
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

today = datetime.today()  # obtiene la fecha actual
start_of_month = datetime(today.year, today.month, 1)  # obtiene la fecha de inicio del mes actual
start_of_month=start_of_month.strftime('%Y-%m-%d')
today=today.strftime('%Y-%m-%d')

now = datetime.now()  # Obtiene la fecha y hora actual
primer_dia_mes_actual = datetime(now.year, now.month, 1)  # Obtiene el primer día del mes actual
ultimo_dia_mes_anterior = primer_dia_mes_actual - timedelta(days=1)  # Resta un día para obtener el último día del mes anterior
primer_dia_mes_anterior = datetime(ultimo_dia_mes_anterior.year, ultimo_dia_mes_anterior.month, 1)  # Obtiene el primer día del mes anterior
fecha_inicio_mes_anterior = primer_dia_mes_anterior.strftime('%Y-%m-%d')

    
hoy = datetime.now()
ayer = hoy - timedelta(days=2)
primer_dia_mes = ayer.replace(day=1)


##########################################
# TRABAJO CON TABLA DE GOOGLE SHEET  ################
##########################################

###PRESUPUESTO
sheet_id='1ofJ38O2-dLfw7TZOxeTDYUzlWgl6hN2iMLZCMscTgGo'
hoja='Presupuesto'
gsheet_url_presupuesto = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id, hoja)
df_presupuesto=pd.read_csv(gsheet_url_presupuesto)
df_presupuesto = df_presupuesto.convert_dtypes()
df_presupuesto['FECHASQL'] = pd.to_datetime(df_presupuesto['FECHASQL'], format='%d/%m/%Y')
df_presupuesto= df_presupuesto.loc[(df_presupuesto["FECHASQL"] <= ayer.strftime('%Y-%m-%d'))& (df_presupuesto["FECHASQL"] >= primer_dia_mes.strftime('%Y-%m-%d')),:]




df_presupuesto_gasoleos=df_presupuesto.loc[(df_presupuesto['CODPRODUCTO']=='GO') | (df_presupuesto['CODPRODUCTO']=='EU')]

egncTotal=df_presupuesto_gasoleos
egncTotal = egncTotal.rename({'Fecha':'FECHASQL', 'VENTAS':'Presupuesto Diario'},axis=1)
egncTotal = egncTotal.reindex(columns=['UEN','FECHASQL','CODPRODUCTO','Presupuesto Diario'])

################################################
############# Volumen diario GASOLEOS YPF 
################################################

df_gasoleosTotal = pd.read_sql(f'''
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
    WHERE FECHASQL >= '{primer_dia_mes.strftime('%Y-%d-%m')}'
        AND FECHASQL <= '{ayer.strftime('%Y-%d-%m')}'
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
#####Volumen Diario GASOLEOS REDMAS APPYPF######
############################################

df_gasoleosREDMAS = pd.read_sql(f'''
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
	SELECT emp.UEN,emp.FECHASQL,sum(-emp.VOLUMEN) as 'Pruebas Surtidor',emp.CODPRODUCTO,MAX(emp.PRECIO) AS 'Precio Promos'
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL >= '{primer_dia_mes.strftime('%Y-%d-%m')}'
        AND FECHASQL <= '{ayer.strftime('%Y-%d-%m')}'
		and emp.UEN IN (
            'LAMADRID'
            ,'AZCUENAGA'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
            ,'SAN JOSE'
        )
		and (emp.CODPRODUCTO = 'GO' OR emp.CODPRODUCTO = 'EU')
        AND  (P.[DESCRIPCION] like '%PRUEBA%')
		group by emp.FECHASQL,emp.CODPRODUCTO,emp.UEN
		order by CODPRODUCTO,UEN
'''      ,db_conex)
df_gasoleosREDMAS = df_gasoleosREDMAS.convert_dtypes()


df_gasoleosTotal=df_gasoleosTotal.merge(df_gasoleosREDMAS,on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')
df_gasoleosTotal = df_gasoleosTotal.fillna(0)

df_gasoleosTotal['Total Vendido $'] = ((df_gasoleosTotal['VENTA TOTAL VOLUMEN']*df_gasoleosTotal['Precio Cartel'])
                                          +df_gasoleosTotal['Pruebas Surtidor']*df_gasoleosTotal['Precio Cartel'])

df_gasoleosTotal['Volumen Total Vendido']=(df_gasoleosTotal['VENTA TOTAL VOLUMEN']+df_gasoleosTotal['Pruebas Surtidor'])

presupuestoYPF = egncTotal.loc[(egncTotal['UEN'] == 'LAMADRID') | (egncTotal['UEN'] == 'AZCUENAGA') | (egncTotal['UEN'] == 'PERDRIEL')
                               | (egncTotal['UEN'] == 'PERDRIEL2') | (egncTotal['UEN'] == 'SAN JOSE') | (egncTotal['UEN'] == 'PUENTE OLIVE'),:]

df_gasoleosTotal['UEN']= df_gasoleosTotal['UEN'].str.strip()
df_gasoleosTotal['CODPRODUCTO']= df_gasoleosTotal['CODPRODUCTO'].str.strip()

df_gasoleosTotal = df_gasoleosTotal.merge(presupuestoYPF,on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')

# reemplazar los valores 0 en la columna 'Precio Cartel' con valores nulos (NaN)
df_gasoleosTotal['Precio Cartel'] = df_gasoleosTotal['Precio Cartel'].replace(0, np.nan)
# llenar los valores nulos utilizando la función fillna de Pandas con el argumento method='ffill'
df_gasoleosTotal['Precio Cartel'] = df_gasoleosTotal['Precio Cartel'].fillna(method='ffill')
df_gasoleosTotal['Precio Cartel'] = df_gasoleosTotal['Precio Cartel'].fillna(method='bfill')

df_gasoleosTotal['Presupuesto Diario $']=df_gasoleosTotal['Presupuesto Diario']*df_gasoleosTotal['Precio Cartel']

df_gasoleosTotal = df_gasoleosTotal.reindex(columns=['UEN','FECHASQL','Presupuesto Diario','CODPRODUCTO','Volumen Total Vendido','Total Vendido $','Total Vendido YER $','Presupuesto Diario $','Precio Cartel'])
df_gasoleosTotal = df_gasoleosTotal.fillna(0)
df_gasoleosTotal= df_gasoleosTotal.loc[df_gasoleosTotal["FECHASQL"] <= ayer.strftime('%Y-%m-%d'),:]

df_gasoleosTotal=df_gasoleosTotal.sort_values(['UEN','CODPRODUCTO','FECHASQL'])
costogasoleosYPF=df_gasoleosTotal

df_gasoleosTotal = df_gasoleosTotal.reindex(columns=['UEN','Presupuesto Diario','CODPRODUCTO','Volumen Total Vendido','Total Vendido $','Presupuesto Diario $'])
df_gasoleosTotal = df_gasoleosTotal.groupby(
        ["UEN",'CODPRODUCTO']
        , as_index=False
    ).sum(numeric_only=True)

##################################################
####### VENTAS DAPSA #############################
##################################################

####  Volumen diario GASOLEOS YPF 
df_gasoleosdapsa = pd.read_sql(f''' 
         DECLARE @ayer DATETIME
	SET @ayer = DATEADD(day, -2, CAST(GETDATE() AS date))
	DECLARE @inicioMesActual DATETIME
	SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

        DECLARE @inicioMesAnterior DATETIME
        SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

        --Divide por la cant de días del mes anterior y multiplica por la cant de días del
        --mes actual
        
        DECLARE @hoy DATETIME
        SET @hoy = DATEADD(DAY, DATEDIFF(DAY,-1, CURRENT_TIMESTAMP), 0)

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
    WHERE FECHASQL >= '{primer_dia_mes.strftime('%Y-%d-%m')}'
        AND FECHASQL <= '{ayer.strftime('%Y-%d-%m')}'
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
df_naftadapsaDesc = pd.read_sql(f'''

            DECLARE @ayer DATETIME
	SET @ayer = DATEADD(day, -2, CAST(GETDATE() AS date))
	DECLARE @inicioMesActual DATETIME
	SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

	DECLARE @inicioMesAnterior DATETIME
	SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

	--Divide por la cant de días del mes anterior y multiplica por la cant de días del
	--mes actual
	
	DECLARE @hoy DATETIME
	SET @hoy = DATEADD(DAY, DATEDIFF(DAY, -1, CURRENT_TIMESTAMP), 0)
	SELECT emp.UEN,EmP.FECHASQL
        ,EmP.[CODPRODUCTO]
        ,SUM(-EmP.[VOLUMEN]) as 'Descuentos'
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL >= '{primer_dia_mes.strftime('%Y-%d-%m')}'
        AND FECHASQL <= '{ayer.strftime('%Y-%d-%m')}'
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
df_naftadapsaDesc = df_naftadapsaDesc.convert_dtypes()
### CONCATENO TABLA DE VOLUMEN TOTAL VENDIDO CON LA TABLA DE LOS DESCUENTOS
df_gasoleosdapsa = df_gasoleosdapsa.merge(df_naftadapsaDesc, on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')
df_gasoleosdapsa =df_gasoleosdapsa.fillna(0)

df_gasoleosdapsa['Total Vendido $'] = ((df_gasoleosdapsa['Ventas Efectivo']*df_gasoleosdapsa['Precio Cartel'])
                                       +(df_gasoleosdapsa['Venta Cta Cte']*df_gasoleosdapsa['Precio Cta Cte'])
                                      +((df_gasoleosdapsa['ventas Promos']+df_gasoleosdapsa['Descuentos'])*df_gasoleosdapsa['Precio Cartel']))

df_gasoleosdapsa['Volumen Total Vendido']=(df_gasoleosdapsa['Ventas Efectivo']+df_gasoleosdapsa['Venta Cta Cte']
                                           +(df_gasoleosdapsa['ventas Promos']+df_gasoleosdapsa['Descuentos']))

egncTotal['UEN']= egncTotal['UEN'].str.strip()
egncTotal['CODPRODUCTO']= egncTotal['CODPRODUCTO'].str.strip()

presupuestoDAPSA = egncTotal.loc[~((egncTotal['UEN'] == 'LAMADRID') | (egncTotal['UEN'] == 'AZCUENAGA') | (egncTotal['UEN'] == 'PERDRIEL')
                               | (egncTotal['UEN'] == 'PERDRIEL2') | (egncTotal['UEN'] == 'SAN JOSE') | (egncTotal['UEN'] == 'PUENTE OLIVE')) ,:]

df_gasoleosdapsa['UEN']= df_gasoleosdapsa['UEN'].str.strip()
df_gasoleosdapsa['CODPRODUCTO']= df_gasoleosdapsa['CODPRODUCTO'].str.strip()
df_gasoleosdapsa = df_gasoleosdapsa.merge(presupuestoDAPSA,on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')

# reemplazar los valores 0 en la columna 'Precio Cartel' con valores nulos (NaN)
df_gasoleosdapsa['Precio Cartel'] = df_gasoleosdapsa['Precio Cartel'].replace(0, np.nan)
# llenar los valores nulos utilizando la función fillna de Pandas con el argumento method='ffill'
df_gasoleosdapsa['Precio Cartel'] = df_gasoleosdapsa['Precio Cartel'].fillna(method='ffill')
df_gasoleosdapsa['Precio Cartel'] = df_gasoleosdapsa['Precio Cartel'].fillna(method='bfill')

df_gasoleosdapsa['Presupuesto Diario $']=df_gasoleosdapsa['Presupuesto Diario']*df_gasoleosdapsa['Precio Cartel']

#df_gasoleosdapsa = df_gasoleosdapsa.reindex(columns=['UEN','FECHASQL','CODPRODUCTO','Volumen Total Vendido','Total Vendido $','Total Vendido YER $','Presupuesto Diario','Presupuesto Diario $','Precio Cartel'])
df_gasoleosdapsa = df_gasoleosdapsa.fillna(0)
df_gasoleosdapsa= df_gasoleosdapsa.loc[df_gasoleosdapsa["FECHASQL"] <= ayer.strftime('%Y-%m-%d'),:]

df_gasoleosdapsa=df_gasoleosdapsa.sort_values(['UEN','CODPRODUCTO','FECHASQL'])

#df_gasoleosdapsa = df_gasoleosdapsa.reindex(columns=['UEN','Presupuesto Diario','CODPRODUCTO','Volumen Total Vendido','Total Vendido $','Presupuesto Diario $'])
df_gasoleosdapsa = df_gasoleosdapsa.groupby(
        ["UEN",'CODPRODUCTO']
        , as_index=False
    ).sum(numeric_only=True)


mbcTOTAL = df_gasoleosTotal.merge(df_gasoleosdapsa,on=['UEN','CODPRODUCTO','Presupuesto Diario $','Volumen Total Vendido','Presupuesto Diario','Total Vendido $'],how='outer')
mbcTOTAL = mbcTOTAL.rename({'Total Vendido $':'Ventas Acumuladas $'},axis=1)

## Creo Dataframe de GO
GO = mbcTOTAL['CODPRODUCTO'] == "GO"
mbcTOTALGO = mbcTOTAL[GO]
mbcTOTALGO = mbcTOTALGO.rename({'Presupuesto Diario':'Presupuesto Acumulado L','Presupuesto Diario $':'Presupuesto Acumulado $'},axis=1)
mbcTOTALGO = mbcTOTALGO.reindex(columns=['UEN','Ventas Acumuladas $','Presupuesto Acumulado $','Presupuesto Acumulado L','Volumen Total Vendido'])
## Creo Dataframe de EU
EU = mbcTOTAL['CODPRODUCTO'] == "EU"
mbcTotalEU = mbcTOTAL[EU]
mbcTotalEU = mbcTotalEU.rename({'Presupuesto Diario':'Presupuesto Acumulado L','Presupuesto Diario $':'Presupuesto Acumulado $'},axis=1)
mbcTotalEU=mbcTotalEU.reindex(columns=['UEN','Ventas Acumuladas $','Presupuesto Acumulado $','Presupuesto Acumulado L','Volumen Total Vendido'])

###### Columnas de Desvio y Totales GO
mbcTOTALGO['Desvio Presupuestado L']=(mbcTOTALGO['Volumen Total Vendido']-mbcTOTALGO['Presupuesto Acumulado L'])
mbcTOTALGO['Desvio Presupuestado L %']=(mbcTOTALGO['Volumen Total Vendido']/mbcTOTALGO['Presupuesto Acumulado L'])-1

mbcTOTALGO['Desvio Presupuestado $']=(mbcTOTALGO['Ventas Acumuladas $']-mbcTOTALGO['Presupuesto Acumulado $'])
mbcTOTALGO['Desvio Presupuestado $ %']=(mbcTOTALGO['Ventas Acumuladas $']/mbcTOTALGO['Presupuesto Acumulado $'])-1

mbcTOTALGO.loc["colTOTAL"]= pd.Series(
    mbcTOTALGO.sum(numeric_only=True)
    , index=['Ventas Acumuladas $','Presupuesto Acumulado $','Presupuesto Acumulado L','Volumen Total Vendido']
)
mbcTOTALGO.fillna({"UEN":"TOTAL"}, inplace=True)

tasa = (mbcTOTALGO.loc["colTOTAL",'Volumen Total Vendido'] -
    mbcTOTALGO.loc["colTOTAL",'Presupuesto Acumulado L'])
mbcTOTALGO.fillna({"Desvio Presupuestado L":tasa}, inplace=True)

tasa2 = (mbcTOTALGO.loc["colTOTAL",'Volumen Total Vendido'] /
    mbcTOTALGO.loc["colTOTAL",'Presupuesto Acumulado L'])-1
mbcTOTALGO.fillna({'Desvio Presupuestado L %':tasa2}, inplace=True)


tasa3 = (mbcTOTALGO.loc["colTOTAL",'Ventas Acumuladas $'] -
    mbcTOTALGO.loc["colTOTAL",'Presupuesto Acumulado $'])
mbcTOTALGO.fillna({"Desvio Presupuestado $":tasa3}, inplace=True)

tasa4 = (mbcTOTALGO.loc["colTOTAL",'Ventas Acumuladas $'] /
    mbcTOTALGO.loc["colTOTAL",'Presupuesto Acumulado $'])-1
mbcTOTALGO.fillna({'Desvio Presupuestado $ %':tasa4}, inplace=True)

mbcTOTALGO=mbcTOTALGO.reindex(columns=['UEN','Presupuesto Acumulado L','Volumen Total Vendido','Desvio Presupuestado L','Desvio Presupuestado L %','Presupuesto Acumulado $','Ventas Acumuladas $','Desvio Presupuestado $','Desvio Presupuestado $ %'])

###### Columnas de Desvio y Totales EU
mbcTotalEU['Desvio Presupuestado L']=(mbcTotalEU['Volumen Total Vendido']-mbcTotalEU['Presupuesto Acumulado L'])
mbcTotalEU['Desvio Presupuestado L %']=(mbcTotalEU['Volumen Total Vendido']/mbcTotalEU['Presupuesto Acumulado L'])-1

mbcTotalEU['Desvio Presupuestado $']=(mbcTotalEU['Ventas Acumuladas $']-mbcTotalEU['Presupuesto Acumulado $'])
mbcTotalEU['Desvio Presupuestado $ %']=(mbcTotalEU['Ventas Acumuladas $']/mbcTotalEU['Presupuesto Acumulado $'])-1
mbcTotalEU=mbcTotalEU.fillna(0)
mbcTotalEU.loc["colTOTAL"]= pd.Series(
    mbcTotalEU.sum(numeric_only=True)
    , index=['Ventas Acumuladas $','Presupuesto Acumulado $','Presupuesto Acumulado L','Volumen Total Vendido']
)
mbcTotalEU.fillna({"UEN":"TOTAL"}, inplace=True)

tasa = (mbcTotalEU.loc["colTOTAL",'Volumen Total Vendido'] -
    mbcTotalEU.loc["colTOTAL",'Presupuesto Acumulado L'])
mbcTotalEU.fillna({"Desvio Presupuestado L":tasa}, inplace=True)

tasa2 = (mbcTotalEU.loc["colTOTAL",'Volumen Total Vendido'] /
    mbcTotalEU.loc["colTOTAL",'Presupuesto Acumulado L'])-1
mbcTotalEU.fillna({'Desvio Presupuestado L %':tasa2}, inplace=True)


tasa3 = (mbcTotalEU.loc["colTOTAL",'Ventas Acumuladas $'] -
    mbcTotalEU.loc["colTOTAL",'Presupuesto Acumulado $'])
mbcTotalEU.fillna({"Desvio Presupuestado $":tasa3}, inplace=True)

tasa4 = (mbcTotalEU.loc["colTOTAL",'Ventas Acumuladas $'] /
    mbcTotalEU.loc["colTOTAL",'Presupuesto Acumulado $'])-1
mbcTotalEU.fillna({'Desvio Presupuestado $ %':tasa4}, inplace=True)
mbcTotalEU = mbcTotalEU.loc[(mbcTotalEU['Presupuesto Acumulado $'] > 0),:]
mbcTotalEU=mbcTotalEU.reindex(columns=['UEN','Presupuesto Acumulado L','Volumen Total Vendido','Desvio Presupuestado L','Desvio Presupuestado L %','Presupuesto Acumulado $','Ventas Acumuladas $','Desvio Presupuestado $','Desvio Presupuestado $ %'])
######### LE DOY FORMATO AL DATAFRAME

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
            + ((tiempoInicio-pd.to_timedelta(2,"days")).strftime("%d/%m/%y"))
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
mbcTOTALGO = _estiladorVtaTituloD(mbcTOTALGO,EUmColspesos,EUmColslitros,percColsPen, "Ejecucion Presupuestaria Ultra Diesel")
mbcTotalEU = _estiladorVtaTituloD(mbcTotalEU,EUmColspesos,EUmColslitros,percColsPen, "Ejecucion Presupuestaria Infinia Diesel")

# = "C:/Informes/PRESUPUESTO/"
ubicacion= str(pathlib.Path(__file__).parent)+"\\"
nombreGO = "Info_Presupuesto_GO_Acumulado.png"
nombreEU = "Info_Presupuesto_EU_Acumulado.png"
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

df_to_image(mbcTOTALGO, ubicacion, nombreGO)
df_to_image(mbcTotalEU, ubicacion, nombreEU)



#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)



