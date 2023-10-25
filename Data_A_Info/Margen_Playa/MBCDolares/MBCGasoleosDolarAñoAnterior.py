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


######### LECTURA DE EXCEL DE COMISIONES Y TRAMOS GO
ubicacion = "C:/Informes/Margen_Playa/"
aux_semanal = "TRAMOSyCOSTOSgasoleos.xlsx"
comisionGO =pd.read_excel(ubicacion+aux_semanal, sheet_name= 'Comision y Tramo GO')
comisionGO = comisionGO.convert_dtypes()

######### LECTURA DE EXCEL DE COMISIONES Y TRAMOS EU
ubicacion = "C:/Informes/Margen_Playa/"
aux_semanal = "TRAMOSyCOSTOSgasoleos.xlsx"
comisionEU =pd.read_excel(ubicacion+aux_semanal, sheet_name= 'Comision y Tramo EU')
comisionEU = comisionEU.convert_dtypes()

######### LECTURA DE EXCEL DE COSTOS DAPSA
ubicacion = "C:/Informes/Margen_Playa/"
aux_semanal = "CostoDapsa2022.xlsx"
costoDapsa =pd.read_excel(ubicacion+aux_semanal, sheet_name= 'Hoja1')
costoDapsa = costoDapsa.convert_dtypes()
costoDapsa= costoDapsa.loc[costoDapsa["FECHASQL"] >= inicio_mes_pasado.strftime('%Y-%m-%d'),:]
costoDapsa= costoDapsa.loc[costoDapsa["FECHASQL"] <= ultimo_dia_mes_pasado.strftime('%Y-%m-%d'),:]

# Convertir la columna de fecha en formato de fecha
costoDapsa['FECHASQL'] = pd.to_datetime(costoDapsa['FECHASQL'])

# Eliminar las filas duplicadas en la columna de fecha
costoDapsa = costoDapsa.drop_duplicates(subset='FECHASQL', keep='first')

# Establecer la columna de fecha como índice
costoDapsa = costoDapsa.set_index('FECHASQL')

# Reordenar el dataframe con todas las fechas del mes
costoDapsa = costoDapsa.resample('D').ffill()

# Restablecer la columna de fecha como una columna del dataframe
costoDapsa = costoDapsa.reset_index()

costoDapsa = costoDapsa.reindex(columns=['FECHASQL','COSTO GO','COSTO EU'])
######### Precio del Dolar ########
ubicacion = "C:/Informes/Margen_Playa/"
aux_semanal = "PrecioDolar.xlsx"
precioDolar =pd.read_excel(ubicacion+aux_semanal, sheet_name= 'Hoja1')
precioDolar = precioDolar.convert_dtypes()
precioDolar= precioDolar.loc[precioDolar["FECHASQL"] >= inicio_mes_pasado.strftime('%Y-%m-%d'),:]
precioDolar= precioDolar.loc[precioDolar["FECHASQL"] <= ultimo_dia_mes_pasado.strftime('%Y-%m-%d'),:]
# Convertir la columna de fecha a formato datetime
precioDolar['FECHASQL'] = pd.to_datetime(precioDolar['FECHASQL'])

# Ordenar el dataframe por fecha
precioDolar = precioDolar.sort_values('FECHASQL')

# Eliminar las filas duplicadas de acuerdo a la columna de fecha
precioDolar = precioDolar.drop_duplicates(subset='FECHASQL', keep='last')

# Reiniciar el índice del dataframe
precioDolar = precioDolar.reset_index(drop=True)

# Establecer la columna de fecha como índice del dataframe
precioDolar = precioDolar.set_index('FECHASQL')

# Obtener el rango completo de fechas de marzo del año actual
fechas_completas = pd.date_range(start= inicio_mes_pasado.strftime('%Y-%m-%d'), end= ultimo_dia_mes_pasado.strftime('%Y-%m-%d'))

# Reindexar el dataframe con el rango completo de fechas
precioDolar = precioDolar.reindex(fechas_completas)

# Completar los valores faltantes usando el método 'ffill'
precioDolar = precioDolar.fillna(method='ffill')

# Reiniciar el índice del dataframe
precioDolar = precioDolar.reset_index()

# Renombrar la columna de fecha
precioDolar = precioDolar.rename(columns={'index': 'FECHASQL'})

precioDolar= precioDolar.loc[precioDolar["FECHASQL"] == ultimo_dia_mes_pasado.strftime('%Y-%m-%d'),:]

precioDolar=precioDolar['Compra']

precioDolar=int(precioDolar)





################################################
############# Volumen diario GASOLEOS YPF 
################################################

df_gasoleosTotal = pd.read_sql('''
    -- Variables de fecha para inicio y fin de mes del año pasado
DECLARE @inicio_mes_pasado DATE, @fin_mes_pasado DATE

-- Obtener el primer día del mes actual del año pasado
SET @inicio_mes_pasado = DATEADD(YEAR, -1, DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0))

-- Obtener el último día del mes actual del año pasado
SET @fin_mes_pasado = DATEADD(YEAR, -1, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE()), -1))
        
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
    WHERE FECHASQL >= @inicio_mes_pasado
        AND FECHASQL <= @fin_mes_pasado
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
    -- Variables de fecha para inicio y fin de mes del año pasado
DECLARE @inicio_mes_pasado DATE, @fin_mes_pasado DATE

-- Obtener el primer día del mes actual del año pasado
SET @inicio_mes_pasado = DATEADD(YEAR, -1, DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0))

-- Obtener el último día del mes actual del año pasado
SET @fin_mes_pasado = DATEADD(YEAR, -1, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE()), -1))
        
	SELECT emp.UEN,emp.FECHASQL,sum(emp.VOLUMEN) as 'Volumen YER',emp.CODPRODUCTO
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL >= @inicio_mes_pasado
        AND FECHASQL <= @fin_mes_pasado
		and emp.UEN IN (
            'LAMADRID'
            ,'AZCUENAGA'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
            ,'SAN JOSE'
        )
		and (emp.CODPRODUCTO = 'GO' OR emp.CODPRODUCTO = 'EU')
        AND  P.[DESCRIPCION] like '%ruta%'
		group by emp.FECHASQL,emp.CODPRODUCTO,emp.UEN
		order by CODPRODUCTO,UEN
'''      ,db_conex)
df_gasoleosPlaya = df_gasoleosPlaya.convert_dtypes()


############################################
#####Volumen Diario GASOLEOS REDMAS APPYPF######
############################################

df_gasoleosREDMAS = pd.read_sql('''
    -- Variables de fecha para inicio y fin de mes del año pasado
DECLARE @inicio_mes_pasado DATE, @fin_mes_pasado DATE

-- Obtener el primer día del mes actual del año pasado
SET @inicio_mes_pasado = DATEADD(YEAR, -1, DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0))

-- Obtener el último día del mes actual del año pasado
SET @fin_mes_pasado = DATEADD(YEAR, -1, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE()), -1))
        
	SELECT emp.UEN,emp.FECHASQL,sum(emp.VOLUMEN) as 'Volumen REDMAS',emp.CODPRODUCTO,MAX(emp.PRECIO) AS 'Precio Promos'
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL >= @inicio_mes_pasado
        AND FECHASQL <= @fin_mes_pasado
		and emp.UEN IN (
            'LAMADRID'
            ,'AZCUENAGA'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
            ,'SAN JOSE'
        )
		and (emp.CODPRODUCTO = 'GO' OR emp.CODPRODUCTO = 'EU')
        AND  (P.[DESCRIPCION] like '%PROMO%' OR P.DESCRIPCION LIKE '%MERCO%' OR P.DESCRIPCION LIKE '%MAS%')
		group by emp.FECHASQL,emp.CODPRODUCTO,emp.UEN
		order by CODPRODUCTO,UEN
'''      ,db_conex)
df_gasoleosREDMAS = df_gasoleosREDMAS.convert_dtypes()

df_gasoleosTotal=df_gasoleosTotal.merge(df_gasoleosREDMAS,on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')
df_gasoleosTotal=df_gasoleosTotal.merge(df_gasoleosPlaya,on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')
df_gasoleosTotal = df_gasoleosTotal.fillna(0)

df_gasoleosTotal['Volumen Total Vendido']=(df_gasoleosTotal['Ventas Efectivo']+df_gasoleosTotal['Venta Cta Cte']
                                           +df_gasoleosTotal['Volumen REDMAS'])


df_gasoleosTotal = df_gasoleosTotal.reindex(columns=['UEN','FECHASQL','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Precio Cartel'])
df_gasoleosTotal = df_gasoleosTotal.fillna(0)
df_gasoleosTotal= df_gasoleosTotal.loc[df_gasoleosTotal["FECHASQL"] <= ayer.strftime('%Y-%m-%d'),:]

df_gasoleosTotal=df_gasoleosTotal.sort_values(['UEN','CODPRODUCTO','FECHASQL'])
costoGasoleosYPF=df_gasoleosTotal

# Definir una función para aplicar la comisión acumulada según el tramo de ventas
def aplicar_comision_acumulada(df,estacion):
    df=df.sort_values(['UEN','FECHASQL'])
    comi = comisionGO.loc[(comisionGO["UEN"] == estacion),:]
    comi = comi.reset_index()
    df['Ventas Acumuladas']=df['Volumen Total Vendido'].cumsum()

    for i in df.index:
        if df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO I GO']:
            df.loc[i,'Marcador'] = 1
        else:
            if df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO II GO']:
                df.loc[i,'Marcador'] = 2

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
    
    df['Comision USD']=   (df['Volumen Total Vendido']*(df['Precio Cartel']/precioDolar)*df['COMISION'])+(df['Volumen YER']*(df['Precio Cartel']/precioDolar)*comi.loc[0,'COMISION TRAMO I GO'])
    df['Total Vendido USD']=   (df['Volumen Total Vendido']*(df['Precio Cartel']/precioDolar))+(df['Volumen YER']*(df['Precio Cartel']/precioDolar))
    df = df.reindex(columns=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'])
    df = df.groupby(
        ["UEN",'CODPRODUCTO']
        , as_index=False
    ).sum()
    return df

### Aplico la comision a cada estacion para el producto GO
aperdriel1= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'PERDRIEL            ') & (df_gasoleosTotal["CODPRODUCTO"] == 'GO   '),:]
aperdriel1 = aplicar_comision_acumulada(aperdriel1,'PERDRIEL            ')
aperdriel2= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'PERDRIEL2           ') & (df_gasoleosTotal["CODPRODUCTO"] == 'GO   '),:]
aperdriel2 = aplicar_comision_acumulada(aperdriel2,'PERDRIEL2           ')
aazcuenaga= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'AZCUENAGA           ') & (df_gasoleosTotal["CODPRODUCTO"] == 'GO   '),:]
aazcuenaga = aplicar_comision_acumulada(aazcuenaga,'AZCUENAGA           ')
asan_Jose= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'SAN JOSE            ') & (df_gasoleosTotal["CODPRODUCTO"] == 'GO   '),:]
asan_Jose = aplicar_comision_acumulada(asan_Jose,'SAN JOSE            ')
apuente_Olive= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'PUENTE OLIVE        ') & (df_gasoleosTotal["CODPRODUCTO"] == 'GO   '),:]
apuente_Olive = aplicar_comision_acumulada(apuente_Olive,'PUENTE OLIVE        ')
alamadrid= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'LAMADRID            ') & (df_gasoleosTotal["CODPRODUCTO"] == 'GO   '),:]
alamadrid = aplicar_comision_acumulada(alamadrid,'LAMADRID            ')

### Creo tabla de YPF GO
mbcGOYPF=aperdriel1.merge(aperdriel2,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')
mbcGOYPF=mbcGOYPF.merge(aazcuenaga,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')
mbcGOYPF=mbcGOYPF.merge(asan_Jose,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')
mbcGOYPF=mbcGOYPF.merge(apuente_Olive,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')
mbcGOYPF=mbcGOYPF.merge(alamadrid,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')


def aplicar_comision_acumuladaEU(df,estacion):
    df=df.sort_values(['UEN','FECHASQL'])
    comi = comisionEU.loc[(comisionEU["UEN"] == estacion),:]
    comi = comi.reset_index()
    df['Ventas Acumuladas']=df['Volumen Total Vendido'].cumsum()
    for i in df.index:
        if df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO I EU']:
            df.loc[i,'Marcador'] = 1
        else:
            if df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO II EU']:
                df.loc[i,'Marcador'] = 2


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

    df['Comision USD']=   (df['Volumen Total Vendido']*(df['Precio Cartel']/precioDolar)*df['COMISION'])+(df['Volumen YER']*(df['Precio Cartel']/precioDolar)*comi.loc[0,'COMISION TRAMO I EU'])
    df['Total Vendido USD']=   (df['Volumen Total Vendido']*(df['Precio Cartel']/precioDolar))+(df['Volumen YER']*(df['Precio Cartel']/precioDolar))
    df = df.reindex(columns=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'])
    df = df.groupby(
        ["UEN",'CODPRODUCTO']
        , as_index=False
    ).sum()
    return df



### Aplico la comision a cada estacion para el producto EU
perdriel1= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'PERDRIEL            ') & (df_gasoleosTotal["CODPRODUCTO"] == 'EU   '),:]
perdriel1 = aplicar_comision_acumuladaEU(perdriel1,'PERDRIEL            ')
perdriel2= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'PERDRIEL2           ') & (df_gasoleosTotal["CODPRODUCTO"] == 'EU   '),:]
perdriel2 = aplicar_comision_acumuladaEU(perdriel2,'PERDRIEL2           ')
azcuenaga= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'AZCUENAGA           ') & (df_gasoleosTotal["CODPRODUCTO"] == 'EU   '),:]
azcuenaga = aplicar_comision_acumuladaEU(azcuenaga,'AZCUENAGA           ')
san_Jose= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'SAN JOSE            ') & (df_gasoleosTotal["CODPRODUCTO"] == 'EU   '),:]
san_Jose = aplicar_comision_acumuladaEU(san_Jose,'SAN JOSE            ')
puente_Olive= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'PUENTE OLIVE        ') & (df_gasoleosTotal["CODPRODUCTO"] == 'EU   '),:]
puente_Olive = aplicar_comision_acumuladaEU(puente_Olive,'PUENTE OLIVE        ')

puente_Olive['Comision USD'] = 9151

lamadrid= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'LAMADRID            ') & (df_gasoleosTotal["CODPRODUCTO"] == 'EU   '),:]
lamadrid = aplicar_comision_acumuladaEU(lamadrid,'LAMADRID            ')

### Creo tabla de YPF EU
mbcEUYPF=perdriel1.merge(perdriel2,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')
mbcEUYPF=mbcEUYPF.merge(azcuenaga,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')
mbcEUYPF=mbcEUYPF.merge(san_Jose,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')
mbcEUYPF=mbcEUYPF.merge(puente_Olive,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')
mbcEUYPF=mbcEUYPF.merge(lamadrid,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')

### Concateno Tabla De YPF GO con YPF EU
mbcYPF=mbcGOYPF.merge(mbcEUYPF,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Volumen YER','Comision USD','Total Vendido USD'],how='outer')

##################################################
####### VENTAS DAPSA #############################
##################################################

# Obtener la fecha actual
fecha_actual = datetime.datetime.now()
# Crear una nueva fecha con el primer día del mes actual
primer_dia_mes = datetime.datetime(fecha_actual.year, fecha_actual.month, 1)

####  Volumen diario GASOLEOS YPF 
df_gasoleosdapsa = pd.read_sql(''' 
    -- Variables de fecha para inicio y fin de mes del año pasado
DECLARE @inicio_mes_pasado DATE, @fin_mes_pasado DATE

-- Obtener el primer día del mes actual del año pasado
SET @inicio_mes_pasado = DATEADD(YEAR, -1, DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0))

-- Obtener el último día del mes actual del año pasado
SET @fin_mes_pasado = DATEADD(YEAR, -1, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE()), -1))
        
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
    WHERE FECHASQL >= @inicio_mes_pasado
        AND FECHASQL <= @fin_mes_pasado
	    AND VTATOTVOL > '0'
		and (CODPRODUCTO = 'GO' OR CODPRODUCTO = 'EU')
		and UEN IN (
            'SARMIENTO'
            ,'URQUIZA'
            ,'MITRE'
            ,'MERC GUAYMALLEN'
			,'ADOLFO CALLE'
			,'VILLAEUEVA'
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

    -- Variables de fecha para inicio y fin de mes del año pasado
DECLARE @inicio_mes_pasado DATE, @fin_mes_pasado DATE

-- Obtener el primer día del mes actual del año pasado
SET @inicio_mes_pasado = DATEADD(YEAR, -1, DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0))

-- Obtener el último día del mes actual del año pasado
SET @fin_mes_pasado = DATEADD(YEAR, -1, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE()), -1))
        
	SELECT emp.UEN,EmP.FECHASQL
        ,EmP.[CODPRODUCTO]
        ,SUM(-EmP.[VOLUMEN]) as 'Descuentos'
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL >= @inicio_mes_pasado
        AND FECHASQL <= @fin_mes_pasado
		and emp.UEN IN (
            'SARMIENTO'
            ,'URQUIZA'
            ,'MITRE'
            ,'MERC GUAYMALLEN'
			,'ADOLFO CALLE'
			,'VILLAEUEVA'
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



df_gasoleosdapsa = df_gasoleosdapsa.reindex(columns=['UEN','FECHASQL','CODPRODUCTO','Volumen Total Vendido','Total Vendido $','Total Vendido YER $','Precio Cartel'])
df_gasoleosdapsa = df_gasoleosdapsa.fillna(0)

df_gasoleosdapsa=df_gasoleosdapsa.sort_values(['UEN','CODPRODUCTO','FECHASQL'])


## FUNCION PARA APLICAR COMISION GO

def aplicar_comision_acumuladaDAPSA(df):
    df=df.sort_values(['UEN','FECHASQL'])
    df = df.merge(costoDapsa,on=['FECHASQL'],how='outer')

    df['Comision']= (df['Precio Cartel']-df['COSTO GO'])/precioDolar
    df['Comision USD']= df['Comision']*df['Volumen Total Vendido']
    df['Total Vendido USD'] = (df['Volumen Total Vendido']*df['Precio Cartel'])/precioDolar
    df = df.reindex(columns=['UEN','CODPRODUCTO','Volumen Total Vendido','Total Vendido USD','Comision USD'])
    df = df.groupby(
        ["UEN",'CODPRODUCTO']
        , as_index=False
    ).sum()
    return df

### Aplico la comision a cada estacion para el producto GO
las_heras= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'LAS HERAS           ') & (df_gasoleosdapsa["CODPRODUCTO"] == 'GO   '),:]
las_heras = aplicar_comision_acumuladaDAPSA(las_heras)
amercado1= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'MERC GUAYMALLEN     ') & (df_gasoleosdapsa["CODPRODUCTO"] == 'GO   '),:]
amercado1 = aplicar_comision_acumuladaDAPSA(amercado1)
amercado2= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'MERCADO 2           ') & (df_gasoleosdapsa["CODPRODUCTO"] == 'GO   '),:]
amercado2 = aplicar_comision_acumuladaDAPSA(amercado2)
sarmiento= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'SARMIENTO           ') & (df_gasoleosdapsa["CODPRODUCTO"] == 'GO   '),:]
sarmiento = aplicar_comision_acumuladaDAPSA(sarmiento)
villa_nueva= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'VILLAEUEVA          ') & (df_gasoleosdapsa["CODPRODUCTO"] == 'GO   '),:]
villa_nueva = aplicar_comision_acumuladaDAPSA(villa_nueva)
adolfo_Calle= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'ADOLFO CALLE        ') & (df_gasoleosdapsa["CODPRODUCTO"] == 'GO   '),:]
adolfo_Calle = aplicar_comision_acumuladaDAPSA(adolfo_Calle)
mitre= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'MITRE               ') & (df_gasoleosdapsa["CODPRODUCTO"] == 'GO   '),:]
mitre = aplicar_comision_acumuladaDAPSA(mitre)
urquiza= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'URQUIZA             ') & (df_gasoleosdapsa["CODPRODUCTO"] == 'GO   '),:]
urquiza = aplicar_comision_acumuladaDAPSA(urquiza)


### Creo tabla de YPF GO
mbcGODAPSA=las_heras.merge(amercado1,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Total Vendido USD','Comision USD'],how='outer')
mbcGODAPSA=mbcGODAPSA.merge(amercado2,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Total Vendido USD','Comision USD'],how='outer')
mbcGODAPSA=mbcGODAPSA.merge(sarmiento,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Total Vendido USD','Comision USD'],how='outer')
mbcGODAPSA=mbcGODAPSA.merge(villa_nueva,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Total Vendido USD','Comision USD'],how='outer')
mbcGODAPSA=mbcGODAPSA.merge(adolfo_Calle,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Total Vendido USD','Comision USD'],how='outer')
mbcGODAPSA=mbcGODAPSA.merge(mitre,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Total Vendido USD','Comision USD'],how='outer')
mbcGODAPSA=mbcGODAPSA.merge(urquiza,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Total Vendido USD','Comision USD'],how='outer')



## FUNCION PARA APLICAR COMISION EU
def aplicar_comision_acumuladaDAPSAEU(df):
    df=df.sort_values(['UEN','FECHASQL'])
    df = df.merge(costoDapsa,on=['FECHASQL'],how='outer')

    df['Comision']= (df['Precio Cartel']-df['COSTO EU'])/precioDolar
    df['Comision USD']= df['Comision']*df['Volumen Total Vendido']
    df['Total Vendido USD'] = (df['Volumen Total Vendido']*df['Precio Cartel'])/precioDolar
    df = df.reindex(columns=['UEN','CODPRODUCTO','Volumen Total Vendido','Total Vendido USD','Comision USD'])
    df = df.groupby(
        ["UEN",'CODPRODUCTO']
        , as_index=False
    ).sum()
    return df

### Aplico la comision a cada estacion para el producto EU

mercado1= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'MERC GUAYMALLEN     ') & (df_gasoleosdapsa["CODPRODUCTO"] == 'EU   '),:]
mercado1 = aplicar_comision_acumuladaDAPSAEU(mercado1)
mercado2= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'MERCADO 2           ') & (df_gasoleosdapsa["CODPRODUCTO"] == 'EU   '),:]
mercado2 = aplicar_comision_acumuladaDAPSAEU(mercado2)

### Creo tabla de DAPSA EU
mbcGODAPSAEU=mercado1.merge(mercado2,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Total Vendido USD','Comision USD'],how='outer')
### Concateno Tablas de Dapsa GO y EU
mbcDapsa=mbcGODAPSA.merge(mbcGODAPSAEU,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Total Vendido USD','Comision USD'],how='outer')

### Concateno tablas de YPF y Dapsa TOTALES
mbcTOTAL = mbcYPF.merge(mbcDapsa,on=['UEN','CODPRODUCTO','Volumen Total Vendido','Total Vendido USD','Comision USD'],how='outer')



## Creo Dataframe de GO
go = mbcTOTAL['CODPRODUCTO'] == "GO   "
mbcTOTALGO = mbcTOTAL[go]
mbcTOTALGO = mbcTOTALGO.reindex(columns=['UEN','Volumen Total Vendido','Volumen YER','Total Vendido USD','Comision USD'])
## Creo Dataframe de EU
eu = mbcTOTAL['CODPRODUCTO'] == "EU   "
mbcTotalEU = mbcTOTAL[eu]
mbcTotalEU=mbcTotalEU.reindex(columns=['UEN','Volumen Total Vendido','Volumen YER','Total Vendido USD','Comision USD'])
mbcTOTALGO=mbcTOTALGO.fillna(0)
mbcTotalEU=mbcTotalEU.fillna(0)

mbcTotalGOAñoant=mbcTOTALGO
mbcTOTALEUAñoant=mbcTotalEU
