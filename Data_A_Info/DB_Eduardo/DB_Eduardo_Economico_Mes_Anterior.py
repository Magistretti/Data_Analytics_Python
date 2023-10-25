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
import datetime

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
    
##### Fechas
# Obtener la fecha actual
fecha_actual = datetime.date.today()
# Obtener el primer día del mes actual
primer_dia_mes_actual = fecha_actual.replace(day=1)
# Obtener el último día del mes anterior
ultimo_dia_mes_anterior = primer_dia_mes_actual - datetime.timedelta(days=1)
# Obtener el primer día del mes anterior
primer_dia_mes_anterior = ultimo_dia_mes_anterior.replace(day=1)
# Obtener la fecha actual
fecha_actual = datetime.date.today()
# Obtener el mes actual
mes_actual = fecha_actual.month
# Obtener el año actual
anio_actual = fecha_actual.year
# Calcular el mes hace dos meses atrás
mes_dos_atras = mes_actual - 2
# Calcular el año dos meses atrás
if mes_dos_atras <= 0:
    mes_dos_atras += 12
    anio_dos_atras = anio_actual - 1
else:
    anio_dos_atras = anio_actual
# Obtener el primer día del mes dos meses atrás
primer_dia_dos_meses_atras = datetime.date(anio_dos_atras, mes_dos_atras, 1)


primer_dia_dos_meses_atras = primer_dia_dos_meses_atras
ultimo_dia_mes_anterior = ultimo_dia_mes_anterior
ultimo_dia_dos_mes_anterior=primer_dia_mes_anterior - datetime.timedelta(days=1)

hoy = datetime.datetime.now()
ayer = hoy - datetime.timedelta(days=1)

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

######### LECTURA DE EXCEL DE COSTOS DAPSA EU
ubicacion = "C:/Informes/Margen_Playa/"
aux_semanal = "TRAMOSyCOSTOSgasoleos.xlsx"
costoDapsaGO =pd.read_excel(ubicacion+aux_semanal, sheet_name= 'Costo Dapsa GO')
costoDapsaGO = costoDapsaGO.convert_dtypes()

######### LECTURA DE EXCEL DE COSTOS DAPSA EU
ubicacion = "C:/Informes/Margen_Playa/"
aux_semanal = "TRAMOSyCOSTOSgasoleos.xlsx"
costoDapsaEU =pd.read_excel(ubicacion+aux_semanal, sheet_name= 'Costo Dapsa EU')
costoDapsaEU = costoDapsaEU.convert_dtypes()



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
    WHERE FECHASQL >= '{primer_dia_dos_meses_atras.strftime("%Y-%d-%m")}'
        AND FECHASQL < '{primer_dia_mes_anterior.strftime("%Y-%d-%m")}'
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

df_gasoleosPlaya = pd.read_sql(f'''
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
    WHERE FECHASQL >= '{primer_dia_dos_meses_atras.strftime("%Y-%d-%m")}'
        AND FECHASQL < '{primer_dia_mes_anterior.strftime("%Y-%d-%m")}'
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
	SELECT emp.UEN,emp.FECHASQL,sum(emp.VOLUMEN) as 'Volumen REDMAS',emp.CODPRODUCTO
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL >= '{primer_dia_dos_meses_atras.strftime("%Y-%d-%m")}'
        AND FECHASQL < '{primer_dia_mes_anterior.strftime("%Y-%d-%m")}'
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

precioCartelypf=df_gasoleosTotal['Precio Cartel'].max()

df_gasoleosTotal = df_gasoleosTotal.fillna(0)

df_gasoleosTotal=df_gasoleosTotal.sort_values(['UEN','CODPRODUCTO','FECHASQL'])
 
gasoleos = df_gasoleosTotal
# Definir una función para aplicar la comisión acumulada según el tramo de ventas
def aplicar_comision_acumulada(df,estacion):
    df=df.sort_values(['UEN','FECHASQL'])
    comi = comisionGO.loc[(comisionGO["UEN"] == estacion),:]
    comi = comi.reset_index()
    t1=comi.loc[0,'COMISION TRAMO I GO']
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
                
    df['Comision $']= (df['COMISION']*df['Total Vendido $'])
    df['Comision YER $']=(df['Total Vendido YER $']*t1)
    df['Ventas Acumuladas $'] = df['Total Vendido $']
    df = df.reindex(columns=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $','Total Vendido YER $','Comision YER $'])
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
mbcGOYPF=aperdriel1.merge(aperdriel2,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $','Total Vendido YER $','Comision YER $'],how='outer')
mbcGOYPF=mbcGOYPF.merge(aazcuenaga,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $','Total Vendido YER $','Comision YER $'],how='outer')
mbcGOYPF=mbcGOYPF.merge(asan_Jose,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $','Total Vendido YER $','Comision YER $'],how='outer')
mbcGOYPF=mbcGOYPF.merge(apuente_Olive,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $','Total Vendido YER $','Comision YER $'],how='outer')
mbcGOYPF=mbcGOYPF.merge(alamadrid,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $','Total Vendido YER $','Comision YER $'],how='outer')



def aplicar_comision_acumuladaEU(df,estacion):
    df=df.sort_values(['UEN','FECHASQL'])
    comi = comisionEU.loc[(comisionEU["UEN"] == estacion),:]
    comi = comi.reset_index()
    df['Ventas Acumuladas']=df['Volumen Total Vendido'].cumsum()
    t1=comi.loc[0,'COMISION TRAMO I EU']
    t2=comi.loc[0,'COMISION TRAMO II EU']
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
                df.loc[r,'COMISION']=t2
                
    df['Comision $']= (df['COMISION']*df['Total Vendido $'])
    df['Comision YER $']=(df['Total Vendido YER $']*t1)
    df['Ventas Acumuladas $'] = df['Total Vendido $']
    df = df.reindex(columns=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $','Total Vendido YER $','Comision YER $'])
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
lamadrid= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'LAMADRID            ') & (df_gasoleosTotal["CODPRODUCTO"] == 'EU   '),:]
lamadrid = aplicar_comision_acumuladaEU(lamadrid,'LAMADRID            ')
### Creo tabla de YPF EU
mbcEUYPF=perdriel1.merge(perdriel2,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $','Total Vendido YER $','Comision YER $'],how='outer')
mbcEUYPF=mbcEUYPF.merge(azcuenaga,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $','Total Vendido YER $','Comision YER $'],how='outer')
mbcEUYPF=mbcEUYPF.merge(san_Jose,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $','Total Vendido YER $','Comision YER $'],how='outer')
mbcEUYPF=mbcEUYPF.merge(puente_Olive,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $','Total Vendido YER $','Comision YER $'],how='outer')
mbcEUYPF=mbcEUYPF.merge(lamadrid,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $','Total Vendido YER $','Comision YER $'],how='outer')

### Concateno Tabla De YPF GO con YPF EU
mbcYPF=mbcGOYPF.merge(mbcEUYPF,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $','Total Vendido YER $','Comision YER $'],how='outer')




##################################################
####### VENTAS DAPSA #############################
##################################################

####  Volumen diario GASOLEOS YPF 
df_gasoleosdapsa = pd.read_sql(f'''
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
    WHERE FECHASQL >= '{primer_dia_dos_meses_atras.strftime("%Y-%d-%m")}'
        AND FECHASQL < '{primer_dia_mes_anterior.strftime("%Y-%d-%m")}'
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
    '''
     ,db_conex)
df_gasoleosdapsa = df_gasoleosdapsa.convert_dtypes()
df_gasoleosdapsa =df_gasoleosdapsa.fillna(0)
### Descuentos
df_gasoleosdapsaDesc = pd.read_sql(f'''

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
    WHERE FECHASQL >= '{primer_dia_dos_meses_atras.strftime("%Y-%d-%m")}'
        AND FECHASQL < '{primer_dia_mes_anterior.strftime("%Y-%d-%m")}'
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

df_gasoleosdapsa = df_gasoleosdapsa.merge(df_gasoleosdapsaDesc, on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')
df_gasoleosdapsa =df_gasoleosdapsa.fillna(0)

df_gasoleosdapsa['Total Vendido $'] = ((df_gasoleosdapsa['Ventas Efectivo']*df_gasoleosdapsa['Precio Cartel'])
                                       +(df_gasoleosdapsa['Venta Cta Cte']*df_gasoleosdapsa['Precio Cta Cte'])
                                      +((df_gasoleosdapsa['ventas Promos']+df_gasoleosdapsa['Descuentos'])*df_gasoleosdapsa['Precio Cartel']))

df_gasoleosdapsa['Volumen Total Vendido']=(df_gasoleosdapsa['Ventas Efectivo']+df_gasoleosdapsa['Venta Cta Cte']
                                           +(df_gasoleosdapsa['ventas Promos']+df_gasoleosdapsa['Descuentos']))
precioCartelDapsa=df_gasoleosdapsa['Precio Cartel'].max()

import datetime


today = datetime.date.today()
first_day_last_month = primer_dia_dos_meses_atras.strftime("%Y-%m-%d")
last_day_last_month = ultimo_dia_dos_mes_anterior.strftime("%Y-%m-%d")



df_gasoleosdapsa=df_gasoleosdapsa.sort_values(['UEN','CODPRODUCTO','FECHASQL'])

costoDapsaGO= costoDapsaGO.loc[(costoDapsaGO["FECHASQL"] <= last_day_last_month) & (costoDapsaGO["FECHASQL"] >= first_day_last_month),:]

costoDapsaEU= costoDapsaEU.loc[(costoDapsaEU["FECHASQL"] <= last_day_last_month) & (costoDapsaEU["FECHASQL"] >= first_day_last_month),:]
## FUNCION PARA APLICAR COMISION GO

def aplicar_comision_acumuladaDAPSA(df):
    df=df.sort_values(['UEN','FECHASQL'])
    df = df.merge(costoDapsaGO,on=['FECHASQL','CODPRODUCTO'],how='outer')

    df['Comision']= df['Precio Cartel']-df['COSTO GO DAP']
    df['Comision $']= df['Comision']*df['Volumen Total Vendido']
    df['Ventas Acumuladas $'] = df['Volumen Total Vendido']*df['Precio Cartel']
    df = df.reindex(columns=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $'])
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
villa_nueva= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'VILLANUEVA          ') & (df_gasoleosdapsa["CODPRODUCTO"] == 'GO   '),:]
villa_nueva = aplicar_comision_acumuladaDAPSA(villa_nueva)
adolfo_Calle= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'ADOLFO CALLE        ') & (df_gasoleosdapsa["CODPRODUCTO"] == 'GO   '),:]
adolfo_Calle = aplicar_comision_acumuladaDAPSA(adolfo_Calle)
mitre= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'MITRE               ') & (df_gasoleosdapsa["CODPRODUCTO"] == 'GO   '),:]
mitre = aplicar_comision_acumuladaDAPSA(mitre)
urquiza= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'URQUIZA             ') & (df_gasoleosdapsa["CODPRODUCTO"] == 'GO   '),:]
urquiza = aplicar_comision_acumuladaDAPSA(urquiza)

### Creo tabla de YPF GO
mbcGODAPSA=las_heras.merge(amercado1,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $'],how='outer')
mbcGODAPSA=mbcGODAPSA.merge(amercado2,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $'],how='outer')
mbcGODAPSA=mbcGODAPSA.merge(sarmiento,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $'],how='outer')
mbcGODAPSA=mbcGODAPSA.merge(villa_nueva,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $'],how='outer')
mbcGODAPSA=mbcGODAPSA.merge(adolfo_Calle,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $'],how='outer')
mbcGODAPSA=mbcGODAPSA.merge(mitre,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $'],how='outer')
mbcGODAPSA=mbcGODAPSA.merge(urquiza,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $'],how='outer')

## FUNCION PARA APLICAR COMISION EU
def aplicar_comision_acumuladaDAPSAEU(df):
    df=df.sort_values(['UEN','FECHASQL'])
    df = df.merge(costoDapsaEU,on=['FECHASQL','CODPRODUCTO'],how='outer')

    df['Comision']= df['Precio Cartel']-df['COSTO EU DAP']
    df['Comision $']= df['Comision']*df['Volumen Total Vendido']
    df['Ventas Acumuladas $'] = df['Volumen Total Vendido']*df['Precio Cartel']
    df = df.reindex(columns=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $'])
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
mbcGODAPSAEU=mercado1.merge(mercado2,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $'],how='outer')
### Concateno Tablas de Dapsa GO y EU
mbcDapsa=mbcGODAPSA.merge(mbcGODAPSAEU,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $'],how='outer')

### Concateno tablas de YPF y Dapsa TOTALES
mbcTOTAL = mbcYPF.merge(mbcDapsa,on=['UEN','CODPRODUCTO','Ventas Acumuladas $','Comision $'],how='outer')



## Creo Dataframe de GO
go = mbcTOTAL['CODPRODUCTO'] == "GO   "
mbcTOTALGO = mbcTOTAL[go]
mbcTOTALGO = mbcTOTALGO.rename({'Comision $':'MBC Acumulado $'},axis=1)
mbcTOTALGO = mbcTOTALGO.reindex(columns=['UEN','Ventas Acumuladas $','MBC Acumulado $','Total Vendido YER $','Comision YER $'])
## Creo Dataframe de EU
eu = mbcTOTAL['CODPRODUCTO'] == "EU   "
mbcTotalEU = mbcTOTAL[eu]
mbcTotalEU = mbcTotalEU.rename({'Comision $':'MBC Acumulado $'},axis=1)
mbcTotalEU=mbcTotalEU.reindex(columns=['UEN','Ventas Acumuladas $','MBC Acumulado $','Total Vendido YER $','Comision YER $'])

###### Columnas de Desvio y Totales GO


mbcTOTALGO=mbcTOTALGO.reindex(columns=['UEN','Ventas Acumuladas $','MBC Acumulado $','Total Vendido YER $','Comision YER $'])

###### Columnas de Desvio y Totales EU

mbcTotalEU=mbcTotalEU.reindex(columns=['UEN','Ventas Acumuladas $','MBC Acumulado $','Total Vendido YER $','Comision YER $'])


# Varialbes para reporte general

mbcGasoleosGO=mbcTOTALGO
mbcGasoleosGO=mbcGasoleosGO.fillna(0)
mbcGasoleosGO['MBC Acumulado $']=mbcGasoleosGO['MBC Acumulado $']+mbcGasoleosGO['Comision YER $']
mbcGasoleosGO['Ventas Acumuladas $']=mbcGasoleosGO['Ventas Acumuladas $']+mbcGasoleosGO['Total Vendido YER $']

mbcGasoleosEU=mbcTotalEU
mbcGasoleosEU=mbcGasoleosEU.fillna(0)
mbcGasoleosEU['MBC Acumulado $']=mbcGasoleosEU['MBC Acumulado $']+mbcGasoleosEU['Comision YER $']
mbcGasoleosEU['Ventas Acumuladas $']=mbcGasoleosEU['Ventas Acumuladas $']+mbcGasoleosEU['Total Vendido YER $']

