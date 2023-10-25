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

## Ventas ACumuladas
df_VentasAcum = pd.read_sql('''
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
	select UEN, sum(VTATOTVOL) as 'Ventas Acum', CODPRODUCTO from EmpVenta 
    where FECHASQL >= @inicioMesActual 
	and FECHASQL < @hoy 
    and CODPRODUCTO not like 'GNC'
    and CODPRODUCTO not like 'NN'
    group by UEN, CODPRODUCTO
  '''      ,db_conex)
df_VentasAcum = df_VentasAcum.convert_dtypes()
## Regalos acmulados
df_regalosTrasladosAcum = pd.read_sql("""
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
SELECT
        EmP.[UEN]
        ,EmP.[CODPRODUCTO]
        ,sum(-EmP.[VOLUMEN]) as VTATOTVOL
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL >= @inicioMesActual 
	and FECHASQL < @hoy 
        AND EmP.VOLUMEN > '0' 
        AND (EmP.[CODPROMO] = '30'
            OR P.[DESCRIPCION] like '%PRUEBA%'
            OR P.[DESCRIPCION] like '%TRASLADO%'
            OR P.[DESCRIPCION] like '%MAYORISTA%'
        )
    group by emp.UEN, emp.CODPRODUCTO
""", db_conex)
df_regalosTrasladosAcum = df_regalosTrasladosAcum.convert_dtypes()
df_VentasAcum = df_VentasAcum.merge(df_regalosTrasladosAcum,on=['UEN','CODPRODUCTO'],how='outer')
df_VentasAcum = df_VentasAcum.fillna(0)
df_VentasAcum['Ventas Acumuladas']=df_VentasAcum['Ventas Acum'] + df_VentasAcum['VTATOTVOL']
df_VentasAcum=df_VentasAcum.reindex(columns=['UEN','CODPRODUCTO','Ventas Acumuladas'])


####### Volumen Vendido el dia de hoy
df_VentasHoy = pd.read_sql('''
    select UEN, sum(VTATOTVOL) as 'Ventas Ho', CODPRODUCTO from EmpVenta 
    where FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date)) 
    and CODPRODUCTO not like 'GNC'
    and CODPRODUCTO not like 'NN'
    group by UEN, CODPRODUCTO
    order by UEN
  '''      ,db_conex)
df_VentasHoy = df_VentasHoy.convert_dtypes()

## Regalos
df_regalosTraslados = pd.read_sql("""
    SELECT
        EmP.[UEN]
        ,EmP.[CODPRODUCTO]
        ,sum(-EmP.[VOLUMEN]) as VTATOTVOL
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
        AND EmP.VOLUMEN > '0' 
        AND (EmP.[CODPROMO] = '30'
            OR P.[DESCRIPCION] like '%PRUEBA%'
            OR P.[DESCRIPCION] like '%TRASLADO%'
            OR P.[DESCRIPCION] like '%MAYORISTA%'
        )
    group by emp.UEN, emp.CODPRODUCTO
    order by UEN
""", db_conex)
df_regalosTraslados = df_regalosTraslados.convert_dtypes()
df_VentasHoy = df_VentasHoy.merge(df_regalosTraslados,on=['UEN','CODPRODUCTO'],how='outer')
df_VentasHoy = df_VentasHoy.merge(df_VentasAcum,on=['UEN','CODPRODUCTO'],how='outer')
df_VentasHoy = df_VentasHoy.fillna(0)
df_VentasHoy['Ventas Hoy']=df_VentasHoy['Ventas Ho'] + df_VentasHoy['VTATOTVOL']

df_VentasHoy = df_VentasHoy.loc[:,df_VentasHoy.columns!='VTATOTVOL']
df_VentasHoy = df_VentasHoy.loc[:,df_VentasHoy.columns!='Ventas Ho']


####### Volumen Vendido en el Turno 3 Hoy
df_VolFinTurn3Hoy = pd.read_sql('''
select UEN,CODPRODUCTO, SUM(VOLFINAL-VOLFINAGUA) AS 'Volumen Turno 3', SUM(VOLFINAL) AS VOLFINAL, SUM(VOLFINAGUA) AS VOLFINAGUA  from VOLTURNOS 
where TURNO = 3  
AND codproducto <> 'GNC' 
and CODPRODUCTO not like 'NN'
AND  FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
GROUP BY UEN, CODPRODUCTO
ORDER BY UEN

  '''      ,db_conex)
df_VolFinTurn3Hoy = df_VolFinTurn3Hoy.convert_dtypes()

####### Volumen vendido en el Turno 3 Ayer
df_VolFinTurn3Ayer = pd.read_sql('''
select UEN,CODPRODUCTO, SUM(VOLFINAL-VOLFINAGUA) AS 'Volumen Turno 3 Ayer', SUM(VOLFINAL) AS VOLFINALAY, SUM(VOLFINAGUA) AS VOLFINAGUAAY  from VOLTURNOS 
where TURNO = 3  
AND codproducto <> 'GNC' 
and CODPRODUCTO not like 'NN'
AND  FECHASQL = DATEADD(day, -2, CAST(GETDATE() AS date))
GROUP BY UEN, CODPRODUCTO
ORDER BY UEN

  '''      ,db_conex)
df_VolFinTurn3Ayer = df_VolFinTurn3Ayer.convert_dtypes()

######## TURNO 1 HOY VOLUMEN


df_VolumTurno1 = pd.read_sql('''
select UEN,CODPRODUCTO, SUM(VOLFINAL-VOLFINAGUA) AS 'Volumen Turno 1', SUM(VOLFINAL) AS VOLFINALAY, SUM(VOLFINAGUA) AS VOLFINAGUAAY  from VOLTURNOS 
where TURNO = 1  
AND codproducto <> 'GNC' 
and CODPRODUCTO not like 'NN'
AND  FECHASQL = DATEADD(day, 0, CAST(GETDATE() AS date))
GROUP BY UEN, CODPRODUCTO
ORDER BY UEN
  '''      ,db_conex)
df_VolumTurno1 = df_VolumTurno1.convert_dtypes()


######## Descargas de Combustible
df_Descargas = pd.read_sql('''
select UEN, CODPRODUCTO, sum(VOLPRECARTEL) as VOLPRECARTEL ,sum(VOLUMENCT) as DESCARGAS,sum(VOLUMENVR) as VOLUMENVR,sum(VOLUMENCEM) as VOLUMENCEM
from RemDetalle 
where  
codproducto <> 'GNC' 
and CODPRODUCTO not like 'NN'
AND  FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
group by UEN, CODPRODUCTO
order by UEN

  '''      ,db_conex)
df_Descargas = df_Descargas.convert_dtypes()

#### Concateno tablas de ventas , volumen turno 3 y descargas
consistencia = df_VentasHoy.merge(df_VolFinTurn3Ayer, on=['UEN','CODPRODUCTO'],how='outer')

consistencia = consistencia.merge(df_Descargas, on=['UEN','CODPRODUCTO'],how='outer')
consistencia = consistencia.merge(df_VolumTurno1,on=['UEN','CODPRODUCTO'],how='outer')
consistencia = consistencia.fillna(0)
# Creo columna Consistencia
consistencia['Consistencia'] = consistencia['Volumen Turno 3 Ayer']+consistencia['DESCARGAS']- consistencia['Ventas Hoy']
#concateno Tablas
consistencia = consistencia.merge(df_VolFinTurn3Hoy, on=['UEN','CODPRODUCTO'],how='outer')
consistencia = consistencia.fillna(0)
### Creo columnas 
consistencia['Desvio'] = consistencia['Consistencia'] - consistencia['Volumen Turno 3']
consistencia['Descargas Consistencia'] = consistencia['DESCARGAS'] - consistencia ['VOLUMENVR']
consistencia['DesvioDescargas'] = consistencia['DESCARGAS'] - consistencia ['VOLUMENVR']

###### Cuando el desvio sea mayor a 500L Escribir X 

consistencia['Descarga Consistencia'] = 'hola'
consistencia.loc[consistencia.DesvioDescargas<500,'Descarga Consistencia']='Ok'
consistencia.loc[consistencia.DesvioDescargas>=500,'Descarga Consistencia']='X'
consistencia.loc[consistencia.DesvioDescargas<=-500,'Descarga Consistencia']='X'

########## Elimino Columnas que no van a entrar en el reporte

consistencia = consistencia.loc[:,consistencia.columns!="VOLUMENVR"]
consistencia = consistencia.loc[:,consistencia.columns!="VOLUMENCEM"]
consistencia = consistencia.loc[:,consistencia.columns!="VOLFINALAY"]
consistencia = consistencia.loc[:,consistencia.columns!="VOLFINAGUA"]
consistencia = consistencia.loc[:,consistencia.columns!="VOLFINAL"]
consistencia = consistencia.loc[:,consistencia.columns!="VOLFINAGUAAY"]
consistencia = consistencia.loc[:,consistencia.columns!="VOLPRECARTEL"]

######### Ordeno por UEN
consistencia['BANDERA']=''
for i in consistencia.index:
    if consistencia.loc[i,'UEN']== 'PERDRIEL            ':
        consistencia.loc[i,'BANDERA']='YPF'
    elif consistencia.loc[i,'UEN']== 'PERDRIEL2           ':
        consistencia.loc[i,'BANDERA']='YPF'
    elif consistencia.loc[i,'UEN']== 'PUENTE OLIVE        ':
        consistencia.loc[i,'BANDERA']='YPF'
    elif consistencia.loc[i,'UEN']== 'SAN JOSE            ':
        consistencia.loc[i,'BANDERA']='YPF'
    elif consistencia.loc[i,'UEN']== 'LAMADRID            ':
        consistencia.loc[i,'BANDERA']='YPF'
    elif consistencia.loc[i,'UEN']== 'AZCUENAGA           ':
        consistencia.loc[i,'BANDERA']='YPF'
    else:
        consistencia.loc[i,'BANDERA']='DAPSA'


consistencia = consistencia.sort_values(['BANDERA','UEN'])
#consistencia = consistencia.loc[:,consistencia.columns!="BANDERA"]

######### Reordeno las columnas
consistencia = consistencia.reindex(columns= ['BANDERA','UEN','CODPRODUCTO','Volumen Turno 3 Ayer','DESCARGAS','Ventas Hoy','Ventas Acumuladas','Consistencia','Volumen Turno 3','Desvio','Volumen Turno 1','Descarga Consistencia','DesvioDescargas'])

######## Creo variable para el excel ya que en el excel si enviare todas las columnas
consistenciaExcel = consistencia
consistenciaExcel = consistenciaExcel.loc[:,consistenciaExcel.columns!='Ventas Acumuladas']
#### Elimino columnas que no entraran en las imagenes
consistencia = consistencia.loc[:,consistencia.columns!="Volumen Turno 3 Ayer"]
consistencia = consistencia.loc[:,consistencia.columns!="Descargas"]
consistencia = consistencia.loc[:,consistencia.columns!="Consistencia"]
consistencia = consistencia.loc[:,consistencia.columns!="Descarga Consistencia"]
consistencia = consistencia.loc[:,consistencia.columns!="Ventas Hoy"]
consistencia = consistencia.loc[:,consistencia.columns!="Desvio"]
df_VolumTurno1 = df_VolumTurno1.loc[:,df_VolumTurno1.columns!="VOLFINALAY"]
df_VolumTurno1 = df_VolumTurno1.loc[:,df_VolumTurno1.columns!="VOLFINAGUAAY"]


#############################################
############### DAPSA #######################
#############################################
df_VolumTurno1 = df_VolumTurno1.merge(consistencia,on=['UEN','CODPRODUCTO','Volumen Turno 1'],how='outer')
######### Creo Imagen GO Dapsa #######
df_VolumTurno1=df_VolumTurno1.merge(df_VentasHoy,on=['UEN','CODPRODUCTO','Ventas Acumuladas'],how='outer')
df_VolumTurno1 = df_VolumTurno1.fillna(0)
df_VolumTurno1=df_VolumTurno1.reindex(columns=['UEN','CODPRODUCTO','Volumen Turno 1','Ventas Hoy','DESCARGAS','Ventas Acumuladas'])
df_VolumTurno1 = df_VolumTurno1.rename({'Ventas Hoy':'Ventas Ayer','DESCARGAS':'Descargas Ayer'},axis=1)
#########

goDAPSA = df_VolumTurno1
go = goDAPSA['CODPRODUCTO'] == "GO   "
goDAPSA = goDAPSA[go]

goDAPSA = goDAPSA.convert_dtypes()
goDAPSA = goDAPSA[goDAPSA.UEN.isin(["LAS HERAS           "
                            ,'MERCADO 2           '
                            ,'MERC GUAYMALLEN     '
                            ,'MITRE               '
                            ,'SARMIENTO           '
                            ,'URQUIZA             '
                            ,'VILLANUEVA          '
                            ,'ADOLFO CALLE        '])]
goDAPSA=goDAPSA.sort_values('UEN')
goDAPSA.loc["colTOTAL"]= pd.Series(
    goDAPSA.sum()
    , index=['Ventas Ayer','Ventas Acumuladas','Descargas Ayer']
)
goDAPSA.fillna({"UEN":"TOTAL"}, inplace=True)
goDAPSA.loc["Total Acumulado"]= pd.Series()
goDAPSA.fillna({"UEN":"Total Acumulado"}, inplace=True)
optimo=(goDAPSA.loc['colTOTAL','Ventas Acumuladas'])
goDAPSA.fillna({"Ventas Ayer":optimo}, inplace=True)
goDAPSA = goDAPSA.loc[:,goDAPSA.columns!='Ventas Acumuladas']
goDAPSA = goDAPSA.astype(str)
goDAPSA = goDAPSA.replace({'<NA>': np.nan, '<Null>': np.nan, '<EMPTY>': np.nan})
goDAPSA=goDAPSA.fillna(' ')
######### Creo Imagen EU Dapsa #######

euDAPSA = df_VolumTurno1

go = euDAPSA['CODPRODUCTO'] == 'EU   '
euDAPSA = euDAPSA[go]

euDAPSA = euDAPSA.convert_dtypes()
euDAPSA = euDAPSA[euDAPSA.UEN.isin(["LAS HERAS           "
                            ,'MERCADO 2           '
                            ,'MERC GUAYMALLEN     '
                            ,'MITRE               '
                            ,'SARMIENTO           '
                            ,'URQUIZA             '
                            ,'VILLANUEVA          '
                            ,'ADOLFO CALLE        '])]
euDAPSA=euDAPSA.sort_values('UEN')
euDAPSA.loc["colTOTAL"]= pd.Series(
    euDAPSA.sum()
    , index=['Ventas Ayer','Ventas Acumuladas','Descargas Ayer']
)
euDAPSA.fillna({"UEN":"TOTAL"}, inplace=True)
euDAPSA.loc["Total Acumulado"]= pd.Series()
euDAPSA.fillna({"UEN":"Total Acumulado"}, inplace=True)
optimo=(euDAPSA.loc['colTOTAL','Ventas Acumuladas'])
euDAPSA.fillna({"Ventas Ayer":optimo}, inplace=True)
euDAPSA = euDAPSA.loc[:,euDAPSA.columns!='Ventas Acumuladas']
euDAPSA = euDAPSA.astype(str)
euDAPSA = euDAPSA.replace({'<NA>': np.nan, '<Null>': np.nan, '<EMPTY>': np.nan})
euDAPSA=euDAPSA.fillna(' ')
######### Creo Imagen NS Dapsa #######

nsDAPSA = df_VolumTurno1
go = nsDAPSA['CODPRODUCTO'] == 'NS   '
nsDAPSA = nsDAPSA[go]

nsDAPSA = nsDAPSA.convert_dtypes()
nsDAPSA = nsDAPSA[nsDAPSA.UEN.isin(["LAS HERAS           "
                            ,'MERCADO 2           '
                            ,'MERC GUAYMALLEN     '
                            ,'MITRE               '
                            ,'SARMIENTO           '
                            ,'URQUIZA             '
                            ,'VILLANUEVA          '
                            ,'ADOLFO CALLE        '])]
nsDAPSA=nsDAPSA.sort_values('UEN')
nsDAPSA.loc["colTOTAL"]= pd.Series(
    nsDAPSA.sum()
    , index=['Ventas Ayer','Ventas Acumuladas','Descargas Ayer']
)
nsDAPSA.fillna({"UEN":"TOTAL"}, inplace=True)
nsDAPSA.loc["Total Acumulado"]= pd.Series()
nsDAPSA.fillna({"UEN":"Total Acumulado"}, inplace=True)
optimo=(nsDAPSA.loc['colTOTAL','Ventas Acumuladas'])
nsDAPSA.fillna({"Ventas Ayer":optimo}, inplace=True)
nsDAPSA = nsDAPSA.loc[:,nsDAPSA.columns!='Ventas Acumuladas']
nsDAPSA = nsDAPSA.astype(str)
nsDAPSA = nsDAPSA.replace({'<NA>': np.nan, '<Null>': np.nan, '<EMPTY>': np.nan})
nsDAPSA=nsDAPSA.fillna(' ')

######### Creo Imagen NU Dapsa #######

nuDAPSA = df_VolumTurno1

go = nuDAPSA['CODPRODUCTO'] == "NU   "
nuDAPSA = nuDAPSA[go]

nuDAPSA = nuDAPSA.convert_dtypes()
nuDAPSA = nuDAPSA[nuDAPSA.UEN.isin(["LAS HERAS           "
                            ,'MERCADO 2           '
                            ,'MERC GUAYMALLEN     '
                            ,'MITRE               '
                            ,'SARMIENTO           '
                            ,'URQUIZA             '
                            ,'VILLANUEVA          '
                            ,'ADOLFO CALLE        '])]

nuDAPSA=nuDAPSA.sort_values('UEN')
nuDAPSA.loc["colTOTAL"]= pd.Series(
    nuDAPSA.sum()
    , index=['Ventas Ayer','Ventas Acumuladas','Descargas Ayer']
)
nuDAPSA.fillna({"UEN":"TOTAL"}, inplace=True)
nuDAPSA.loc["Total Acumulado"]= pd.Series()
nuDAPSA.fillna({"UEN":"Total Acumulado"}, inplace=True)
optimo=(nuDAPSA.loc['colTOTAL','Ventas Acumuladas'])
nuDAPSA.fillna({"Ventas Ayer":optimo}, inplace=True)
nuDAPSA = nuDAPSA.loc[:,nuDAPSA.columns!='Ventas Acumuladas']
nuDAPSA = nuDAPSA.astype(str)
nuDAPSA = nuDAPSA.replace({'<NA>': np.nan, '<Null>': np.nan, '<EMPTY>': np.nan})
nuDAPSA=nuDAPSA.fillna(' ')

#############################################
############### YPF #########################
#############################################

######### Creo Imagen GO YPF #######


goYPF = df_VolumTurno1
go = goYPF['CODPRODUCTO'] == "GO   "
goYPF = goYPF[go]

goYPF = goYPF.convert_dtypes()
goYPF = goYPF[goYPF.UEN.isin(["PERDRIEL            "
                            ,'PERDRIEL2           '
                            ,'PUENTE OLIVE        '
                            ,'SAN JOSE            '
                            ,'LAMADRID            '
                            ,'AZCUENAGA           '
                            ])]
goYPF=goYPF.sort_values('UEN')
goYPF.loc["colTOTAL"]= pd.Series(
    goYPF.sum()
    , index=['Ventas Ayer','Ventas Acumuladas','Descargas Ayer']
)
goYPF.fillna({"UEN":"TOTAL"}, inplace=True)
goYPF.loc["Total Acumulado"]= pd.Series()
goYPF.fillna({"UEN":"Total Acumulado"}, inplace=True)
optimo=(goYPF.loc['colTOTAL','Ventas Acumuladas'])
goYPF.fillna({"Ventas Ayer":optimo}, inplace=True)
goYPF = goYPF.loc[:,goYPF.columns!='Ventas Acumuladas']
goYPF = goYPF.astype(str)
goYPF = goYPF.replace({'<NA>': np.nan, '<Null>': np.nan, '<EMPTY>': np.nan})
goYPF=goYPF.fillna(' ')

######### Creo Imagen EU YPF #######

euYPF = df_VolumTurno1

go = euYPF['CODPRODUCTO'] == 'EU   '
euYPF = euYPF[go]

euYPF = euYPF.convert_dtypes()
euYPF = euYPF[euYPF.UEN.isin(["PERDRIEL            "
                            ,'PERDRIEL2           '
                            ,'PUENTE OLIVE        '
                            ,'SAN JOSE            '
                            ,'LAMADRID            '
                            ,'AZCUENAGA           '
                            ])]
euYPF=euYPF.sort_values('UEN')
euYPF.loc["colTOTAL"]= pd.Series(
    euYPF.sum()
    , index=['Ventas Ayer','Ventas Acumuladas','Descargas Ayer']
)
euYPF.fillna({"UEN":"TOTAL"}, inplace=True)
euYPF.loc["Total Acumulado"]= pd.Series()
euYPF.fillna({"UEN":"Total Acumulado"}, inplace=True)
optimo=(euYPF.loc['colTOTAL','Ventas Acumuladas'])
euYPF.fillna({"Ventas Ayer":optimo}, inplace=True)
euYPF = euYPF.loc[:,euYPF.columns!='Ventas Acumuladas']
euYPF = euYPF.astype(str)
euYPF = euYPF.replace({'<NA>': np.nan, '<Null>': np.nan, '<EMPTY>': np.nan})
euYPF=euYPF.fillna(' ')

######### Creo Imagen NS YPF #######

nsYPF = df_VolumTurno1
go = nsYPF['CODPRODUCTO'] == 'NS   '
nsYPF = nsYPF[go]

nsYPF = nsYPF.convert_dtypes()
nsYPF = nsYPF[nsYPF.UEN.isin(["PERDRIEL            "
                            ,'PERDRIEL2           '
                            ,'PUENTE OLIVE        '
                            ,'SAN JOSE            '
                            ,'LAMADRID            '
                            ,'AZCUENAGA           '
                            ])]

nsYPF=nsYPF.sort_values('UEN')
nsYPF.loc["colTOTAL"]= pd.Series(
    nsYPF.sum()
    , index=['Ventas Ayer','Ventas Acumuladas','Descargas Ayer']
)
nsYPF.fillna({"UEN":"TOTAL"}, inplace=True)
nsYPF.loc["Total Acumulado"]= pd.Series()
nsYPF.fillna({"UEN":"Total Acumulado"}, inplace=True)
optimo=(nsYPF.loc['colTOTAL','Ventas Acumuladas'])
nsYPF.fillna({"Ventas Ayer":optimo}, inplace=True)
nsYPF = nsYPF.loc[:,nsYPF.columns!='Ventas Acumuladas']
nsYPF = nsYPF.astype(str)
nsYPF = nsYPF.replace({'<NA>': np.nan, '<Null>': np.nan, '<EMPTY>': np.nan})
nsYPF=nsYPF.fillna(' ')





######### Creo Imagen NU YPF #######

nuYPF = df_VolumTurno1

go = nuYPF['CODPRODUCTO'] == "NU   "
nuYPF = nuYPF[go]

nuYPF = nuYPF.convert_dtypes()
nuYPF = nuYPF[nuYPF.UEN.isin(["PERDRIEL            "
                            ,'PERDRIEL2           '
                            ,'PUENTE OLIVE        '
                            ,'SAN JOSE            '
                            ,'LAMADRID            '
                            ,'AZCUENAGA           '
                            ])]

nuYPF=nuYPF.sort_values('UEN')
nuYPF.loc["colTOTAL"]= pd.Series(
    nuYPF.sum()
    , index=['Ventas Ayer','Ventas Acumuladas','Descargas Ayer']
)
nuYPF.fillna({"UEN":"TOTAL"}, inplace=True)
nuYPF.loc["Total Acumulado"]= pd.Series()
nuYPF.fillna({"UEN":"Total Acumulado"}, inplace=True)
optimo=(nuYPF.loc['colTOTAL','Ventas Acumuladas'])
nuYPF.fillna({"Ventas Ayer":optimo}, inplace=True)
nuYPF = nuYPF.loc[:,nuYPF.columns!='Ventas Acumuladas']
nuYPF = nuYPF.astype(str)
nuYPF = nuYPF.replace({'<NA>': np.nan, '<Null>': np.nan, '<EMPTY>': np.nan})
nuYPF=nuYPF.fillna(' ')

######### Creo el Destilador de Fuente DEL EXCEL

def estilador_excel(df,columnas,list_Col_Num,list_Col_Perc,titulo):

    resultado = df.style \
        .format("{0:,.2f}", subset=list_Col_Num) \
        .format("{:,.2%}", subset=list_Col_Perc) \
        .hide_index() \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>") \
        .set_properties(subset= columnas+list_Col_Num+list_Col_Perc
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
        ]) 
    evitarTotales = df.index.get_level_values(0)
    return resultado

colum = []
colporc = []
colnum=["Descarga Consistencia","Volumen Turno 3","Volumen Turno 3 Ayer","DesvioDescargas"]
consistenciaExcel = estilador_excel(consistenciaExcel, colum,colnum,colporc, "a")


######### Creo el Destilador de Fuente de las imagenes
def formato_celda(celda):
    try:
        valor = pd.to_numeric(celda)
        if valor < 1:
            return '{:.2f}'.format(valor)
        else:
            return '{:,.2f}'.format(valor)

    except:
        return celda
    
def esilador_Imagen(df,list_Col_Num,list_Col_Perc,titulo):

   
    resultado = df.style \
        .format(formato_celda) \
        .hide_index() \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(0,"days")).strftime("%d/%m/%y"))
            + "<br>") \
        .set_properties(subset= list_Col_Num+list_Col_Perc
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
        .apply(lambda x: ["background: black" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == "colTOTAL" 
            else "" for i in x]
        , axis=1) \
        .apply(lambda x: ["background: black" if x.name == "Total Acumulado" 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == "Total Acumulado" 
            else "" for i in x]
        , axis=1) \

    return resultado

colum = ["Volumen Turno 1","Ventas Ayer",'Descargas Ayer']
colporc = []
colnum=[]

#### Aplico el estilador a cada imagen 
consistenciaImagenGoDAPSA = esilador_Imagen(goDAPSA, colum,colnum, "Volumen GO DAPSA")
consistenciaImagenEuDAPSA = esilador_Imagen(euDAPSA, colum,colnum, "Volumen EU DAPSA")
consistenciaImagenNuDAPSA = esilador_Imagen(nuDAPSA, colum,colnum, "Volumen NU DAPSA")
consistenciaImagenNsDAPSA = esilador_Imagen(nsDAPSA, colum,colnum, "Volumen NS DAPSA")
consistenciaImagenGoYPF = esilador_Imagen(goYPF, colum,colnum, "Volumen GO YPF")
consistenciaImagenEuYPF = esilador_Imagen(euYPF, colum,colnum, "Volumen EU YPF")
consistenciaImagenNuYPF = esilador_Imagen(nuYPF, colum,colnum, "Volumen NU YPF")
consistenciaImagenNsYPF = esilador_Imagen(nsYPF, colum,colnum, "Volumen NS YPF")
#### DEFINO EL DESTINO DONDE SE GUARDARA LA IMAGEN Y EL NOMBRE

ubicacion = "C:/Informes/Informe descargas y volumenes/"
#nombrePen = "volumen.png"
#nombrePenDiario = "desc.png"
### IMPRIMO LA IMAGEN 
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

df_to_image(consistenciaImagenGoDAPSA, ubicacion, "VolumGoDapsa.png")
df_to_image(consistenciaImagenEuDAPSA, ubicacion, "VolumEuDapsa.png")
df_to_image(consistenciaImagenNsDAPSA, ubicacion, "VolumNsDapsa.png")
df_to_image(consistenciaImagenNuDAPSA, ubicacion, "VolumNuDapsa.png")
df_to_image(consistenciaImagenGoYPF, ubicacion, "VolumGoYPF.png")
df_to_image(consistenciaImagenEuYPF, ubicacion, "VolumEuYPF.png")
df_to_image(consistenciaImagenNsYPF, ubicacion, "VolumNsYPF.png")
df_to_image(consistenciaImagenNuYPF, ubicacion, "VolumNuYPF.png")

ubicacionExcel = "C:/Informes/Informe descargas y volumenes/"

nombreExcel = "volumen.xlsx"

### IMPRIMO LA Excel 

def df_to_Excel(df, ubicacion, nombre):
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
        df.to_excel(ubicacion+nombre)
    else:
        df.to_excel(ubicacion+nombre)

df_to_Excel(consistenciaExcel, ubicacionExcel, nombreExcel)
#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)