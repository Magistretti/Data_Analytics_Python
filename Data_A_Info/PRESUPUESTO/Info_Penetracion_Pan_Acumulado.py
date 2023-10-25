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

########################################
################ MENSUAL ACUMULADO #####
########################################

##########################   DESPACHOS ACUMULADOS MENSUAL

df_despachosM = pd.read_sql('''
	
		
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


SELECT UEN,COUNT(DISTINCT ID) AS DESPACHOS,TURNO FROM Despapro 
WHERE FECHASQL >= @inicioMesActual
	AND FECHASQL < @hoy
	AND UEN NOT IN ('AZCUENAGA           '
        , 'LAMADRID            '
        , 'PUENTE OLIVE        '
        ,'SAN JOSE            ')
	and VOLUMEN < '60'
	GROUP BY UEN,TURNO 
    ''' ,db_conex)

#### Filtro y creo columnas Turno 1 ,2 y 3


# Utilizamos pivot_table para reorganizar el dataframe
df_pivot = pd.pivot_table(df_despachosM, values='DESPACHOS', index=['UEN'], columns=['TURNO'], aggfunc=sum)

# Creamos las nuevas columnas 'turno 1', 'turno 2' y 'turno 3' y las llenamos con los datos de la tabla pivotada
df_pivot.columns = ['TURNO ' + str(col) for col in df_pivot.columns]
df_pivot.reset_index(inplace=True)

df_despachosM=df_pivot

df_despachosM = df_despachosM.reset_index()


df_despachosM = df_despachosM.sort_values('UEN', ascending = True)
###### Elimino columna Turno 1 Ya que Panaderia en Turno 1 no tiene Ventas
df_despachosM = df_despachosM.loc[:,df_despachosM.columns!="index"]
df_despachosM = df_despachosM.loc[:,df_despachosM.columns!="TURNO 1"]
df_despachosM = df_despachosM.rename({'TURNO 2':'TURNO 1 desp','TURNO 3':'TURNO 2 desp'},axis=1)
############### VENTA PANADERIA ACUMULADO MENSUAL

ventaPanaderiaM = pd.read_sql('''
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
	select a.UEN, a.TURNO, sum(a.CANTIDAD) as CANTIDAD from 
	(select  p.UEN, p.TURNO, p.CANTIDAD as CANTIDAD
	from dbo.PanSalDe as p
	inner join dbo.PanSalGe as g on
	p.UEN = g.UEN
	and p.FECHASQL=g.FECHASQL
	and p.TURNO = g.TURNO
	and p.NROCOMP = g.NROCOMP
	where g.NROCLIENTE = '30'
	and p.PRECIO > '10'
	and (p.CODIGO = 12
	or p.CODIGO = 16
	or p.CODIGO = 11
	or p.CODIGO = 14
	or p.CODIGO = 15
    or p.CODIGO = 17)
	and p.FECHASQL >= @inicioMesActual
	and	p.FECHASQL < @hoy
    AND p.UEN NOT IN ('AZCUENAGA           '
        , 'LAMADRID            '
        , 'PUENTE OLIVE        '
        ,'SAN JOSE            ')) AS a
	GROUP BY a.UEN, a.TURNO
  '''     ,db_conex)

####Filtro y Creo columnas TURNO 1, 2 Y 3

# Utilizamos pivot_table para reorganizar el dataframe
df_pivot = pd.pivot_table(ventaPanaderiaM, values='CANTIDAD', index=['UEN'], columns=['TURNO'], aggfunc=sum)

# Creamos las nuevas columnas 'turno 1', 'turno 2' y 'turno 3' y las llenamos con los datos de la tabla pivotada
df_pivot.columns = ['TURNO ' + str(col) for col in df_pivot.columns]
df_pivot.reset_index(inplace=True)

ventaPanaderiaM=df_pivot


########  VENTA PANADERIA PEDRIEL ACUMULADO

ventaPanaderiaMP = pd.read_sql('''
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
    SELECT e.UEN, (e.CANTIDAD * p.ENVASE / 12) as CANTIDAD,E.TURNO
  FROM [Rumaos].[dbo].[SCEgreso] as E
  left join SCprodUEN as P on
  E.UEN = P.UEN
  AND E.CODIGO = P.CODIGO
  where  
   E.UEN = 'xpress'
  AND (P.AGRUPACION = 'PANIFICADOS' OR P.AGRUPACION = 'PANIFICADOS PROMOS')
  and P.PREVENTA > '10'
	and E.FECHASQL >= @inicioMesActual
	and	E.FECHASQL < @hoy
     '''  ,db_conex)

# Utilizamos pivot_table para reorganizar el dataframe
df_pivot = pd.pivot_table(ventaPanaderiaMP, values='CANTIDAD', index=['UEN'], columns=['TURNO'], aggfunc=sum)

# Creamos las nuevas columnas 'turno 1', 'turno 2' y 'turno 3' y las llenamos con los datos de la tabla pivotada
df_pivot.columns = ['TURNO ' + str(col) for col in df_pivot.columns]
df_pivot.reset_index(inplace=True)

ventaPanaderiaMP=df_pivot

ventaPanaderiaMP = ventaPanaderiaMP.replace({ 'XPRESS              ' : 'PERDRIEL            '})
########CONCATENO TABLAS VENTAS PAN ACUMULADO Y DESPACHOS TOTALES ACUMULADOS
ventaPanaderiaM = ventaPanaderiaM.merge(ventaPanaderiaMP, on=["UEN",'TURNO 1','TURNO 2'], how='outer')
ventaPanaderiaM = ventaPanaderiaM.groupby(
        ["UEN"]
        , as_index=False
    ).sum()


penetracionPanM= ventaPanaderiaM.merge(df_despachosM, on=["UEN"], how= "outer")
penetracionPanM = penetracionPanM.groupby(
        ["UEN"]
        , as_index=False
    ).sum()

penetracionPanM = penetracionPanM.fillna(0)

########CREO COLUMNAS DE PENETRACION TURNO 1 Y TURNO 2 Y PRESUPUESTO
penetracionPanM = penetracionPanM.assign(PenetracionT1A= penetracionPanM["TURNO 1"] / penetracionPanM["TURNO 1 desp"] )
penetracionPanM = penetracionPanM.assign(PenetracionT2A= penetracionPanM["TURNO 2"]/ penetracionPanM["TURNO 2 desp"] )
penetracionPanM = penetracionPanM.assign(PenetracionTotalA=(penetracionPanM["TURNO 1"] + penetracionPanM["TURNO 2"]) / (penetracionPanM["TURNO 1 desp"] + penetracionPanM["TURNO 2 desp"]))
penetracionPanM = penetracionPanM.assign(PresupuestoA= 0.0375)
penetracionPanM = penetracionPanM.fillna(0)

####### CREO COLUMNAS DE DESVIOS
penetracionPanM = penetracionPanM.assign(DesvioT1A= (penetracionPanM["PenetracionT1A"] / penetracionPanM["PresupuestoA"]) - 1)
penetracionPanM = penetracionPanM.assign(DesvioT2A= (penetracionPanM["PenetracionT2A"] / penetracionPanM["PresupuestoA"]) - 1 )
penetracionPanM = penetracionPanM.assign(DesvioTotalA= (penetracionPanM["PenetracionTotalA"] / penetracionPanM["PresupuestoA"]) - 1 )
#penetracionPan = penetracionPan.style.format("{0:,.2%}", subset= ["PenetracionT1","PenetracionT2","Presupuesto","DesvioT1","DesvioT2"])


#############################################   
######################### DIARIO
##############################################


###############VENTA PANADERIA DIARIA


ventaPanaderia = pd.read_sql('''
    select DISTINCT(p.UEN), p.TURNO, sum(p.CANTIDAD) as CANTIDAD
	from dbo.PanSalDe as p
	inner join dbo.PanSalGe as g on
	p.UEN = g.UEN
	and p.FECHASQL=g.FECHASQL
	and p.TURNO = g.TURNO
	and p.NROCOMP = g.NROCOMP
	where g.NROCLIENTE = '30'
	and p.PRECIO > '10'
	and (p.CODIGO = 12
	or p.CODIGO = 16
	or p.CODIGO = 11
	or p.CODIGO = 14
	or p.CODIGO = 15
	OR p.CODIGO = 17)
	and p.FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
    AND p.UEN NOT IN ('AZCUENAGA           '
        , 'LAMADRID            '
        , 'PUENTE OLIVE        '
        ,'SAN JOSE            ')
    group by p.UEN,P.TURNO
    '''  ,db_conex)

#### Filtro y creo columnas Turno 1 ,2 y 3
ventaPanaderia = ventaPanaderia.convert_dtypes()
# Utilizamos pivot_table para reorganizar el dataframe
df_pivot = pd.pivot_table(ventaPanaderia, values='CANTIDAD', index=['UEN'], columns=['TURNO'], aggfunc=sum)

# Creamos las nuevas columnas 'turno 1', 'turno 2' y 'turno 3' y las llenamos con los datos de la tabla pivotada
df_pivot.columns = ['TURNO ' + str(col) for col in df_pivot.columns]
df_pivot.reset_index(inplace=True)

ventaPanaderia=df_pivot

######### VENTA PEDRIEL PANADERIA

ventaPanaderiaP = pd.read_sql('''
    SELECT e.UEN, (e.CANTIDAD * p.ENVASE / 12) as CANTIDAD,E.TURNO
  FROM [Rumaos].[dbo].[SCEgreso] as E
  left join SCprodUEN as P on
  E.UEN = P.UEN
  AND E.CODIGO = P.CODIGO
  where FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date)) 
  and E.UEN = 'xpress'
  AND (P.AGRUPACION = 'PANIFICADOS' OR P.AGRUPACION = 'PANIFICADOS PROMOS')
  and P.PREVENTA > '10'
    '''  ,db_conex)
ventaPanaderiaP = ventaPanaderiaP.convert_dtypes()

# Utilizamos pivot_table para reorganizar el dataframe
df_pivot = pd.pivot_table(ventaPanaderiaP, values='CANTIDAD', index=['UEN'], columns=['TURNO'], aggfunc=sum)

# Creamos las nuevas columnas 'turno 1', 'turno 2' y 'turno 3' y las llenamos con los datos de la tabla pivotada
df_pivot.columns = ['TURNO ' + str(col) for col in df_pivot.columns]
df_pivot.reset_index(inplace=True)

ventaPanaderiaP=df_pivot

ventaPanaderiaP = ventaPanaderiaP.replace({ 'XPRESS              ' : 'PERDRIEL            '})
if 'TURNO 1' in ventaPanaderiaP:
    x=1
else:
    ventaPanaderiaP['TURNO 1']=0

ventaPanaderia = ventaPanaderia.merge(ventaPanaderiaP, on=['UEN','TURNO 1','TURNO 2'],how='outer')
ventaPanaderia = ventaPanaderia.fillna(0)


############## DESPACHOS TOTALES


df_despachos = pd.read_sql('''
	
SELECT UEN,COUNT(DISTINCT ID) AS DESPACHOS,TURNO FROM Despapro 
WHERE FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
	AND UEN NOT IN ('AZCUENAGA           '
        , 'LAMADRID            '
        , 'PUENTE OLIVE        '
        ,'SAN JOSE            '
		,'perdriel'
		,'perdriel2')
	and VOLUMEN < '60'
	GROUP BY UEN,TURNO
     ''',db_conex)

#### Filtro y creo columnas Turno 1 ,2 y 3

df_despachos = df_despachos.convert_dtypes()


# Utilizamos pivot_table para reorganizar el dataframe
df_pivot = pd.pivot_table(df_despachos, values='DESPACHOS', index=['UEN'], columns=['TURNO'], aggfunc=sum)

# Creamos las nuevas columnas 'turno 1', 'turno 2' y 'turno 3' y las llenamos con los datos de la tabla pivotada
df_pivot.columns = ['TURNO ' + str(col) for col in df_pivot.columns]
df_pivot.reset_index(inplace=True)

df_despachos=df_pivot


df_despachos = df_despachos.sort_values('UEN', ascending = True)

#### Elimino columna TURNO 1 ya que en este turno Panaderia no tiene ventas
df_despachos = df_despachos.loc[:,df_despachos.columns!="TURNO 1"]

df_despachos = df_despachos.rename({'TURNO 2':'TURNO 1 desp D','TURNO 3':'TURNO 2 desp D'},axis=1)

########CONCATENO TABLAS VENTAS PAN Y DESPACHOS TOTALES DIARIAS

penetracionPan= ventaPanaderia.merge(df_despachos, on=["UEN"], how="outer")
penetracionPan = penetracionPan.groupby(
        ["UEN"]
        , as_index=False
    ).sum()

########CREO COLUMNAS DE PENETRACION TURNO 1 Y TURNO 2 Y PRESUPUESTO
penetracionPan = penetracionPan.assign(PenetracionT1= penetracionPan["TURNO 1"] / penetracionPan["TURNO 1 desp D"] )
penetracionPan = penetracionPan.assign(PenetracionT2= penetracionPan["TURNO 2"]/ penetracionPan["TURNO 2 desp D"] )
penetracionPan = penetracionPan.assign(PenetracionTotalD= (penetracionPan["TURNO 1"]+penetracionPan["TURNO 2"] )/ (penetracionPan["TURNO 1 desp D"]+penetracionPan["TURNO 2 desp D"]))
penetracionPan = penetracionPan.assign(Presupuesto= 0.0375)
penetracionPan.loc[penetracionPan['UEN'] == 'URQUIZA             ', 'Presupuesto'] = 0.025
penetracionPan.loc[penetracionPan['UEN'] == 'ADOLFO CALLE        ', 'Presupuesto'] = 0.02
penetracionPan.loc[penetracionPan['UEN'] == 'MERCADO 2           ', 'Presupuesto'] = 0.045
penetracionPan = penetracionPan.fillna(0)


####### CREO COLUMNAS DE DESVIOS
penetracionPan = penetracionPan.assign(DesvioT1= (penetracionPan["PenetracionT1"] / penetracionPan["Presupuesto"]) - 1)
penetracionPan = penetracionPan.assign(DesvioT2= (penetracionPan["PenetracionT2"] / penetracionPan["Presupuesto"]) - 1 )
penetracionPan = penetracionPan.assign(DesvioTotalD= (penetracionPan["PenetracionTotalD"] / penetracionPan["Presupuesto"]) - 1 )
#penetracionPan = penetracionPan.style.format("{0:,.2%}", subset= ["PenetracionT1","PenetracionT2","Presupuesto","DesvioT1","DesvioT2"])

############## CONCATENO TABLAS DIARIAS CON ACUMULADAS

penetracionPanT = penetracionPan.merge(penetracionPanM, on=["UEN"], how="outer")



penetracionPanT.drop(penetracionPanT[(penetracionPanT['UEN'] == 'PERDRIEL            ')].index, inplace=True)
penetracionPanT.drop(penetracionPanT[(penetracionPanT['UEN'] == 'PERDRIEL2           ')].index, inplace=True)

### Creo columna (fila) TOTALES
penetracionPanT.loc["colTOTAL"]= pd.Series(
    penetracionPanT.sum()
    , index=["TURNO 1_x","TURNO 2_x","TURNO 1_y","TURNO 2_y",'TURNO 1 desp D','TURNO 2 desp D',
            'TURNO 1 desp','TURNO 2 desp']
)
penetracionPanT.fillna({"UEN":"TOTAL"}, inplace=True)


#Creo totales de PENETRACION DIARIA TURNO 1
tasa = (penetracionPanT.loc["colTOTAL","TURNO 1_x"] /
    penetracionPanT.loc["colTOTAL","TURNO 1 desp D"])
penetracionPanT.fillna({"PenetracionT1":tasa}, inplace=True)
#Creo totales de PENETRACION DIARIA TURNO 2
tasa1 = (penetracionPanT.loc["colTOTAL","TURNO 2_x"] /
    penetracionPanT.loc["colTOTAL","TURNO 2 desp D"])
penetracionPanT.fillna({"PenetracionT2":tasa1}, inplace=True)
tasa2 = ((penetracionPanT.loc["colTOTAL","TURNO 1_x"]+penetracionPanT.loc["colTOTAL","TURNO 2_x"]) /
    (penetracionPanT.loc["colTOTAL","TURNO 1 desp D"]+penetracionPanT.loc["colTOTAL","TURNO 2 desp D"]))
penetracionPanT.fillna({"PenetracionTotalD":tasa1}, inplace=True)
penetracionPanT.fillna({"Presupuesto":0.0375}, inplace=True)

#Creo totales de PENETRACION ACUMULADA TURNO 1
tasa3 = (penetracionPanT.loc["colTOTAL","TURNO 1_y"] /
    penetracionPanT.loc["colTOTAL","TURNO 1 desp"])
penetracionPanT.fillna({"PenetracionT1A":tasa3}, inplace=True)
#Creo totales de PENETRACION ACUMULADA TURNO 2 Y PRESUPUESTO
tasa4 = (penetracionPanT.loc["colTOTAL","TURNO 2_y"] /
    penetracionPanT.loc["colTOTAL","TURNO 2 desp"])
penetracionPanT.fillna({"PenetracionT2A":tasa4}, inplace=True)

tasa5 = ((penetracionPanT.loc["colTOTAL","TURNO 1_y"]+penetracionPanT.loc["colTOTAL","TURNO 2_y"]) /
    (penetracionPanT.loc["colTOTAL","TURNO 1 desp"]+penetracionPanT.loc["colTOTAL","TURNO 2 desp"]))
penetracionPanT.fillna({"PenetracionTotalA":tasa5}, inplace=True)

penetracionPanT.fillna({"PresupuestoA":0.0375}, inplace=True)
#Creo totales de DESVIO DIARIO TURNO 1 Y 2

tasa6 = ((penetracionPanT.loc["colTOTAL","PenetracionT1"] /
    penetracionPanT.loc["colTOTAL","Presupuesto"]))
penetracionPanT.fillna({"DesvioT1":tasa6}, inplace=True)

tasa7 = (penetracionPanT.loc["colTOTAL","PenetracionT2"] /
    penetracionPanT.loc["colTOTAL","Presupuesto"])
penetracionPanT.fillna({"DesvioT2":tasa7}, inplace=True)

tasa7 = (penetracionPanT.loc["colTOTAL","PenetracionTotalD"] /
    penetracionPanT.loc["colTOTAL","Presupuesto"])
penetracionPanT.fillna({"DesvioTotalD":tasa7}, inplace=True)

#Creo totales de DESVIO ACUMULADO TURNO 1 Y 2
tasa8 = (penetracionPanT.loc["colTOTAL","PenetracionT1A"] /
    penetracionPanT.loc["colTOTAL","PresupuestoA"])
penetracionPanT.fillna({"DesvioT1A":tasa8}, inplace=True)

tasa9 = (penetracionPanT.loc["colTOTAL","PenetracionT2A"] /
    penetracionPanT.loc["colTOTAL","PresupuestoA"])
penetracionPanT.fillna({"DesvioT2A":tasa9}, inplace=True)

tasa9 = (penetracionPanT.loc["colTOTAL","PenetracionTotalA"] /
    penetracionPanT.loc["colTOTAL","PresupuestoA"])
penetracionPanT.fillna({"DesvioTotalA":tasa9}, inplace=True)

CONTROL=penetracionPanT
#### RENOMBRO COLUMNAS 

penetracionPanT = penetracionPanT.rename({'PenetracionT1': 'Penetracion Diaria TURNO 1'
, 'PenetracionT2': 'Penetracion Diaria TURNO 2','PenetracionTotalD': 'Penetracion Total Diaria','Presupuesto':'Presupuesto Diario','DesvioT1':'Desvio Diario TURNO 1'
,'DesvioT2':'Desvio Diario TURNO 2','DesvioTotalD':'Desvio Total Diario','PenetracionT1A':'Penetracion Acumulada TURNO 1'
,'PenetracionT2A':'Penetracion Acumulada TURNO 2','PenetracionTotalA':'Penetracion Total Acumulada','PresupuestoA':'Presupuesto Acumulado','DesvioT1A':'Desvio Acumulado TURNO 1','DesvioT2A':'Desvio Acumulado TURNO 2', 'DesvioTotalA':'Desvio Total Acumulado'}, axis=1)
######### DATAFRAME CON LA PENETRACION Y EL DESVIO DIARIO ELIMINO COLUMNAS QUE NO ENTRAN EN ESTE INFORME
penetracionPanDiario = penetracionPanT
penetracionPanDiario=penetracionPanDiario.reindex(columns=['UEN',"Penetracion Diaria TURNO 1"
	,"Penetracion Diaria TURNO 2"
    ,'Penetracion Total Diaria'
	,"Presupuesto Diario"
	,"Desvio Diario TURNO 1"
	,"Desvio Diario TURNO 2"
    ,"Desvio Total Diario"])
######### DATAFRAME QUE CONTIENE SOLO PENETRACION ELIMINO COLUMNAS QUE NO ENTRAN EN ESTE INFORME
penetraciononly = penetracionPanT
penetraciononly=penetraciononly.reindex(columns=['UEN',"Penetracion Diaria TURNO 1"
	,"Penetracion Diaria TURNO 2"
    ,"Penetracion Total Diaria"
    ,'Presupuesto Diario'                                             
    ,"Penetracion Acumulada TURNO 1"
	,"Penetracion Acumulada TURNO 2"
    ,"Penetracion Total Acumulada"])
penetraciononly['Presupuesto Diario'] = penetraciononly['Presupuesto Diario'].astype(float)
penetraciononly["Penetracion Total Acumulada"] = penetraciononly["Penetracion Total Acumulada"].astype(float)
penetraciononly['Desvio']=(penetraciononly['Penetracion Total Acumulada']/penetraciononly['Presupuesto Diario'])-1
penetraciononly=penetraciononly.drop('Penetracion Diaria TURNO 1', axis='columns')
penetraciononly=penetraciononly.drop('Penetracion Diaria TURNO 2', axis='columns')
penetraciononly=penetraciononly.drop('Penetracion Total Diaria', axis='columns')


penetraciononly=penetraciononly.reindex(columns=["UEN","Penetracion Acumulada TURNO 1","Penetracion Acumulada TURNO 2","Penetracion Total Acumulada",'Presupuesto Diario','Desvio'])

######## CAMBIO FORMATO DE LA IMAGEN (COLOR A FILAS Y COLUMNAS)

def _estiladorVtaTitulo(df, list_Col_Num, list_Col_Perc, titulo):
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
    #presupuesto_min = df["Presupuesto Diario"].min()
    #presupuesto_max = df["Presupuesto Diario"].max()
    resultado = df.style \
        .format("{0:,.0f}", subset=list_Col_Num) \
        .format("{:,.2%}", subset=list_Col_Perc) \
        .hide(axis=0) \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>"
        ) \
        .set_properties(subset=list_Col_Perc+list_Col_Num,
                         **{"text-align": "center", "width": "100px"}) \
        .set_properties(border="2px solid black") \
        .set_table_styles([
            {"selector": "caption",
             "props": [
                 ("font-size", "20px"),
                 ("text-align", "center")
             ]},
            {"selector": "th",
             "props": [
                 ("text-align", "center"),
                 ("background-color", "black"),
                 ("color", "white")
             ]}
        ]) \
        .apply(lambda x: ["background: black" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1)
    evitarTotales = df.index.get_level_values(0)
    resultado = resultado.background_gradient(
        cmap="Dark2" # Red->Yellow->Green
        ,vmin=-0.02
        ,vmax=0.02
        ,subset=pd.IndexSlice[evitarTotales[:-1],"Presupuesto Diario"]
    )
    subset_columns = pd.IndexSlice[evitarTotales[:-1], ['Desvio']]

    resultado= resultado.applymap(table_color,subset=subset_columns)

    return resultado

def table_color(val):
    """
    Takes a scalar and returns a string with
    the css property `'color: red'` for less than 60 marks, green otherwise.
    """
    color = 'blue' if val > 0 else 'red'
    return 'color: % s' % color

#### Defino columnas para cada Dataframe (Numericas)
numCols = [
        
         ]
### COLUMNAS PARA INFORME PENETRACION
percColsPen = [
	"Presupuesto Diario"
    ,"Penetracion Acumulada TURNO 1"
	,"Penetracion Acumulada TURNO 2"
    ,"Penetracion Total Acumulada"
    ,'Desvio'
]

###### Aplico el formato elegido a la imagen
penetraciononly = _estiladorVtaTitulo(penetraciononly,numCols,percColsPen, "INFO PENETRACION PANADERIA")
###### DEFINO NOMBRE Y UBICACION DE DONDE SE VA A GUARDAR LA IMAGEN

ubicacion = str(pathlib.Path(__file__).parent)+"\\"
nombrePen = "Info_Penetracion_Pan_Acumulado.png"
nombrePenDiario = "Info_PenetracionPan.png"
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

df_to_image(penetraciononly, ubicacion, nombrePen)
#df_to_image(penetracionPanDiario,ubicacion,nombrePenDiario)


#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)


