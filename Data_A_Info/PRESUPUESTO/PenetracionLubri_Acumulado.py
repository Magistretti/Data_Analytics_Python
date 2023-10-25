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

#ubicacion = str(pathlib.Path(__file__).parent)+"\\"
aux_semanal = "PresupuestoLubricantes.xlsx"
presuLubri =pd.read_excel('C:/Informes/Penetracion Lubricantes/PresupuestoLubricantes.xlsx', sheet_name= 'Hoja1')
presuLubri = presuLubri.convert_dtypes()
presuLubri = presuLubri.groupby(
        ["UEN"]
        , as_index=False
    ).sum()
presuLubri= presuLubri.reset_index()



##########################################
################ MENSUAL ACUMULADO
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
	and VOLUMEN < '60'
	GROUP BY UEN,TURNO 
    ''' ,db_conex)

# Utilizamos pivot_table para reorganizar el dataframe
df_pivot = pd.pivot_table(df_despachosM, values='DESPACHOS', index=['UEN'], columns=['TURNO'], aggfunc=sum)

# Creamos las nuevas columnas 'turno 1', 'turno 2' y 'turno 3' y las llenamos con los datos de la tabla pivotada
df_pivot.columns = ['TURNO ' + str(col) for col in df_pivot.columns]
df_pivot.reset_index(inplace=True)

df_despachosM=df_pivot

df_despachosM = df_despachosM.reset_index()


df_despachosM = df_despachosM.sort_values('UEN', ascending = True)

##### Elimino columna Turnos y Despachos ya que la informacion de estas columnas ahora la tengo en TURNO 1, TURNO 2 Y TURNO 3
df_despachosM = df_despachosM.loc[:,df_despachosM.columns!="index"]

df_despachosM = df_despachosM.fillna(0)

##### Agrupo por UEN
df_despachosM = df_despachosM.groupby(
        ["UEN"]
        , as_index=False
    ).sum()



############### VENTA LUBRICANTES ACUMULADO MENSUAL ##################

ventaLubricantesM = pd.read_sql('''
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
	select DISTINCT(M.ID),M.UEN, M.TURNO, (M.CANTIDAD * P.Envase * -1) AS CANTIDAD from dbo.VMovDet as M
	left join dbo.PLProdUEN as P
	on M.CODPRODUCTO = P.CODIGO
        and M.UEN = P.UEN
	where P.AGRUPACION = 'LUBRICANTES'
	and M.TIPOMOVIM = 3
	and M.FECHASQL >= @inicioMesActual
			and	M.FECHASQL < @hoy
  '''      ,db_conex)

# Utilizamos pivot_table para reorganizar el dataframe
df_pivot = pd.pivot_table(ventaLubricantesM, values='CANTIDAD', index=['UEN'], columns=['TURNO'], aggfunc=sum)

# Creamos las nuevas columnas 'turno 1', 'turno 2' y 'turno 3' y las llenamos con los datos de la tabla pivotada
df_pivot.columns = ['TURNO ' + str(col) for col in df_pivot.columns]
df_pivot.reset_index(inplace=True)

ventaLubricantesM=df_pivot

ventaLubricantesM = ventaLubricantesM.reset_index()


ventaLubricantesM = ventaLubricantesM.sort_values('UEN', ascending = True)

##### Elimino columna Turnos y Despachos ya que la informacion de estas columnas ahora la tengo en TURNO 1, TURNO 2 Y TURNO 3
ventaLubricantesM = ventaLubricantesM.loc[:,ventaLubricantesM.columns!="index"]
ventaLubricantesM = ventaLubricantesM.loc[:,ventaLubricantesM.columns!="ID"]

ventaLubricantesM = ventaLubricantesM.fillna(0)
##### Agrupo por UEN
ventaLubricantesM = ventaLubricantesM.groupby(
        ["UEN"]
        , as_index=False
    ).sum()

ventaLubricantesM = ventaLubricantesM.rename({'TURNO 1':'TURNO 1 ventas','TURNO 2':'TURNO 2 ventas','TURNO 3':'TURNO 3 ventas'},axis=1)

penetracionLubricantesM = ventaLubricantesM.merge(df_despachosM, on= ['UEN'], how= 'outer')

penetracionLubricantesM = penetracionLubricantesM.assign(PenetracionT1A= penetracionLubricantesM["TURNO 1 ventas"] / penetracionLubricantesM["TURNO 1"] )
penetracionLubricantesM = penetracionLubricantesM.assign(PenetracionT2A= penetracionLubricantesM["TURNO 2 ventas"]/ penetracionLubricantesM["TURNO 2"] )
penetracionLubricantesM = penetracionLubricantesM.assign(PenetracionT3A= penetracionLubricantesM["TURNO 3 ventas"]/ penetracionLubricantesM["TURNO 3"] )
penetracionLubricantesM = penetracionLubricantesM.assign(PenetracionTotalA=(penetracionLubricantesM["TURNO 1 ventas"] + penetracionLubricantesM["TURNO 2 ventas"]+penetracionLubricantesM["TURNO 3 ventas"]) /
 (penetracionLubricantesM["TURNO 1"] + penetracionLubricantesM["TURNO 2"] + penetracionLubricantesM["TURNO 3"]))
penetracionLubricantesM = penetracionLubricantesM.fillna(0)



#############################################   
######################### DIARIO
##############################################

############## DESPACHOS TOTALES


df_despachos = pd.read_sql(
'''
	
SELECT UEN,COUNT(DISTINCT ID) AS DESPACHOS,TURNO FROM Despapro 
WHERE FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
	and VOLUMEN < '60'
	GROUP BY UEN,TURNO 
''',db_conex)

#### Filtro y creo columnas Turno 1 ,2 y 3

df_pivot = pd.pivot_table(df_despachos, values='DESPACHOS', index=['UEN'], columns=['TURNO'], aggfunc=sum)

# Creamos las nuevas columnas 'turno 1', 'turno 2' y 'turno 3' y las llenamos con los datos de la tabla pivotada
df_pivot.columns = ['TURNO ' + str(col) for col in df_pivot.columns]
df_pivot.reset_index(inplace=True)

df_despachos=df_pivot

df_despachos = df_despachos.reset_index()


df_despachos = df_despachos.sort_values('UEN', ascending = True)

##### Elimino columna Turnos y Despachos ya que la informacion de estas columnas ahora la tengo en TURNO 1, TURNO 2 Y TURNO 3
df_despachos = df_despachos.loc[:,df_despachos.columns!="index"]

df_despachos = df_despachos.rename({'TURNO 1':'TURNO 1 diario','TURNO 2':'TURNO 2 diario','TURNO 3':'TURNO 3 diario'},axis=1)

df_despachos = df_despachos.fillna(0)
##### Agrupo por UEN
df_despachos = df_despachos.groupby(
        ["UEN"]
        , as_index=False
    ).sum()

###############VENTA LUBRICANTES DIARIA


ventaLubricantes = pd.read_sql('''

	    select DISTINCT(M.ID), M.UEN, M.TURNO, (M.CANTIDAD * P.Envase * -1) AS CANTIDAD from dbo.VMovDet as M
	left join dbo.PLProdUEN as P
	on M.CODPRODUCTO = P.CODIGO
	where P.AGRUPACION = 'LUBRICANTES'
	and M.TIPOMOVIM = 3
	and M.FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
	
 ''',db_conex)

# Utilizamos pivot_table para reorganizar el dataframe
df_pivot = pd.pivot_table(ventaLubricantes, values='CANTIDAD', index=['UEN'], columns=['TURNO'], aggfunc=sum)

# Creamos las nuevas columnas 'turno 1', 'turno 2' y 'turno 3' y las llenamos con los datos de la tabla pivotada
df_pivot.columns = ['TURNO ' + str(col) for col in df_pivot.columns]
df_pivot.reset_index(inplace=True)

ventaLubricantes=df_pivot

ventaLubricantes = ventaLubricantes.reset_index()


ventaLubricantes = ventaLubricantes.sort_values('UEN', ascending = True)

##### Elimino columna Turnos y Despachos ya que la informacion de estas columnas ahora la tengo en TURNO 1, TURNO 2 Y TURNO 3
ventaLubricantes = ventaLubricantes.loc[:,ventaLubricantes.columns!="index"]
ventaLubricantes = ventaLubricantes.loc[:,ventaLubricantes.columns!="ID"]
ventaLubricantes = ventaLubricantes.fillna(0)

##### Agrupo por UEN
ventaLubricantes = ventaLubricantes.groupby(
        ["UEN"]
        , as_index=False
    ).sum()

ventaLubricantes = ventaLubricantes.rename({'TURNO 1':'TURNO 1 ventas D','TURNO 2':'TURNO 2 ventas D','TURNO 3':'TURNO 3 ventas D'},axis=1)


penetracionLubricantes = ventaLubricantes.merge(df_despachos, on= ['UEN'], how= 'outer')
penetracionLubricantes=penetracionLubricantes.fillna(0)

penetracionLubricantes = penetracionLubricantes.assign(PenetracionT1= penetracionLubricantes["TURNO 1 ventas D"] / penetracionLubricantes["TURNO 1 diario"] )
penetracionLubricantes = penetracionLubricantes.assign(PenetracionT2= penetracionLubricantes["TURNO 2 ventas D"]/ penetracionLubricantes["TURNO 2 diario"] )
penetracionLubricantes = penetracionLubricantes.assign(PenetracionT3= penetracionLubricantes["TURNO 3 ventas D"]/ penetracionLubricantes["TURNO 3 diario"] )
penetracionLubricantes = penetracionLubricantes.assign(PenetracionTotal=(penetracionLubricantes["TURNO 1 ventas D"] + penetracionLubricantes["TURNO 2 ventas D"]+penetracionLubricantes["TURNO 3 ventas D"]) /
 (penetracionLubricantes["TURNO 1 diario"] + penetracionLubricantes["TURNO 2 diario"] + penetracionLubricantes["TURNO 3 diario"]))

penetracionLubricantes = penetracionLubricantes.convert_dtypes()
presuLubri = presuLubri.convert_dtypes()
penetracionLubricantes = penetracionLubricantes.merge(presuLubri, on='UEN', how='outer')
#penetracionLubricantes = pd.concat([penetracionLubricantes,presuLubri], axis=1)
penetracionLubricantes = penetracionLubricantes.groupby(
        ["UEN"]
        , as_index=False
    ).sum()

penetracionLubricantes = penetracionLubricantes.fillna(0)

####### CREO COLUMNAS DE DESVIOS
penetracionLubricantes = penetracionLubricantes.assign(DesvioT1= (penetracionLubricantes["PenetracionT1"] / penetracionLubricantes["Presupuesto Mensual"]) - 1)
penetracionLubricantes = penetracionLubricantes.assign(DesvioT2= (penetracionLubricantes["PenetracionT2"] / penetracionLubricantes["Presupuesto Mensual"]) - 1 )
penetracionLubricantes = penetracionLubricantes.assign(DesvioT3= (penetracionLubricantes["PenetracionT3"] / penetracionLubricantes["Presupuesto Mensual"]) - 1 )
penetracionLubricantes = penetracionLubricantes.assign(DesvioTotal= (penetracionLubricantes["PenetracionTotal"] / penetracionLubricantes["Presupuesto Mensual"]) - 1 )
penetracionLubricantes = penetracionLubricantes.groupby(
        ["UEN"]
        , as_index=False
    ).sum()

#penetracionLubricantes = penetracionLubricantes.style.format("{0:,.2%}", subset= ["PenetracionT1","PenetracionT2","Presupuesto Mensual","DesvioT1","DesvioT2"])



#####CONCATENO TABLAS

penetracionLubricantes = penetracionLubricantes.merge(penetracionLubricantesM, on='UEN', how='outer')



# BANDERA
def _bandera(uen):
    if uen in [
        "AZCUENAGA           "
        , "LAMADRID            "
        , "PERDRIEL            "
        , "PERDRIEL2           "
        , "PUENTE OLIVE        "
        , "SAN JOSE            "
        , 'YPF'
    ]:
        return "YPF"
    else:
        return "DAPSA"

# Create column BANDERA from column UEN
# Liquid Fuel
penetracionLubricantes["BANDERA"] = penetracionLubricantes.apply(
    lambda row: _bandera(row["UEN"])
        , axis= 1
)

#############################################################
###########CREO TOTALES
### Creo columna (fila) TOTALES YPF
ypf = penetracionLubricantes['BANDERA']=='YPF'
ypf = penetracionLubricantes[ypf]

ypf.loc["YPFS"]= pd.Series(
    ypf.sum()
    , index=['TURNO 1 ventas D','TURNO 2 ventas D','TURNO 3 ventas D','TURNO 1 ventas','TURNO 2 ventas','TURNO 3 ventas'
            ,'TURNO 1','TURNO 2','TURNO 3','TURNO 1 diario','TURNO 2 diario','TURNO 3 diario']
)
ypf.fillna({"UEN":"YPFA"}, inplace=True)



#Creo totales de PENETRACION DIARIA TURNO 1
penetracionT1YPF = (ypf.loc["YPFS","TURNO 1 ventas D"] /
    ypf.loc["YPFS","TURNO 1 diario"])

#Creo totales de PENETRACION DIARIA TURNO 2
penetracionT2YPF = (ypf.loc["YPFS","TURNO 2 ventas D"] /
    ypf.loc["YPFS","TURNO 2 diario"])
#Creo totales de PENETRACION DIARIA TURNO 3
penetracionT3YPF = (ypf.loc["YPFS","TURNO 3 ventas D"] /
    ypf.loc["YPFS","TURNO 3 diario"])


penetracionTotalYPF = ((ypf.loc["YPFS","TURNO 1 ventas D"]+ypf.loc["YPFS","TURNO 2 ventas D"]+ypf.loc["YPFS","TURNO 3 ventas D"]) /
    (ypf.loc["YPFS","TURNO 1 diario"]+ypf.loc["YPFS","TURNO 2 diario"]+ypf.loc["YPFS","TURNO 3 diario"]))

ypf.fillna({"PenetracionT1":penetracionT1YPF}, inplace=True)
ypf.fillna({"PenetracionT2":penetracionT2YPF}, inplace=True)
ypf.fillna({"PenetracionT3":penetracionT3YPF}, inplace=True)
ypf.fillna({"PenetracionTotal":penetracionTotalYPF}, inplace=True)


#Creo totales de PENETRACION ACUMULADA TURNO 1
penetracionT1AYPF = (ypf.loc["YPFS","TURNO 1 ventas"] /
    ypf.loc["YPFS","TURNO 1"])

#Creo totales de PENETRACION ACUMULADA TURNO 2 
penetracionT2AYPF = (ypf.loc["YPFS","TURNO 2 ventas"] /
    ypf.loc["YPFS","TURNO 2"])

#Creo totales de PENETRACION ACUMULADA TURNO 3 
penetracionT3AYPF = (ypf.loc["YPFS","TURNO 3 ventas"] /
    ypf.loc["YPFS","TURNO 3"])



penetracionTotalAYPF = ((ypf.loc["YPFS","TURNO 1 ventas"]+ypf.loc["YPFS","TURNO 2 ventas"]+ypf.loc["YPFS","TURNO 3 ventas"]) /
    (ypf.loc["YPFS","TURNO 1"]+ypf.loc["YPFS","TURNO 2"]+ypf.loc["YPFS","TURNO 3"]))


ypf.fillna({"PenetracionT1A":penetracionT1AYPF}, inplace=True)
ypf.fillna({"PenetracionT2A":penetracionT2AYPF}, inplace=True)
ypf.fillna({"PenetracionT3A":penetracionT3AYPF}, inplace=True)
ypf.fillna({"PenetracionTotalA":penetracionTotalAYPF}, inplace=True)

ypf.fillna({"Presupuesto Mensual":0.03}, inplace=True)
#Creo totales de DESVIO DIARIO TURNO 1 , 2 y 3


desvioT1YPF = ((ypf.loc["YPFS","PenetracionT1"] /
    ypf.loc["YPFS","Presupuesto Mensual"])-1)


desvioT2YPF = ((ypf.loc["YPFS","PenetracionT2"] /
    ypf.loc["YPFS","Presupuesto Mensual"]) -1)


desvioT3YPF = ((ypf.loc["YPFS","PenetracionT3"] /
    ypf.loc["YPFS","Presupuesto Mensual"])-1)


desvioTotalYPF = ((ypf.loc["YPFS","PenetracionTotal"] /
    ypf.loc["YPFS","Presupuesto Mensual"])-1)


ypf.fillna({"DesvioT1":desvioT1YPF}, inplace=True)
ypf.fillna({"DesvioT2":desvioT2YPF}, inplace=True)
ypf.fillna({"DesvioT3":desvioT3YPF}, inplace=True)
ypf.fillna({"DesvioTotal":desvioTotalYPF}, inplace=True)

#############################################################
###########CREO TOTALES
### Creo columna (fila) TOTALES DAPSA
dapsa = penetracionLubricantes['BANDERA']=='DAPSA'
dapsa = penetracionLubricantes[dapsa]
dapsa = dapsa.fillna(0)
dapsa.loc["DAPSA"]= pd.Series(
    dapsa.sum()
    , index=['TURNO 1 ventas D','TURNO 2 ventas D','TURNO 3 ventas D','TURNO 1 ventas','TURNO 2 ventas','TURNO 3 ventas'
            ,'TURNO 1','TURNO 2','TURNO 3','TURNO 1 diario','TURNO 2 diario','TURNO 3 diario']
)
dapsa.fillna({"UEN":"DAPSAA"}, inplace=True)

#Creo totales de PENETRACION DIARIA TURNO 1
penetracionT1DAP = (dapsa.loc["DAPSA","TURNO 1 ventas D"] /
    dapsa.loc["DAPSA","TURNO 1 diario"])

#Creo totales de PENETRACION DIARIA TURNO 2
penetracionT2DAP = (dapsa.loc["DAPSA","TURNO 2 ventas D"] /
    dapsa.loc["DAPSA","TURNO 2 diario"])
#Creo totales de PENETRACION DIARIA TURNO 3
penetracionT3DAP = (dapsa.loc["DAPSA","TURNO 3 ventas D"] /
    dapsa.loc["DAPSA","TURNO 3 diario"])


penetracionTotalDAP = ((dapsa.loc["DAPSA","TURNO 1 ventas D"]+dapsa.loc["DAPSA","TURNO 2 ventas D"]+dapsa.loc["DAPSA","TURNO 3 ventas D"]) /
    (dapsa.loc["DAPSA","TURNO 1 diario"]+dapsa.loc["DAPSA","TURNO 2 diario"]+dapsa.loc["DAPSA","TURNO 3 diario"]))

dapsa.fillna({"PenetracionT1":penetracionT1DAP}, inplace=True)
dapsa.fillna({"PenetracionT2":penetracionT2DAP}, inplace=True)
dapsa.fillna({"PenetracionT3":penetracionT3DAP}, inplace=True)
dapsa.fillna({"PenetracionTotal":penetracionTotalDAP}, inplace=True)


#Creo totales de PENETRACION ACUMULADA TURNO 1
penetracionT1ADAP = (dapsa.loc["DAPSA","TURNO 1 ventas"] /
    dapsa.loc["DAPSA","TURNO 1"])

#Creo totales de PENETRACION ACUMULADA TURNO 2 
penetracionT2ADAP = (dapsa.loc["DAPSA","TURNO 2 ventas"] /
    dapsa.loc["DAPSA","TURNO 2"])

#Creo totales de PENETRACION ACUMULADA TURNO 3 
penetracionT3ADAP = (dapsa.loc["DAPSA","TURNO 3 ventas"] /
    dapsa.loc["DAPSA","TURNO 3"])



penetracionTotalADAP = ((dapsa.loc["DAPSA","TURNO 1 ventas"]+dapsa.loc["DAPSA","TURNO 2 ventas"]+dapsa.loc["DAPSA","TURNO 3 ventas"]) /
    (dapsa.loc["DAPSA","TURNO 1"]+dapsa.loc["DAPSA","TURNO 2"]+dapsa.loc["DAPSA","TURNO 3"]))


dapsa.fillna({"PenetracionT1A":penetracionT1ADAP}, inplace=True)
dapsa.fillna({"PenetracionT2A":penetracionT2ADAP}, inplace=True)
dapsa.fillna({"PenetracionT3A":penetracionT3ADAP}, inplace=True)
dapsa.fillna({"PenetracionTotalA":penetracionTotalADAP}, inplace=True)

dapsa.fillna({"Presupuesto Mensual":0.015}, inplace=True)
#Creo totales de DESVIO DIARIO TURNO 1 , 2 y 3


desvioT1DAP = ((dapsa.loc["DAPSA","PenetracionT1"] /
    dapsa.loc["DAPSA","Presupuesto Mensual"])-1)


desvioT2DAP = ((dapsa.loc["DAPSA","PenetracionT2"] /
    dapsa.loc["DAPSA","Presupuesto Mensual"]) -1)


desvioT3DAP = ((dapsa.loc["DAPSA","PenetracionT3"] /
    dapsa.loc["DAPSA","Presupuesto Mensual"])-1)


desvioTotalDAP = ((dapsa.loc["DAPSA","PenetracionTotal"] /
    dapsa.loc["DAPSA","Presupuesto Mensual"])-1)


dapsa.fillna({"DesvioT1":desvioT1DAP}, inplace=True)
dapsa.fillna({"DesvioT2":desvioT2DAP}, inplace=True)
dapsa.fillna({"DesvioT3":desvioT3DAP}, inplace=True)
dapsa.fillna({"DesvioTotal":desvioTotalDAP}, inplace=True)






###########CREO TOTALES
### Creo columna (fila) TOTALES
penetracionLubricantes.loc["colTOTAL"]= pd.Series(
    penetracionLubricantes.sum()
    , index=['TURNO 1 ventas D','TURNO 2 ventas D','TURNO 3 ventas D','TURNO 1 ventas','TURNO 2 ventas','TURNO 3 ventas'
            ,'TURNO 1','TURNO 2','TURNO 3','TURNO 1 diario','TURNO 2 diario','TURNO 3 diario']
)
penetracionLubricantes.fillna({"UEN":"TOTAL"}, inplace=True)


#Creo totales de PENETRACION DIARIA TURNO 1
tasa = (penetracionLubricantes.loc["colTOTAL","TURNO 1 ventas D"] /
    penetracionLubricantes.loc["colTOTAL","TURNO 1 diario"])
penetracionLubricantes.fillna({"PenetracionT1":tasa}, inplace=True)

#Creo totales de PENETRACION DIARIA TURNO 2
tasa1 = (penetracionLubricantes.loc["colTOTAL","TURNO 2 ventas D"] /
    penetracionLubricantes.loc["colTOTAL","TURNO 2 diario"])
penetracionLubricantes.fillna({"PenetracionT2":tasa1}, inplace=True)
#Creo totales de PENETRACION DIARIA TURNO 3
tasa1 = (penetracionLubricantes.loc["colTOTAL","TURNO 3 ventas D"] /
    penetracionLubricantes.loc["colTOTAL","TURNO 3 diario"])
penetracionLubricantes.fillna({"PenetracionT3":tasa1}, inplace=True)


tasa2 = ((penetracionLubricantes.loc["colTOTAL","TURNO 1 ventas D"]+penetracionLubricantes.loc["colTOTAL","TURNO 2 ventas D"]+penetracionLubricantes.loc["colTOTAL","TURNO 3 ventas D"]) /
    (penetracionLubricantes.loc["colTOTAL","TURNO 1 diario"]+penetracionLubricantes.loc["colTOTAL","TURNO 2 diario"]+penetracionLubricantes.loc["colTOTAL","TURNO 3 diario"]))
penetracionLubricantes.fillna({"PenetracionTotal":tasa1}, inplace=True)


#Creo totales de PENETRACION ACUMULADA TURNO 1
tasa3 = (penetracionLubricantes.loc["colTOTAL","TURNO 1 ventas"] /
    penetracionLubricantes.loc["colTOTAL","TURNO 1"])
penetracionLubricantes.fillna({"PenetracionT1A":tasa3}, inplace=True)
#Creo totales de PENETRACION ACUMULADA TURNO 2 
tasa4 = (penetracionLubricantes.loc["colTOTAL","TURNO 2 ventas"] /
    penetracionLubricantes.loc["colTOTAL","TURNO 2"])
penetracionLubricantes.fillna({"PenetracionT2A":tasa4}, inplace=True)
#Creo totales de PENETRACION ACUMULADA TURNO 3 
tasa4 = (penetracionLubricantes.loc["colTOTAL","TURNO 3 ventas"] /
    penetracionLubricantes.loc["colTOTAL","TURNO 3"])
penetracionLubricantes.fillna({"PenetracionT3A":tasa4}, inplace=True)


tasa5 = ((penetracionLubricantes.loc["colTOTAL","TURNO 1 ventas"]+penetracionLubricantes.loc["colTOTAL","TURNO 2 ventas"]+penetracionLubricantes.loc["colTOTAL","TURNO 3 ventas"]) /
    (penetracionLubricantes.loc["colTOTAL","TURNO 1"]+penetracionLubricantes.loc["colTOTAL","TURNO 2"]+penetracionLubricantes.loc["colTOTAL","TURNO 3"]))
penetracionLubricantes.fillna({"PenetracionTotalA":tasa5}, inplace=True)


#Creo totales de DESVIO DIARIO TURNO 1 , 2 y 3

penetracionLubricantes.fillna({"Presupuesto Mensual":0.025}, inplace=True)
tasa10 = ((penetracionLubricantes.loc["colTOTAL","PenetracionT1"] /
    penetracionLubricantes.loc["colTOTAL","Presupuesto Mensual"])-1)
penetracionLubricantes.fillna({"DesvioT1":tasa10}, inplace=True)

tasa11 = ((penetracionLubricantes.loc["colTOTAL","PenetracionT2"] /
    penetracionLubricantes.loc["colTOTAL","Presupuesto Mensual"]) -1)
penetracionLubricantes.fillna({"DesvioT2":tasa11}, inplace=True)

tasa12 = ((penetracionLubricantes.loc["colTOTAL","PenetracionT3"] /
    penetracionLubricantes.loc["colTOTAL","Presupuesto Mensual"])-1)
penetracionLubricantes.fillna({"DesvioT3":tasa12}, inplace=True)

tasa7 = ((penetracionLubricantes.loc["colTOTAL","PenetracionTotal"] /
    penetracionLubricantes.loc["colTOTAL","Presupuesto Mensual"])-1)
penetracionLubricantes.fillna({"DesvioTotal":tasa7}, inplace=True)

'''
#Creo totales de DESVIO ACUMULADO TURNO 1 Y 2
tasa8 = (penetracionLubricantesM.loc["colTOTAL","PenetracionT1A"] /
    penetracionLubricantesM.loc["colTOTAL","PresupuestoA"])
penetracionLubricantesM.fillna({"DesvioT1A":tasa8}, inplace=True)

tasa9 = (penetracionLubricantesM.loc["colTOTAL","PenetracionT2A"] /
    penetracionLubricantesM.loc["colTOTAL","PresupuestoA"])
penetracionLubricantesM.fillna({"DesvioT2A":tasa9}, inplace=True)

tasa9 = (penetracionLubricantesM.loc["colTOTAL","PenetracionT3A"] /
    penetracionLubricantesM.loc["colTOTAL","PresupuestoA"])
penetracionLubricantesM.fillna({"DesvioT3A":tasa9}, inplace=True)

tasa9 = (penetracionLubricantesM.loc["colTOTAL","PenetracionTotalA"] /
    penetracionLubricantesM.loc["colTOTAL","PresupuestoA"])
penetracionLubricantesM.fillna({"DesvioTotalA":tasa9}, inplace=True)
'''




#ELIMINO COLUMNAS DE TURNOS
penetracionLubricantes = penetracionLubricantes.loc[:,penetracionLubricantes.columns!="TURNO 1"]
penetracionLubricantes = penetracionLubricantes.loc[:,penetracionLubricantes.columns!="TURNO 2"]
penetracionLubricantes = penetracionLubricantes.loc[:,penetracionLubricantes.columns!="TURNO 3"]

penetracionLubricantes = penetracionLubricantes.loc[:,penetracionLubricantes.columns!="TURNO 1 ventas"]
penetracionLubricantes = penetracionLubricantes.loc[:,penetracionLubricantes.columns!="TURNO 2 ventas"]
penetracionLubricantes = penetracionLubricantes.loc[:,penetracionLubricantes.columns!="TURNO 3 ventas"]

penetracionLubricantes = penetracionLubricantes.loc[:,penetracionLubricantes.columns!="TURNO 1 diario"]
penetracionLubricantes = penetracionLubricantes.loc[:,penetracionLubricantes.columns!="TURNO 2 diario"]
penetracionLubricantes = penetracionLubricantes.loc[:,penetracionLubricantes.columns!="TURNO 3 diario"]

penetracionLubricantes = penetracionLubricantes.loc[:,penetracionLubricantes.columns!="TURNO 1 ventas D"]
penetracionLubricantes = penetracionLubricantes.loc[:,penetracionLubricantes.columns!="TURNO 2 ventas D"]
penetracionLubricantes = penetracionLubricantes.loc[:,penetracionLubricantes.columns!="TURNO 3 ventas D"]


############# TABLA DE PENETRACION ONLY 
penetracionLubriOnly = penetracionLubricantes
penetracionLubriOnly= penetracionLubriOnly.loc[:,penetracionLubriOnly.columns!="DesvioT1"]
penetracionLubriOnly= penetracionLubriOnly.loc[:,penetracionLubriOnly.columns!="DesvioT2"]
penetracionLubriOnly= penetracionLubriOnly.loc[:,penetracionLubriOnly.columns!="DesvioT3"]
penetracionLubriOnly= penetracionLubriOnly.loc[:,penetracionLubriOnly.columns!="DesvioTotal"]


penetracionLubriOnly = penetracionLubriOnly.sort_values(['BANDERA','UEN'])
penetracionLubriOnly = penetracionLubriOnly.reset_index()
penetracionLubriOnly.loc[15,'UEN']='YPF'
#TOTALES YPF
penetracionLubriOnly.fillna({"PenetracionT1":penetracionT1YPF}, inplace=True)
penetracionLubriOnly.fillna({"PenetracionT2":penetracionT2YPF}, inplace=True)
penetracionLubriOnly.fillna({"PenetracionT3":penetracionT3YPF}, inplace=True)
penetracionLubriOnly.fillna({"PenetracionTotal":penetracionTotalYPF}, inplace=True)
penetracionLubriOnly.fillna({"Presupuesto Mensual":0.015}, inplace=True)

penetracionLubriOnly.fillna({"PenetracionT1A":penetracionT1AYPF}, inplace=True)
penetracionLubriOnly.fillna({"PenetracionT2A":penetracionT2AYPF}, inplace=True)
penetracionLubriOnly.fillna({"PenetracionT3A":penetracionT3AYPF}, inplace=True)
penetracionLubriOnly.fillna({"PenetracionTotalA":penetracionTotalAYPF}, inplace=True)

penetracionLubriOnly=penetracionLubriOnly.fillna(0)
#TOTALES DAPSA
penetracionLubriOnly.loc[16,'UEN']='DAPSA'


penetracionLubriOnly.fillna({"PenetracionT1":penetracionT1DAP}, inplace=True)
penetracionLubriOnly.fillna({"PenetracionT2":penetracionT2DAP}, inplace=True)
penetracionLubriOnly.fillna({"PenetracionT3":penetracionT3DAP}, inplace=True)
penetracionLubriOnly.fillna({"PenetracionTotal":penetracionTotalDAP}, inplace=True)
penetracionLubriOnly.fillna({"Presupuesto Mensual":0.03}, inplace=True)
penetracionLubriOnly.fillna({"PenetracionT1A":penetracionT1ADAP}, inplace=True)
penetracionLubriOnly.fillna({"PenetracionT2A":penetracionT2ADAP}, inplace=True)
penetracionLubriOnly.fillna({"PenetracionT3A":penetracionT3ADAP}, inplace=True)
penetracionLubriOnly.fillna({"PenetracionTotalA":penetracionTotalADAP}, inplace=True)

penetracionLubriOnly = penetracionLubriOnly.loc[:,penetracionLubriOnly.columns!="BANDERA"]
penetracionLubriOnly = penetracionLubriOnly.loc[:,penetracionLubriOnly.columns!="index"]

penetracionLubriOnly.index=[1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,9,0]
penetracionLubriOnly = penetracionLubriOnly.sort_index()


penetracionLubriOnly = penetracionLubriOnly.rename({'UEN': 'UEN', 'PenetracionT1': 'Penetracion Diaria TURNO 1'
, 'PenetracionT2': 'Penetracion Diaria TURNO 2','PenetracionT3': 'Penetracion Diaria TURNO 3','PenetracionTotal': 'Penetracion Total Diaria'
,'PenetracionT1A':'Penetracion Acumulada TURNO 1','PenetracionT2A':'Penetracion Acumulada TURNO 2','PenetracionT3A':'Penetracion Acumulada TURNO 3','PenetracionTotalA':'Penetracion Total Acumulada'}, axis=1)

penetracionLubriOnly = penetracionLubriOnly.loc[:,penetracionLubriOnly.columns!="level_0"]


########### TABLA PENETRACION Y DESVIO DIARIO #######
penetracionDiaria=penetracionLubricantes

penetracionDiaria= penetracionDiaria.loc[:,penetracionDiaria.columns!="PenetracionT1A"]
penetracionDiaria= penetracionDiaria.loc[:,penetracionDiaria.columns!="PenetracionT2A"]
penetracionDiaria= penetracionDiaria.loc[:,penetracionDiaria.columns!="PenetracionT3A"]
penetracionDiaria= penetracionDiaria.loc[:,penetracionDiaria.columns!="PenetracionTotalA"]

penetracionDiaria = penetracionDiaria.sort_values(['BANDERA','UEN'])
penetracionDiaria = penetracionDiaria.reset_index()
penetracionDiaria.loc[15,'UEN']='YPF'
#TOTALES YPF
penetracionDiaria.fillna({"PenetracionT1":penetracionT1YPF}, inplace=True)
penetracionDiaria.fillna({"PenetracionT2":penetracionT2YPF}, inplace=True)
penetracionDiaria.fillna({"PenetracionT3":penetracionT3YPF}, inplace=True)
penetracionDiaria.fillna({"PenetracionTotal":penetracionTotalYPF}, inplace=True)
penetracionDiaria.fillna({"Presupuesto Mensual":0.03}, inplace=True)
penetracionDiaria.fillna({"DesvioT1":desvioT1YPF}, inplace=True)
penetracionDiaria.fillna({"DesvioT2":desvioT2YPF}, inplace=True)
penetracionDiaria.fillna({"DesvioT3":desvioT3YPF}, inplace=True)
penetracionDiaria.fillna({"DesvioTotal":desvioTotalYPF}, inplace=True)

penetracionDiaria=penetracionDiaria.fillna(0)
#TOTALES YPF
penetracionDiaria.loc[16,'UEN']='DAPSA'


penetracionDiaria.fillna({"PenetracionT1":penetracionT1DAP}, inplace=True)
penetracionDiaria.fillna({"PenetracionT2":penetracionT2DAP}, inplace=True)
penetracionDiaria.fillna({"PenetracionT3":penetracionT3DAP}, inplace=True)
penetracionDiaria.fillna({"PenetracionTotal":penetracionTotalDAP}, inplace=True)
penetracionDiaria.fillna({"Presupuesto Mensual":0.015}, inplace=True)
penetracionDiaria.fillna({"DesvioT1":desvioT1DAP}, inplace=True)
penetracionDiaria.fillna({"DesvioT2":desvioT2DAP}, inplace=True)
penetracionDiaria.fillna({"DesvioT3":desvioT3DAP}, inplace=True)
penetracionDiaria.fillna({"DesvioTotal":desvioTotalDAP}, inplace=True)

penetracionDiaria = penetracionDiaria.loc[:,penetracionDiaria.columns!="BANDERA"]
penetracionDiaria = penetracionDiaria.loc[:,penetracionDiaria.columns!="index"]

penetracionDiaria.index=[1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,9,0]
penetracionDiaria = penetracionDiaria.sort_index()
penetracionDiaria = penetracionDiaria.rename({'UEN': 'UEN', 'PenetracionT1': 'Penetracion Diaria TURNO 1'
, 'PenetracionT2': 'Penetracion Diaria TURNO 2','PenetracionT3': 'Penetracion Diaria TURNO 3','PenetracionTotal': 'Penetracion Total Diaria'
,'Presupuesto Mensual':'Presupuesto Diario','DesvioT1':'Desvio Diario TURNO 1','DesvioT2':'Desvio Diario TURNO 2','DesvioT3':'Desvio Diario TURNO 3','DesvioTotal':'Desvio Diario Total'}, axis=1)

penetracionDiaria = penetracionDiaria.loc[:,penetracionDiaria.columns!="level_0"]

penetracionLubriOnly=penetracionLubriOnly.drop('Penetracion Diaria TURNO 1',axis='columns' )
penetracionLubriOnly=penetracionLubriOnly.drop('Penetracion Diaria TURNO 2',axis='columns' )
penetracionLubriOnly=penetracionLubriOnly.drop('Penetracion Diaria TURNO 3',axis='columns' )
penetracionLubriOnly=penetracionLubriOnly.drop('Penetracion Total Diaria',axis='columns' )


penetracionLubriOnly['Desvio']=(penetracionLubriOnly['Penetracion Total Acumulada']/penetracionLubriOnly['Presupuesto Mensual'])-1
    



def _estiladorVtaTituloOnly(df, list_Col_Num, list_Col_Perc, titulo):
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
        .format("{:,.3%}", subset=list_Col_Perc) \
        .hide(axis=0) \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>") \
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
        .apply(lambda x: ["background: black" if x.name in [
            df.index[-1]
            , df.index[0]
            , df.index[9]
        ]
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name in [
            df.index[-1]
            , df.index[0]
            , df.index[9]
        ]
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["font-size: 15px" if x.name in [
            df.index[-1]
            , df.index[0]
            , df.index[9]
        ]
            else "" for i in x]
            , axis=1)  
        
    evitarTotales = df.index.get_level_values(0)
    
    resultado = resultado.background_gradient(
        cmap="Dark2" # Red->Yellow->Green
        ,vmin=-0.0048
        ,vmax=0.0001
        ,subset=pd.IndexSlice[evitarTotales[:-1],"Presupuesto Mensual"])
    
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


numCols = [ 
         ]

percColsPen = [
     "Presupuesto Mensual"
    ,"Penetracion Acumulada TURNO 1"
	,"Penetracion Acumulada TURNO 2"
    ,"Penetracion Acumulada TURNO 3"
    ,"Penetracion Total Acumulada"
    ,"Desvio"
]
percCols = [
	 "Desvio Diario TURNO 1"
	,"Desvio Diario TURNO 2"
    ,"Penetracion Acumulada TURNO 1"
	,"Penetracion Acumulada TURNO 2"
	,"Presupuesto Acumulado"
	,"Desvio Acumulado TURNO 1"
	,"Desvio Acumulado TURNO 2"
    ,"Desvio"
]

percColsDiaria = [
    "Penetracion Diaria TURNO 1",
    "Penetracion Diaria TURNO 2",
    "Penetracion Diaria TURNO 3",
    "Desvio Diario TURNO 1"
	,"Desvio Diario TURNO 2"
    ,"Desvio Diario TURNO 3"
    ,"Desvio Diario Total"
    ,"Presupuesto Diario"
    
]


#penetracionLubriOnly=penetracionLubriOnly.drop('Penetracion Diaria TURNO 1',axis='columns' )
penetracionLubriOnly=penetracionLubriOnly.reindex(columns=['UEN',  'Penetracion Acumulada TURNO 1', 'Penetracion Acumulada TURNO 2', 'Penetracion Acumulada TURNO 3','Penetracion Total Acumulada','Presupuesto Mensual', 'Desvio' ])

#penetracionDiaria = _estiladorVtaTituloD(penetracionDiaria,numCols,percColsDiaria, "EJECUCION LUBRICANTES PRESUPUESTADO DIARIO")
penetracionLubriOnly = _estiladorVtaTituloOnly(penetracionLubriOnly,numCols,percColsPen, "INFO PENETRACION LUBRICANTES")

#ubicacion = "C:/Informes/Penetracion Lubricantes/"
ubicacion= str(pathlib.Path(__file__).parent)+"\\"
#nombrePen = "Info_PenetracionLubri_Diario_________.png"
nombrePenDiario = "Info_Penetracion_Lubri_Acumulado.png"

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

#df_to_image(penetracionDiaria, ubicacion, nombrePen)
df_to_image(penetracionLubriOnly,ubicacion,nombrePenDiario)
#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)












