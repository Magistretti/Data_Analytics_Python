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
################ MENSUAL ACUMULADO
########################################

##########################   DESPACHOS MENSUAL

######### Despachos Ultimos 7 Dias
df_despachosS = pd.read_sql('''
	
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


    select count(distinct ID) as 'Cantidad de Despachos' from VIEW_DESPAPRO_FILTRADA
    where  FECHASQL >= DATEADD(day, -7, CAST(GETDATE() AS date))
            and	FECHASQL < @hoy
    
    ''' ,db_conex)
df_despachosS.loc[0,'Semana']='Ultimos 7 Dias'


######### Despachos 8 a 14 dias atras
df_despachosS2 = pd.read_sql('''
	
    select count(distinct ID) as 'Cantidad de Despachos' from VIEW_DESPAPRO_FILTRADA
    where  FECHASQL >= DATEADD(day, -14, CAST(GETDATE() AS date))
            and	FECHASQL < DATEADD(day, -7, CAST(GETDATE() AS date)) 
    
    ''' ,db_conex)

df_despachosS2.loc[0,'Semana']='Entre 8 y 14 dias Previos'

######### Despachos 15 a 21 dias atras
df_despachosS3 = pd.read_sql('''
	
    select count(distinct ID) as 'Cantidad de Despachos' from VIEW_DESPAPRO_FILTRADA
    where  FECHASQL >= DATEADD(day, -21, CAST(GETDATE() AS date))
            and	FECHASQL < DATEADD(day, -14, CAST(GETDATE() AS date)) 
    
    ''' ,db_conex)

df_despachosS3.loc[0,'Semana']='Entre 15 y 21 dias Previos'
######### Despachos 22 a 28 dias atras
df_despachosS4 = pd.read_sql('''
	
    select count(distinct ID) as 'Cantidad de Despachos' from VIEW_DESPAPRO_FILTRADA
    where  FECHASQL >= DATEADD(day, -28, CAST(GETDATE() AS date))
            and	FECHASQL < DATEADD(day, -21, CAST(GETDATE() AS date)) 
    
    ''' ,db_conex)
df_despachosS4.loc[0,'Semana']='Entre 22 y 28 dias Previos'

### Concateno Las 3 tablas de Despachos Para cada rango de fecha
df_despachos = df_despachosS.merge(df_despachosS2, on=['Semana','Cantidad de Despachos'],how='outer')
df_despachos = df_despachos.merge(df_despachosS3,on=['Semana','Cantidad de Despachos'],how='outer')
df_despachos = df_despachos.merge(df_despachosS4,on=['Semana','Cantidad de Despachos'],how='outer')






##########################   Operaciones Con Red PAgo MENSUAL

######### RED PAGO Ultimos 7 Dias
df_redPagoS = pd.read_sql('''
	
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

		select count(distinct W.ID) as 'Cant Op con RedPago'
	from TransacEWallet AS W join VIEW_DESPAPRO_FILTRADA as D 
	on W.IDDespacho = D.ID
	AND W.UEN = D.UEN
	AND W.TipoComp = D.TIPOCOMP
	AND W.LetraComp = D.LETRACOMP
	AND W.NroComp = D.NROCOMP
    And W.PtoVta = D.PTOVTA
    AND W.FECHATUR = D.FECHASQL
	where W.FECHATUR >= DATEADD(day, -7, CAST(GETDATE() AS date))
		and	W.FECHATUR < @hoy
		and W.Departamento = 'PLAYA'
		AND W.TipoComp like 'FAC' 
    
    ''' ,db_conex)
df_redPagoS.loc[0,'Semana']='Ultimos 7 Dias'

######### RED PAGO 8 a 14 dias atras
df_redPagoS1 = pd.read_sql('''


	select count(distinct W.ID) as 'Cant Op con RedPago'
	from TransacEWallet AS W join VIEW_DESPAPRO_FILTRADA as D 
	on W.IDDespacho = D.ID
	AND W.UEN = D.UEN
	AND W.TipoComp = D.TIPOCOMP
	AND W.LetraComp = D.LETRACOMP
	AND W.NroComp = D.NROCOMP
    And W.PtoVta = D.PTOVTA
    AND W.FECHATUR = D.FECHASQL
	where W.FECHATUR >= DATEADD(day, -14, CAST(GETDATE() AS date))
		and	W.FECHATUR < DATEADD(day, -7, CAST(GETDATE() AS date))
		and W.Departamento = 'PLAYA'
		AND W.TipoComp like 'FAC' 

    ''' ,db_conex)
df_redPagoS1.loc[0,'Semana']='Entre 8 y 14 dias Previos'

######### RED PAGO 15 a 21 dias atras
df_redPagoS2 = pd.read_sql('''
	select count(distinct W.ID) as 'Cant Op con RedPago'
	from TransacEWallet AS W join VIEW_DESPAPRO_FILTRADA as D 
	on W.IDDespacho = D.ID
	AND W.UEN = D.UEN
	AND W.TipoComp = D.TIPOCOMP
	AND W.LetraComp = D.LETRACOMP
	AND W.NroComp = D.NROCOMP
    And W.PtoVta = D.PTOVTA
    AND W.FECHATUR = D.FECHASQL
	where W.FECHATUR >= DATEADD(day, -21, CAST(GETDATE() AS date))
		and	W.FECHATUR < DATEADD(day, -14, CAST(GETDATE() AS date))
		and W.Departamento = 'PLAYA'
		AND W.TipoComp like 'FAC' 

    ''' ,db_conex)
df_redPagoS2.loc[0,'Semana']='Entre 15 y 21 dias Previos'

######### RED PAGO 22 a 28 dias atras
df_redPagoS3 = pd.read_sql('''
	select count(distinct W.ID) as 'Cant Op con RedPago'
	from TransacEWallet AS W join VIEW_DESPAPRO_FILTRADA as D 
	on W.IDDespacho = D.ID
	AND W.UEN = D.UEN
	AND W.TipoComp = D.TIPOCOMP
	AND W.LetraComp = D.LETRACOMP
	AND W.NroComp = D.NROCOMP
    And W.PtoVta = D.PTOVTA
    AND W.FECHATUR = D.FECHASQL
	where W.FECHATUR >= DATEADD(day, -28, CAST(GETDATE() AS date))
		and	W.FECHATUR < DATEADD(day, -21, CAST(GETDATE() AS date))
		and W.Departamento = 'PLAYA'
		AND W.TipoComp like 'FAC' 
    ''' ,db_conex)
df_redPagoS3.loc[0,'Semana']='Entre 22 y 28 dias Previos'

### Concateno las 3 tablas de Operaciones Con RedPago 
df_redPago = df_redPagoS.merge(df_redPagoS1, on=['Semana','Cant Op con RedPago'],how='outer')
df_redPago = df_redPago.merge(df_redPagoS2,on=['Semana','Cant Op con RedPago'],how='outer')
df_redPago = df_redPago.merge(df_redPagoS3,on=['Semana','Cant Op con RedPago'],how='outer')

### Concateno Tabla de Despachos Con RedPago
df_PenRedPago= df_redPago.merge(df_despachos,on='Semana',how='outer')


### Creo Columna de Penetracion RedPago
df_PenRedPago["Penetracion %"]= df_PenRedPago['Cant Op con RedPago'] / df_PenRedPago['Cantidad de Despachos']

### Creo TOTALES

df_PenRedPago.loc["colTOTAL"]= pd.Series(
    df_PenRedPago.sum()
    , index=["Cantidad de Despachos","Cant Op con RedPago"]
)
df_PenRedPago.fillna({"Semana":"TOTAL"}, inplace=True)

tasa = (df_PenRedPago.loc["colTOTAL","Cant Op con RedPago"] /
        df_PenRedPago.loc["colTOTAL","Cantidad de Despachos"])
df_PenRedPago.fillna({"Penetracion %":tasa}, inplace=True)


df_PenRedPago=df_PenRedPago.reindex(columns=['Semana','Cant Op con RedPago','Cantidad de Despachos','Penetracion %'])



###### Estilador de la imagen

def _estiladorVtaTituloP(df,list_Col_Num0, list_Col_Num, list_Col_Perc, titulo):
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
        .format("{0:,.0f}", subset=list_Col_Num0) \
        .format("{0:,.2f}", subset=list_Col_Num) \
        .format("{:,.2%}", subset=list_Col_Perc) \
        .hide_index() \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>") \
        .set_properties(subset= list_Col_Perc + list_Col_Num + list_Col_Num0
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

#Columnas Sin decimales
numCols0 = ["Cant Op con RedPago"
            , "Cantidad de Despachos"

]

##Columnas con decimales

numCols = [ 
         ]

## Columnas porcentajes
percColsPen = [ "Penetracion %"
]

######### Aplico a la tabla el formato
df_PenRedPago = _estiladorVtaTituloP(df_PenRedPago,numCols0,numCols,percColsPen, "RedPago Playa - Penetracion en Cantidad de Operaciones")



ubicacion = "C:/Informes/Red Pago//"
nombreDespachos = "PenRedPagoCantidad.png"

def df_to_image(df, ubicacion, nombreDespachos):
   
   
    if os.path.exists(ubicacion+nombreDespachos):
        os.remove(ubicacion+nombreDespachos)
        dfi.export(df, ubicacion+nombreDespachos)
    else:
        dfi.export(df, ubicacion+nombreDespachos)


df_to_image(df_PenRedPago, ubicacion, nombreDespachos)


# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo 1er Despachos Camioneros"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)



