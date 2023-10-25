import os
import math
import numpy as np
from DatosLogin import login
from Conectores import conectorMSSQL
from PIL import Image
from openpyxl import load_workbook
import pandas as pd
from pandas.api.types import CategoricalDtype
import dataframe_image as dfi
import pyodbc #Library to connect to Microsoft SQL Server
from DatosLogin import login #Holds connection data to the SQL Server
import sys
import pathlib
from decimal import Decimal
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

##########################   DESPACHOS ACUMULADOS MENSUAL por Empleado

df_despachosM = pd.read_sql( '''  
      	DECLARE @inicioMesActual DATETIME
	SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, CURRENT_TIMESTAMP), 0)

	DECLARE @inicioMesAnterior DATETIME
	SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

	--Divide por la cant de días del mes anterior y multiplica por la cant de días del
	--mes actual
	
	DECLARE @hoy DATETIME
	SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

	--Divide por la cantidad de días cursados del mes actual y multiplica por la cant
	--de días del mes actual

    
	SELECT 
	
       D.UEN,count(distinct D.ID) as 'Despachos Totales',P.NOMBRE as 'Apellido y Nombre'
    FROM [Rumaos].[dbo].[VIEW_DESPAPRO_FILTRADA] as D
			join PersonalUEN as P
			on D.UEN = P.UEN
			and D.CodPersonal = P.CODPERSONAL
    WHERE 
		
		 D.FECHASQL >= @inicioMesActual
		and	D.FECHASQL < @hoy
        and D.CODPRODUCTO not like 'GNC'
		and D.UEN IN('LAMADRID'
            ,'AZCUENAGA'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
            ,'SAN JOSE')

		group by d.UEN,p.Nombre
		order by d.UEN



 '''  ,db_conex)
df_despachosM = df_despachosM.convert_dtypes()


############### VENTA Con App YPF por Empleado 


df_VentasYPFAPP = pd.read_sql('''
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
        
SELECT count(distinct Y.ID) AS 'Despachos APPYPF', Y.UEN, P.NOMBRE AS 'Apellido y Nombre'
  FROM [MercadoPago].[dbo].[Transacciones]  as Y
  inner join [Rumaos].[dbo].[VIEW_DESPAPRO_FILTRADA] as D
  on Y.UEN = D.UEN
  AND Y.IDDESPACHO = D.IDMOVIM
  join [Rumaos].[dbo].[PersonalUEN] as P
  on D.UEN = P.UEN
  and D.CodPersonal = P.CODPERSONAL
    where
	D.FECHASQL >= @inicioMesActual
	and	D.FECHASQL < @hoy
    and D.CODPRODUCTO not like 'GNC'
	and AppYPF = 1
	group by Y.UEN,P.NOMBRE

  '''      ,db_conex)
df_VentasYPFAPP = df_VentasYPFAPP.convert_dtypes()

## Concateno Tablas De Despachos Totales Con Despachos con App YPF
df_VentasYPFAPP = df_VentasYPFAPP.merge(df_despachosM,on=['UEN','Apellido y Nombre'],how='outer')
df_VentasYPFAPP = df_VentasYPFAPP.fillna(0)
## Creo columna de Penetracion App YPF
df_VentasYPFAPP['Penetracion App YPF'] = df_VentasYPFAPP['Despachos APPYPF'] / df_VentasYPFAPP['Despachos Totales']
df_VentasYPFAPP = df_VentasYPFAPP.fillna(0)
## Ordeno en Funcion  a UEN
df_VentasYPFAPP = df_VentasYPFAPP.sort_values('UEN')
df_VentasYPFAPP = df_VentasYPFAPP.reset_index()
## Reordeno Columnas
df_VentasYPFAPP=df_VentasYPFAPP.reindex(columns=["UEN", 'Apellido y Nombre','Despachos APPYPF','Despachos Totales','Penetracion App YPF'])
## Le Quito El nombre de La UEN cuando este Repetido para crear una especie de subrgrupo
x=0
q=0
w=0
r=0
t=0
y=0
for i in df_VentasYPFAPP.index:
    if df_VentasYPFAPP.loc[i,'UEN']=='AZCUENAGA           ':
        x = x+1
        if x!=1:
            df_VentasYPFAPP.loc[i,'UEN']=""
    elif df_VentasYPFAPP.loc[i,'UEN']=='PERDRIEL            ':
        q = q +1
        if q !=1:
            df_VentasYPFAPP.loc[i,'UEN']=""
    elif df_VentasYPFAPP.loc[i,'UEN']=='PERDRIEL2           ':
        w  = w +1
        if w !=1:
            df_VentasYPFAPP.loc[i,'UEN']=""
    elif df_VentasYPFAPP.loc[i,'UEN']=='PUENTE OLIVE        ':
        r  = r +1
        if r !=1:
            df_VentasYPFAPP.loc[i,'UEN']=""
    elif df_VentasYPFAPP.loc[i,'UEN']=='SAN JOSE            ':
        t  = t +1
        if t !=1:
            df_VentasYPFAPP.loc[i,'UEN']=""
    elif df_VentasYPFAPP.loc[i,'UEN']=='LAMADRID            ':
        y  = y +1
        if y !=1:
            df_VentasYPFAPP.loc[i,'UEN']=""

## Redondeo el numero a 3 decimales 
df_VentasYPFAPP['Penetracion App YPF']=pd.Series([round(val,3) for val in df_VentasYPFAPP['Penetracion App YPF']])
#creo totales
df_VentasYPFAPP.loc["colTOTAL"]= pd.Series(
    df_VentasYPFAPP.sum()
    , index=['Despachos APPYPF',"Despachos Totales"]
)
df_VentasYPFAPP.fillna({"UEN":"TOTAL"}, inplace=True)
#Creo totales de Penetracion AppYPF
tasa = (df_VentasYPFAPP.loc["colTOTAL","Despachos APPYPF"] /
    df_VentasYPFAPP.loc["colTOTAL","Despachos Totales"])
df_VentasYPFAPP.fillna({'Penetracion App YPF':tasa}, inplace=True)

## Elimino columna de Despachos App YPF ya que no entrara en el reporte
df_VentasYPFAPP = df_VentasYPFAPP.loc[:,df_VentasYPFAPP.columns!="Despachos APPYPF"]

## Creo el estilador del Excel
def _estiladorVtaTitulo(
    df:pd.DataFrame
    , list_Col_Num=[]
    , list_Col_Perc=[]
    , titulo=""
    ):
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
        .format("{:,.1%}", subset=list_Col_Perc) \
        .hide_index() \
        .set_caption(
            titulo
            + " "
            + (pd.to_datetime("today")
            .strftime("%d/%m/%y"))
        ) \
        .set_properties(subset=list_Col_Num + list_Col_Perc
            , **{"text-align": "center", "width": "80px"}) \
        .set_properties(border= "2px solid black") \
        .set_properties(**{'font-weight': "bold"}, 
                         subset=('UEN'))\
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
                    ,("font-size", "14px")
                ]
            }
        ]) \
        .apply(lambda x: ["background-color: black" if x.name == df.index[-1] 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == df.index[-1]
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["font-size: 15px" if x.name == df.index[-1]
            else "" for i in x]
            , axis=1)

    return resultado

## Defino ubicacion y nombre del archivo
ubicacion = "C:/Informes/PenetracionAPPYPF/"
nombrePen = "Penetracion_AppYPF_Emp.xlsx"
## Le aplico el estilador al dataframe
df_ctas_Para_Excel_Estilo = _estiladorVtaTitulo(
    df_VentasYPFAPP
    ,['Despachos Totales']
    ,['Penetracion App YPF']
)

# Entro a donde se guardo el Excel y Modifico Con los Datos Actuales
writer = pd.ExcelWriter(ubicacion + nombrePen
    , engine="xlsxwriter"
)

df_ctas_Para_Excel_Estilo.to_excel(
    writer
    , sheet_name="Penetracion App YPF"
    , header=True
    , index=False
)

worksheet = writer.sheets["Penetracion App YPF"]

# Ajusto El tamaño de cada Columna
for column in df_VentasYPFAPP:
    column_length = max(
        df_VentasYPFAPP[column].astype(str).map(len).max()
        , len(column)
    )
    col_idx = df_VentasYPFAPP.columns.get_loc(column)
    worksheet.set_column(
        col_idx
        , col_idx
        , column_length + 1
    )

writer.save()

#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)


