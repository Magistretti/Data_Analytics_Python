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



vtasSalon = pd.read_sql('''
	
    SELECT A.UEN, COUNT(DISTINCT A.DESPACHOS) AS ID, SUM(A.IMPORTE) AS 'TOTAL VENDIDO' FROM(
    SELECT ID AS DESPACHOS, UEN, IMPORTE
    FROM [Rumaos].[dbo].[SCEgreso] where
    FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
    ) AS A GROUP BY  UEN

    ''' ,db_conex)


vtasSalon = vtasSalon.convert_dtypes()
vtasSalon = vtasSalon.reset_index()


vtasSalon = vtasSalon.assign(Promedio= vtasSalon['TOTAL VENDIDO'] / vtasSalon['ID'])

vtasSalonM = pd.read_sql('''
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
	SELECT A.UEN, COUNT(DISTINCT A.DESPACHOS) AS ID, SUM(A.IMPORTE) AS 'TOTAL VENDIDO' FROM(
	SELECT ID AS DESPACHOS, UEN, IMPORTE
    FROM [Rumaos].[dbo].[SCEgreso] where
  	FECHASQL >= @inicioMesActual
	and	FECHASQL < @hoy
	and UEN not like 'MERC GUAYMALLEN'
    ) AS A GROUP BY  UEN

    ''' ,db_conex)
vtasSalonM = vtasSalonM.convert_dtypes()
vtasSalonM = vtasSalonM.reset_index()

vtasSalonM = vtasSalonM.assign(PromedioM= vtasSalonM['TOTAL VENDIDO'] / vtasSalonM['ID'])
vtasSalonM = vtasSalonM.loc[:,vtasSalonM.columns!="index"]
vtasSalon = vtasSalon.loc[:,vtasSalon.columns!="index"]


vtasSalonT = vtasSalon.merge(vtasSalonM, on='UEN',how='outer')


########### OBJETIVO

objetivo = pd.read_sql('''
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
	SELECT A.UEN, COUNT(DISTINCT A.DESPACHOS) AS IDH, SUM(A.IMPORTE) AS 'TOTAL VENDIDOH' FROM(
	SELECT ID AS DESPACHOS, UEN, IMPORTE
    FROM [Rumaos].[dbo].[SCEgreso] where
  	FECHASQL >= @inicioMesAnterior
	and	FECHASQL < @inicioMesActual
	and UEN not like 'MERC GUAYMALLEN'
    ) AS A GROUP BY  UEN

    ''' ,db_conex)
objetivo = objetivo.convert_dtypes()
objetivo = objetivo.reset_index()

objetivo = objetivo.assign(Objetivo= (objetivo['TOTAL VENDIDOH'] / objetivo['IDH'])*1.15)
objetivo = objetivo.loc[:,objetivo.columns!="index"]
objetivo = objetivo.loc[:,objetivo.columns!="index"]

vtasSalonT = vtasSalonT.merge(objetivo, on='UEN',how='outer')

vtasSalonT['Desvio %']=vtasSalonT['PromedioM']/vtasSalonT['Objetivo'] - 1



vtasSalonT.loc["colTOTAL"]= pd.Series(
    vtasSalonT.sum()
    , index=["ID_x","TOTAL VENDIDO_x","ID_y"
    ,"TOTAL VENDIDO_y","TOTAL VENDIDOH","IDH"]
)
vtasSalonT.fillna({"UEN":"TOTAL"}, inplace=True)

tasa7 = (vtasSalonT.loc["colTOTAL","TOTAL VENDIDO_x"] /
 vtasSalonT.loc["colTOTAL","ID_x"])
vtasSalonT.fillna({"Promedio":tasa7}, inplace=True)

tasa1 = (vtasSalonT.loc["colTOTAL","TOTAL VENDIDO_y"] /
 vtasSalonT.loc["colTOTAL","ID_y"])
vtasSalonT.fillna({"PromedioM":tasa1}, inplace=True)

tasaH = (vtasSalonT.loc["colTOTAL","TOTAL VENDIDOH"] /
 vtasSalonT.loc["colTOTAL","IDH"])*1.15
vtasSalonT.fillna({"Objetivo":tasaH}, inplace=True)

tasaH = (vtasSalonT.loc["colTOTAL","PromedioM"] /
 vtasSalonT.loc["colTOTAL","Objetivo"]) -1
vtasSalonT.fillna({"Desvio %":tasaH}, inplace=True)



vtasSalonT = vtasSalonT.loc[:,vtasSalonT.columns!="TOTAL VENDIDOH"]
vtasSalonT = vtasSalonT.loc[:,vtasSalonT.columns!="IDH"]


vtasSalonT = vtasSalonT.rename({"UEN":"UEN","ID_x":"Tickets Diario","TOTAL VENDIDO_x":"Total Vendido Diario $"
,"Promedio":"Promedio Ticket Diario $","ID_y":"Tickets Acumulado","TOTAL VENDIDO_y":"Total Vendido Acumulado $"
,"PromedioM":"Promedio Ticket Acumulado $","Objetivo":"Objetivo",'Desvio %':'Desvio %'}, axis=1)

vtasSalonT = vtasSalonT[vtasSalonT["UEN"] != 'MERCADO 2           ']

vtasSalonT=vtasSalonT.drop('Tickets Diario', axis='columns')
vtasSalonT=vtasSalonT.drop('Total Vendido Diario $', axis='columns')
vtasSalonT=vtasSalonT.drop('Promedio Ticket Diario $', axis='columns')





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
        .hide(axis=0) \
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
    evitarTotales = df.index.get_level_values(0)
    resultado = resultado.background_gradient(
        cmap="Dark2" # Red->Yellow->Green
        ,vmin=0.001
        ,vmax=0.007
        ,subset=pd.IndexSlice["Objetivo"]
    )

    
    subset_columns = pd.IndexSlice[evitarTotales[:-1], ['Desvio %']]

    resultado= resultado.applymap(table_color,subset=subset_columns)
    return resultado

def table_color(val):
    """
    Takes a scalar and returns a string with
    the css property `'color: red'` for less than 60 marks, green otherwise.
    """
    color = 'blue' if val > 0 else 'red'
    return 'color: % s' % color

numCols0 = ["Total Vendido Acumulado $"
        ,"Tickets Acumulado"]

numCols = [ "Promedio Ticket Acumulado $"
        ,"Objetivo"
         ]

percColsPen = ['Desvio %']


vtasSalonT = _estiladorVtaTituloP(vtasSalonT,numCols0,numCols,percColsPen, "INFO TICKET SALON PROMEDIO")

ubicacion = str(pathlib.Path(__file__).parent)+"\\"
nombrePen = "Info_Promedio_Vtas_Acumulado.png"

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

df_to_image(vtasSalonT, ubicacion, nombrePen)
#df_to_image(vtasSalonM,ubicacion,nombrePenDiario)


#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)
