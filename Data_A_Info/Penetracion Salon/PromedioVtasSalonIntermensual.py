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
	
    SELECT A.UEN, COUNT(DISTINCT A.DESPACHOS) AS ID, SUM(A.Total) AS 'Total Vendido Diario $' FROM(
	SELECT ID AS DESPACHOS, UEN, (IMPORTE) as Total, FECHASQL
    FROM [Rumaos].[dbo].[SCEgreso] where
  	FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
	and UEN not like 'MERC GUAYMALLEN'
	 ) AS A GROUP BY  UEN
    ''' ,db_conex)


vtasSalon = vtasSalon.convert_dtypes()
vtasSalon = vtasSalon.reset_index()
vtasSalon = vtasSalon.fillna(0)

############ Proyectado Mensual
vtasSalonAM = pd.read_sql('''
	
     DECLARE @ayer DATETIME
	SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))
	DECLARE @inicioMesActual DATETIME
	SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

        DECLARE @inicioMesAnterior DATETIME
        SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

        --Divide por la cant de días del mes anterior y multiplica
        --por la cant de días del mes actual
	DECLARE @CantidadDiasMesAnterior INT = DAY(DATEADD(DAY, -1,DATEADD(DAY, 1 - DAY(getdate()), getdate())))
    Declare @pondMesAnt Float
    SET @pondMesAnt =  CAST(DAY(EOMONTH(getdate()-1)) AS float) /@CantidadDiasMesAnterior

	SELECT A.UEN, COUNT(DISTINCT A.DESPACHOS) AS ID, SUM(A.Total) AS 'TOTAL VENDIDO' FROM(
	SELECT ID AS DESPACHOS, UEN, (IMPORTE * @pondMesAnt) as Total, FECHASQL
    FROM [Rumaos].[dbo].[SCEgreso] where
  	FECHASQL >= @inicioMesAnterior
	and FECHASQL < @inicioMesActual
	and UEN not like 'MERC GUAYMALLEN'
	 ) AS A GROUP BY  UEN

    ''' ,db_conex)


vtasSalonAM = vtasSalonAM.convert_dtypes()
vtasSalonAM = vtasSalonAM.reset_index()
vtasSalonAM = vtasSalonAM.fillna(0)


vtasSalonAMM = pd.read_sql('''
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
	Declare @pondMesAct Float
    SET @pondMesAct = CAST(DAY(EOMONTH(@ayer)) AS float) /
    (CAST(DAY(CURRENT_TIMESTAMP) AS float)-1)

	SELECT A.UEN, COUNT(DISTINCT A.DESPACHOS) AS ID, SUM(A.Total) AS 'TOTAL VENDIDO' FROM(
	SELECT ID AS DESPACHOS, UEN, (IMPORTE * @pondMesAct) as Total
    FROM [Rumaos].[dbo].[SCEgreso] where
  	FECHASQL >= @inicioMesActual
	and	FECHASQL < @hoy
	and UEN not like 'MERC GUAYMALLEN'
    ) AS A GROUP BY  UEN

    ''' ,db_conex)
vtasSalonAMM = vtasSalonAMM.convert_dtypes()
vtasSalonAMM = vtasSalonAMM.reset_index()
vtasSalonAMM = vtasSalonAMM.fillna(0)
vtasSalonAMM = vtasSalonAMM.loc[:,vtasSalonAMM.columns!="index"]
vtasSalonAM = vtasSalonAM.loc[:,vtasSalonAM.columns!="index"]
vtasSalonAMM = vtasSalonAMM.loc[:,vtasSalonAMM.columns!="ID"]
vtasSalonAM = vtasSalonAM.loc[:,vtasSalonAM.columns!="ID"]
vtasSalonAMT = vtasSalonAM.merge(vtasSalonAMM, on='UEN',how='outer')
vtasSalonAMT = vtasSalonAMT.fillna(0)
vtasSalonAMT = vtasSalonAMT.assign(Intermensual= (vtasSalonAMT['TOTAL VENDIDO_y']/ vtasSalonAMT['TOTAL VENDIDO_x'])-1)

vtasSalonAMT = vtasSalon.merge(vtasSalonAMT, on='UEN',how='outer')
vtasSalonAMT = vtasSalonAMT.loc[:,vtasSalonAMT.columns!="ID"]
vtasSalonAMT = vtasSalonAMT.loc[:,vtasSalonAMT.columns!="index"]
vtasSalonAMT.loc["colTOTAL"]= pd.Series(
    vtasSalonAMT.sum(numeric_only=True)
    , index=["TOTAL VENDIDO_x"
    ,"TOTAL VENDIDO_y"
    ,"Total Vendido Diario $"]
)
vtasSalonAMT.fillna({"UEN":"TOTAL"}, inplace=True)

tasa7 = (vtasSalonAMT.loc["colTOTAL","TOTAL VENDIDO_y"] /
 vtasSalonAMT.loc["colTOTAL","TOTAL VENDIDO_x"]) -1
vtasSalonAMT.fillna({"Intermensual":tasa7}, inplace=True)




vtasSalonAMT = vtasSalonAMT.rename({"UEN":"UEN","Total Vendido Diario $":"Total Vendido Diario $","TOTAL VENDIDO_x":"Total Proyectado Mes Anterior $"
,"Intermensual":"% Intermensual","TOTAL VENDIDO_y":"Total Proyectado Mes Actual $"}, axis=1)

vtasSalonAMT = vtasSalonAMT.fillna(0)
vtasSalonAMT = vtasSalonAMT[vtasSalonAMT["UEN"] != 'MERCADO 2           ']


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
    resultado = df.style \
        .format("{0:,.2f}", subset=list_Col_Num) \
        .format("{:,.2%}", subset=list_Col_Perc) \
        .hide(axis=0) \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>") \
        .applymap(lambda x: 'color: blue' if x >= 0 else 'color: #FF0000', subset=pd.IndexSlice[:, ['% Intermensual']]) \
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

numCols0 = ["Total Proyectado Mes Anterior $"
        ,"Total Proyectado Mes Actual $"
        ,"Total Vendido Diario $"

]

numCols = [ 
         ]

percColsPen = ["% Intermensual"
]


vtasSalonAMT = _estiladorVtaTitulo(vtasSalonAMT,numCols0,percColsPen, "VENTAS DE SALON")

ubicacion = "C:/Informes/Penetracion Salon/"
nombrePen = "Info_Promedio_VtasIntermensual.png"

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

df_to_image(vtasSalonAMT, ubicacion, nombrePen)
#df_to_image(vtasSalonAMM,ubicacion,nombrePenDiario)


#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)



