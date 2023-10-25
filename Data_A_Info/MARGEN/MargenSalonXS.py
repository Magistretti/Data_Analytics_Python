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


########## Informacion sobre ventas y Margenes Diario

controlVtas = pd.read_sql(   
'''	
	SELECT p.AGRUPACION,SUM(E.IMPORTE) AS 'Ventas Diarias',SUM(e.PreCostoImpIncl) AS 'Costo Diario', ( SUM(E.IMPORTE)-SUM(e.PreCostoImpIncl)) AS 'Margen Diario', (( SUM(E.IMPORTE)-SUM(e.PreCostoImpIncl))/SUM(E.IMPORTE)) as '% Margen Diario'
    FROM [Rumaos].[dbo].[SCEgreso] as  e
	left join dbo.SCprodUEN as P on
	E.UEN = P.UEN
	AND E.CODIGO = P.CODIGO

	where
  	E.FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
	and E.UEN = 'XPRESS'
	and p.AGRUPACION  Not like '%INSUMO%'
	and p.AGRUPACION not like '%FRANQUICIA%'
    AND P.AGRUPACION NOT LIKE 'REGAL%'
	and p.AGRUPACION not like 'Premios%'
	AND E.IMPORTE > 1
    and e.PreCostoImpIncl > 1
	group by p.AGRUPACION

  '''      ,db_conex)
controlVtas = controlVtas.convert_dtypes()


###### Informacion Sobre Ventas, costos y Margenes Acumuladas

controlVtasM = pd.read_sql(   
'''	  
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
	SELECT p.AGRUPACION,SUM(E.IMPORTE) AS 'Ventas Acumuladas',SUM(e.PreCostoImpIncl) AS 'Costo Acumulado', ( SUM(E.IMPORTE)-SUM(e.PreCostoImpIncl)) AS 'Margen Acumulado', (( SUM(E.IMPORTE)-SUM(e.PreCostoImpIncl))/SUM(E.IMPORTE)) as '% Margen Acumulado'
    FROM [Rumaos].[dbo].[SCEgreso] as  e
	left join dbo.SCprodUEN as P on
	E.UEN = P.UEN
	AND E.CODIGO = P.CODIGO

	where
  	FECHASQL >= @inicioMesActual
	and	FECHASQL < @hoy
	and E.UEN = 'XPRESS'
	and p.AGRUPACION  Not like '%INSUMO%'
	and p.AGRUPACION not like '%FRANQUICIA%'
    AND P.AGRUPACION NOT LIKE 'REGAL%'
	and p.AGRUPACION not like 'Premios%'
	AND E.IMPORTE > 1
    and e.PreCostoImpIncl > 1

	group by p.AGRUPACION
  '''      ,db_conex)
controlVtasM = controlVtasM.convert_dtypes()

#### Concateno Tablas Diarias con acumuladas

controlVtas = controlVtas.merge(controlVtasM, on='AGRUPACION', how='outer')
controlVtas = controlVtas.fillna(0)

### Creo columna (fila) TOTALES
controlVtas.loc["colTOTAL"]= pd.Series(
    controlVtas.sum()
    , index=['Ventas Diarias'
        ,'Ventas Acumuladas'
        ,'Costo Diario'
        ,'Costo Acumulado'
        ,'Margen Diario'
        ,'Margen Acumulado']
)
controlVtas.fillna({"UEN":"TOTAL"}, inplace=True)

#Creo totales de % Margen Diario
tasa = (controlVtas.loc["colTOTAL","Margen Diario"] / controlVtas.loc["colTOTAL","Ventas Diarias"])
controlVtas.fillna({"% Margen Diario":tasa}, inplace=True)

tasa1 = (controlVtas.loc["colTOTAL", 'Margen Acumulado']/controlVtas.loc["colTOTAL","Ventas Acumuladas"])
controlVtas.fillna({"% Margen Acumulado":tasa1}, inplace=True)



def _estiladorVtaTituloD(df, list_Col_Num, list_Col_Perc, titulo):
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
        .format("{:,.2%}", subset=list_Col_Perc) \
        .hide_index() \
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
        .apply(lambda x: ["background: black" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1)
    evitarTotales = df.index.get_level_values(0)
    resultado = resultado.background_gradient(
        cmap="Dark2" # Red->Yellow->Green
        ,vmin=-0.1
        ,vmax=0.1
        ,subset=pd.IndexSlice["% Margen Diario"]
    )
    resultado = resultado.background_gradient(
        cmap="Dark2" # Red->Yellow->Green
        ,vmin=-0.1
        ,vmax=0.1
        ,subset=pd.IndexSlice["% Margen Acumulado"]
    )

   
    return resultado

#  columnas sin decimales
numCols = [ 'Ventas Diarias'
        ,'Ventas Acumuladas'
        ,'Costo Diario'
        ,'Costo Acumulado'
        ,'Margen Diario'
        ,'Margen Acumulado'
         ]
# Columnas Porcentaje
percColsPen = ["% Margen Diario"
        ,"% Margen Acumulado"
]

controlVtas = _estiladorVtaTituloD(controlVtas,numCols,percColsPen, "XPRESS")

ubicacion = "C:/Informes/MARGEN/"
nombreGNC = "Info_Presupuesto_GNC.png"
nombreGNCproy = "ControlVtasXS.png"

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

df_to_image(controlVtas, ubicacion, nombreGNCproy)



#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)
