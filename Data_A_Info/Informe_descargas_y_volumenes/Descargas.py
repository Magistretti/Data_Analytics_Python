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
from calendar import monthrange
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



#### Creo datos para volumen Proyectado
diasdelmes=(tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d")
mes=(tiempoInicio-pd.to_timedelta(1,"days")).strftime("%m")
año=(tiempoInicio-pd.to_timedelta(1,"days")).strftime("%y")
diasdelmes = int(diasdelmes)
mes=int(mes)
año=int(año)
num_days = monthrange(año,mes)[1] # num_days = 31.
num_days=int(num_days)
### PRESUPUESTO
##########################################
# TRABAJO CON TABLA DE EXCEL    ################
##########################################

ubicacion = 'C:/Informes/Informe descargas y volumenes'+"\\"
aux_semanal = "PresuDescargasGO.xlsx"
presu =pd.read_excel(ubicacion+aux_semanal, sheet_name= 'Hoja1')
presu = presu.convert_dtypes()



######## Descargas de Combustible
df_Descargas = pd.read_sql('''
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

	select UEN, CODPRODUCTO, sum(VOLPRECARTEL) as VOLPRECARTEL ,sum(VOLUMENCT) as DESCARGAS,sum(VOLUMENVR) as VOLUMENVR,sum(VOLUMENCEM) as VOLUMENCEM
	from RemDetalle 
	where  
	codproducto = 'GO' 
	and UEN IN ('LAMADRID'
            ,'AZCUENAGA'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
            ,'SAN JOSE')
	AND  FECHASQL >= @inicioMesActual
        AND FECHASQL < @hoy
	group by UEN, CODPRODUCTO
	order by UEN

  '''      ,db_conex)
df_Descargas = df_Descargas.convert_dtypes()




df_Descargas["Descargas Proyectadas"] = df_Descargas["DESCARGAS"]/diasdelmes*num_days

df_Descargas=df_Descargas.merge(presu,on='UEN',how='outer')
df_Descargas['Desvio Cantidades']=df_Descargas['Descargas Proyectadas']-df_Descargas['Presupuesto Descargas']
df_Descargas['Desvio %']=(df_Descargas['Descargas Proyectadas']/df_Descargas['Presupuesto Descargas'])-1
df_Descargas["Presupuesto Descargas Proporcional"] = df_Descargas['Presupuesto Descargas']/num_days*diasdelmes
df_Descargas['Desvio Acumulado %']=(df_Descargas['DESCARGAS']/df_Descargas['Presupuesto Descargas Proporcional'])-1
df_Descargas['Desvio Cantidades Acumuladas']=df_Descargas['DESCARGAS']-df_Descargas['Presupuesto Descargas Proporcional']
df_Descargas.loc["colTOTAL"]= pd.Series(
    df_Descargas.sum()
    , index=['DESCARGAS','Descargas Proyectadas','Desvio Cantidades Acumuladas','Presupuesto Descargas','Desvio Cantidades','Presupuesto Descargas Proporcional']
)
df_Descargas.fillna({"UEN":"TOTAL"}, inplace=True)

tasa = (df_Descargas.loc["colTOTAL",'Descargas Proyectadas'] /
    df_Descargas.loc["colTOTAL",'Presupuesto Descargas'])-1
df_Descargas.fillna({"Desvio %":tasa}, inplace=True)

tasa = (df_Descargas.loc["colTOTAL",'DESCARGAS'] /
    df_Descargas.loc["colTOTAL",'Presupuesto Descargas Proporcional'])-1
df_Descargas.fillna({"Desvio Acumulado %":tasa}, inplace=True)

df_Descargas = df_Descargas.rename({'DESCARGAS':'Descargas Acumuladas','Desvio %':'Desvio Proyectado %','Desvio Cantidades':'Desvio Cantidades Proyectadas'},axis=1)
df_Descargas=df_Descargas.reindex(columns=['UEN','Descargas Acumuladas','Descargas Proyectadas',
                                           'Presupuesto Descargas Proporcional','Presupuesto Descargas','Desvio Cantidades Acumuladas',
                                           'Desvio Cantidades Proyectadas','Desvio Acumulado %','Desvio Proyectado %'])

######### LE DOY FORMATO AL DATAFRAME
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
  
    return resultado

#  columnas sin decimales
numCols = [ 'Descargas Acumuladas','Descargas Proyectadas',
                                           'Presupuesto Descargas Proporcional','Presupuesto Descargas','Desvio Cantidades Acumuladas',
                                           'Desvio Cantidades Proyectadas']

# Columnas Porcentaje
percColsPen = ['Desvio Acumulado %','Desvio Proyectado %'
]

### APLICO EL FORMATO A LA TABLA
df_Descargas = _estiladorVtaTituloD(df_Descargas,numCols,percColsPen, "Presupuesto Descargas GO")

#### DEFINO EL DESTINO DONDE SE GUARDARA LA IMAGEN Y EL NOMBRE
ubicacion = "C:/Informes/Informe descargas y volumenes/"
nombrePen = "PresupuestoDescargasGO.png"
### IMPRIMO LA IMAGEN
def df_to_image(df, ubicacion, nombre):
    """
    Esta función usa las biblioteca "dataframe_Image as dfi" y "os" para 
    generar un archivo .png de un dataframe. Si el archivo ya existe, este será
    reemplazado por el nuevo archivo.

    Args:
        df: dataframe a convertir
         ubicacion: ubicacion local donde se quiere grabar el archivo
          nombre: nombre del archivo incluyendo extegoión .png (ej: "hello.png")

    """
    if os.path.exists(ubicacion+nombre):
        os.remove(ubicacion+nombre)
        dfi.export(df, ubicacion+nombre,max_rows=-1)
    else:
        dfi.export(df, ubicacion+nombre,max_rows=-1)

df_to_image(df_Descargas, ubicacion, nombrePen)



#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)





















