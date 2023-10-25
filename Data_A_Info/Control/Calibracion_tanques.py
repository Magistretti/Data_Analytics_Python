import os
import math
import numpy as np
from DatosLogin import login
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

volTanqueInicial=pd.read_sql(
'''
DECLARE @inicioMesActual DATETIME
	SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, CURRENT_TIMESTAMP), 0)

	DECLARE @inicioMesAnterior DATETIME
	SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

	--Divide por la cant de días del mes anterior y multiplica por la cant de días del
	--mes actual
	
	DECLARE @hoy DATETIME
	SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)
    


SELECT V.UEN, V.CODPRODUCTO, V.VOLINICIAL
FROM dbo.VolTanDet as V
WHERE V.FECHASQL = DATEADD(DAY, -8, CAST(GETDATE() AS date))
AND V.CODPRODUCTO!='GNC'
AND V.TURNO=1

ORDER BY V.UEN
''',db_conex
)
volTanqueInicial= volTanqueInicial.convert_dtypes()
volTanqueInicial=volTanqueInicial.groupby(
        ["UEN", "CODPRODUCTO"]
        , as_index=False
    ).sum(numeric_only=True)
volTanqueInicial.fillna(0)

volTanqueFinal=pd.read_sql(
'''
DECLARE @inicioMesActual DATETIME
	SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, CURRENT_TIMESTAMP), 0)

	DECLARE @inicioMesAnterior DATETIME
	SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

	--Divide por la cant de días del mes anterior y multiplica por la cant de días del
	--mes actual
	
	DECLARE @hoy DATETIME
	SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)
    


SELECT V.UEN, V.CODPRODUCTO, V.VOLFINAL
FROM dbo.VolTanDet as V
WHERE V.FECHASQL = DATEADD(DAY,-1,CAST(GETDATE() AS date))
AND V.CODPRODUCTO!='GNC'
AND V.TURNO=3
ORDER BY V.UEN
''',db_conex
)
volTanqueFinal= volTanqueFinal.convert_dtypes()
volTanqueFinal=volTanqueFinal.groupby(
        ["UEN", "CODPRODUCTO"]
        , as_index=False
    ).sum(numeric_only=True)
volTanqueFinal.fillna(0)


vtaTotal= pd.read_sql(
'''
DECLARE @inicioMesActual DATETIME
	SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, CURRENT_TIMESTAMP), 0)

	DECLARE @inicioMesAnterior DATETIME
	SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

	--Divide por la cant de días del mes anterior y multiplica por la cant de días del
	--mes actual
	
	DECLARE @hoy DATETIME
	SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)
    


SELECT V.UEN, V.FECHASQL, V.CODPRODUCTO, V.VTATOTVOL
FROM dbo.EmpVenta as V
WHERE V.FECHASQL < CAST(GETDATE() AS date)
AND V.FECHASQL >= DATEADD(DAY,-8,CAST(GETDATE() AS date))
AND V.CODPRODUCTO!='GNC'
ORDER BY V.UEN

''', db_conex)
vtaTotal= vtaTotal.convert_dtypes()
vtaTotal=vtaTotal.groupby(
        ["UEN", "CODPRODUCTO"]
        , as_index=False
    ).sum(numeric_only=True)
vtaTotal.fillna(0)


pruebasSurtidor= pd.read_sql('''
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

    WHERE EmP.FECHASQL < CAST(GETDATE() AS date)
		AND EmP.FECHASQL >= DATEADD(DAY,-8,CAST(GETDATE() AS date))
		AND emp.CODPRODUCTO!='GNC'
		AND P.[DESCRIPCION] like '%surtidor%'

		group by emp.FECHASQL,emp.CODPRODUCTO,emp.UEN
		order by CODPRODUCTO,UEN
        ''', db_conex)
pruebasSurtidor= pruebasSurtidor.convert_dtypes()
pruebasSurtidor=pruebasSurtidor.groupby(
        ["UEN", "CODPRODUCTO"]
        , as_index=False
    ).sum(numeric_only=True)
pruebasSurtidor.fillna(0)

volDescarga= pd.read_sql('''
    select UEN, CODPRODUCTO,(VOLUMENCT) as DESCARGAS, FECHASQL
    from RemDetalle as e
    where  
    codproducto <> 'GNC' 
    and CODPRODUCTO not like 'NN'
    AND e.FECHASQL < CAST(GETDATE() AS date)
    AND e.FECHASQL >= DATEADD(DAY,-8,CAST(GETDATE() AS date))
'''
, db_conex
)
volDescarga= volDescarga.convert_dtypes()
volDescarga=volDescarga.groupby(
        ["UEN", "CODPRODUCTO"]
        , as_index=False
    ).sum(numeric_only=True)
volDescarga.fillna(0)


volTanqueInicial=volTanqueInicial.merge(vtaTotal, on=['UEN', 'CODPRODUCTO'], how='outer')
volTanqueInicial=volTanqueInicial.merge(pruebasSurtidor, on=['UEN', 'CODPRODUCTO'], how='outer')
volTanqueInicial=volTanqueInicial.merge(volDescarga, on=['UEN', 'CODPRODUCTO'], how='outer')
    
volTanqueInicial= volTanqueInicial.fillna(0)
volTotal=volTanqueInicial.merge(volTanqueFinal, on=['UEN', 'CODPRODUCTO'], how='outer')

volTotal.iloc[16, 4]=0

volTotal = volTotal.drop(volTotal[volTotal['VOLINICIAL'] == 0].index)
volTotal['VTATOTVOL']= volTotal['VTATOTVOL']-volTotal['Volumen YER']
#volTotal['STOCK FINAL'] = (volTotal['VOLINICIAL'] + volTotal['DESCARGAS']) - volTotal['VOLVENTAS']
volTotal['DIFERENCIA 2']=(volTotal['VOLINICIAL'] + volTotal['DESCARGAS']) - volTotal['VOLFINAL']
volTotal.loc[volTotal['DIFERENCIA 2'] != 0, 'CALIBRACION'] = (volTotal['VTATOTVOL']/volTotal['DIFERENCIA 2'])-1

volTotal=volTotal.reindex(columns=['UEN', 'CODPRODUCTO', 'VOLINICIAL', 'DESCARGAS', 'VOLFINAL', 'VTATOTVOL', 'DIFERENCIA 2', 'CALIBRACION'])
    

def calculoAlerta(volTanque):
    #alerta = volTanque.loc[(volTanque['CALIBRACION'] < 0.06) | (volTanque['CALIBRACION'] > 0.08) ]
    alerta = volTanque
    alerta['CODPRODUCTO']= alerta['CODPRODUCTO'].str.strip()
    alerta = alerta.pivot(index='UEN', columns='CODPRODUCTO', values='CALIBRACION')

    alerta=alerta.reset_index()
    alerta=alerta.reindex(columns=['UEN', 'EU', 'GO', 'NS', 'NU'])

    alerta.loc[((alerta['EU']>0.99)|(alerta['EU']<=-0.99)), 'EU']=None
    
    return alerta

#calibracion_liq= calculoCalibracion(volDescarga, volTanqueInicial, volTanqueFinal, pruebasSurtidor, vtaTotal)
alerta= calculoAlerta(volTotal)
#alerta.iloc[13, 1]=None

def _estiladorVtaTituloP(df,list_Col_Num, list_Col_Num0,list_Col_Perc, titulo, evitarTotal):
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
            + ((tiempoInicio-pd.to_timedelta(8,"days")).strftime("%d/%m/%y"))
            + " - "
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
    if evitarTotal==1:
        subset_columns = pd.IndexSlice[evitarTotales[:-1],list_Col_Perc]
    else:
        subset_columns = pd.IndexSlice[list_Col_Perc]

    resultado= resultado.applymap(table_color,subset=subset_columns)
    
    return resultado

def table_color(val):
    """
    Takes a scalar and returns a string with
    the css property `'color: red'` for less than 60 marks, green otherwise.
    """
    
    if pd.notnull(val) and val > 0.0 and val < 0.1:
        color = 'blue'
    else:
        color = 'red'
    return 'color: % s' % color

##Columnas sin decimales
#numCols0 = ['VOLINICIAL','VOLDESCARGA','VOLVENTAS', 'VOLFINAL', 'DIFERENCIA', 'STOCK FINAL', 'DIFERENCIA 2']
numCols0=[]
numCols1=[]
##Columnas con decimales

numCols = []

num=[]

## Columnas porcentajes
percColsPen = ['EU', 'GO', 'NS', 'NU']
#percColsPen = ['CALIBRACION']

#alerta= _estiladorVtaTituloP(alerta, numCols, numCols0, percColsPen, 'INFO CALIBRACION TANQUES Alerta',0)
alerta= _estiladorVtaTituloP(alerta, numCols, numCols0, percColsPen, 'INFO CALIBRACION TANQUES Alerta',0)



ubicacion = str(pathlib.Path(__file__).parent)+"\\"
nombreCalibracionGNC = "Calibracion_tanques_semanal.png"
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

df_to_image(alerta, ubicacion, nombreCalibracionGNC)



