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


##########################   Ventas Diarias

ventasPan = pd.read_sql('''
    select distinct(p.ID),g.UEN, (p.CANTIDAD) as Ventas
	from dbo.PanSalDe as p
	left join dbo.PanSalGe as g on
	p.UEN = g.UEN
	and p.FECHASQL=g.FECHASQL
	and p.TURNO = g.TURNO
	and p.NROCOMP = g.NROCOMP
	left join ProdPanaderia as d on
	p.CODIGO = d.CODIGO
	left join CLIENTES as C on
	C.NROCLIENTE = g.NROCLIENTE
	and c.UEN = g.UEN
	where g.NROCLIENTE = '30' 
	and (p.CODIGO < 17
	and p.CODIGO >= 10)
	and p.FECHASQL =DATEADD(day, -1, CAST(GETDATE() AS date))
    ''' ,db_conex)
ventasPan = ventasPan.convert_dtypes()
ventasPan = ventasPan.loc[:,ventasPan.columns!="ID"]
ventasPan = ventasPan.groupby(
        ["UEN"]
        , as_index=False
    ).sum(numeric_only=True)

ventasPremios = pd.read_sql('''
   select distinct(p.ID),g.UEN, (p.CANTIDAD) as 'Premios/Cortesia'
	from dbo.PanSalDe as p
	left join dbo.PanSalGe as g on
	p.UEN = g.UEN
	and p.FECHASQL=g.FECHASQL
	and p.TURNO = g.TURNO
	and p.NROCOMP = g.NROCOMP
	left join ProdPanaderia as d on
    p.UEN=d.UEN
	and p.CODIGO = d.CODIGO
	left join CLIENTES as C on
	C.NROCLIENTE = g.NROCLIENTE
	and c.UEN = g.UEN
	where (G.NROCLIENTE = '6' or G.NROCLIENTE ='20' or G.NROCLIENTE = '35' OR G.NROCLIENTE = '36' OR G.NROCLIENTE = '37')
	and (p.CODIGO < 17
	and p.CODIGO >= 10)
	and p.FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))

    ''' ,db_conex)
ventasPremios = ventasPremios.convert_dtypes()
ventasPremios = ventasPremios.loc[:,ventasPremios.columns!="ID"]
ventasPremios = ventasPremios.groupby(
        ["UEN"]
        , as_index=False
    ).sum(numeric_only=True)

### Ventas Red MAS
ventasRM = pd.read_sql('''
	DECLARE @inicioMesActual DATETIME
	SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, CURRENT_TIMESTAMP), 0)

	DECLARE @inicioMesAnterior DATETIME
	SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

	--Divide por la cant de días del mes anterior y multiplica por la cant de días del
	--mes actual
	
	DECLARE @hoy DATETIME
	SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

Select P.ID,P.UEN,P.CANTIDAD as 'Ventas con Red Mas'
	from dbo.PanSalDe as p
	left join dbo.PanSalGe as g on
	p.UEN = g.UEN
	and p.FECHASQL=g.FECHASQL
	and p.TURNO = g.TURNO
	and p.NROCOMP = g.NROCOMP
	left join ProdPanaderia as d on
	P.UEN = D.UEN AND
	p.CODIGO = d.CODIGO
	left join CLIENTES as C on
	C.NROCLIENTE = g.NROCLIENTE
	and c.UEN = g.UEN
	where c.NOMBRE is null
    AND p.FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
    ''' ,db_conex)
ventasRM = ventasRM.convert_dtypes()

ventasRM = ventasRM.loc[:,ventasRM.columns!="ID"]
ventasRM = ventasRM.groupby(
        ["UEN"]
        , as_index=False
    ).sum(numeric_only=True)



###
ventasPan = ventasPan.merge(ventasPremios, on='UEN', how= 'outer')
ventasPan = ventasPan.groupby(
        ["UEN"]
        , as_index=False
    ).sum(numeric_only=True)

############### Envios Diarios  ##################
ventasPan = ventasPan.fillna(0)
enviosPan = pd.read_sql('''
select UEN, SUM(CANTIDAD) AS Envios 
	from PanRecDet 
	WHERE CODIGO >= 10 AND CODIGO < 17
	AND FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
	GROUP BY UEN
	ORDER BY UEN
  '''      ,db_conex)

enviosPan = enviosPan.convert_dtypes()
enviosPan = enviosPan.merge(ventasPan, on='UEN', how='outer')

if ventasRM['UEN'].notna().any():
    enviosPan = enviosPan.merge(ventasRM,on='UEN',how='outer')
else: 
    enviosPan['Ventas con Red Mas']=0


enviosPan = enviosPan.groupby(
        ["UEN"]
        , as_index=False
    ).sum(numeric_only=True)

enviosPan['Control'] = 0

if enviosPan.columns[3] == 'Premios/Cortesia':
    x=1
elif enviosPan.columns[3] == 'Control':
    enviosPan['Premios/Cortesia'] = 0

enviosPan= enviosPan.loc[:,enviosPan.columns!="Control"]

enviosPan = enviosPan.assign(Mermas= enviosPan["Envios"] - enviosPan["Ventas"] - enviosPan["Premios/Cortesia"] - enviosPan['Ventas con Red Mas'])
enviosPan = enviosPan.assign(Mermaspocentaje= (enviosPan["Mermas"] / enviosPan["Envios"])* -1)
enviosPan = enviosPan.sort_values('Mermaspocentaje',ascending=True)

### Creo columna (fila) TOTALES
enviosPan.loc["colTOTAL"]= pd.Series(
    enviosPan.sum(numeric_only=True)
    , index=['Ventas'
        ,'Envios'
        ,'Mermas'
        ,'Premios/Cortesia'
        ,'Ventas con Red Mas']
)
enviosPan.fillna({"UEN":"TOTAL"}, inplace=True)

#Creo totales de % Margen Diario
tasa = (enviosPan.loc["colTOTAL","Mermas"] / enviosPan.loc["colTOTAL","Envios"] * -1)
enviosPan.fillna({"Mermaspocentaje":tasa}, inplace=True)
enviosPan = enviosPan.rename({'UEN': 'UEN', 'Envios': 'Envios'
, 'Ventas': 'Ventas','Mermas': 'Mermas','Mermaspocentaje':'% Mermas'},axis = 1)


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
        .format("{0:,.2f}", subset=list_Col_Num) \
        .format("{:,.2%}", subset=list_Col_Perc) \
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
        .applymap(asignar_color, subset='% Mermas')\
        .apply(lambda x: ["background: black" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1)
        
        
    evitarTotales = df.index.get_level_values(0)
    
    return resultado

def asignar_color(valor):
    if valor >= 0:
        return 'color: green'
    elif valor<0 and valor >= -0.15:
        return 'color: orange'
    elif valor < -0.15:
        return 'color: red'

#  columnas sin decimales
numCols = [ 'Envios'
        ,'Ventas'
        ,'Mermas'
        ,'Premios/Cortesia'
           ,'Ventas con Red Mas'
         ]
# Columnas Porcentaje
percColsPen = ["% Mermas"
]

enviosPan = _estiladorVtaTituloD(enviosPan,numCols,percColsPen, "Mermas Panaderia Playa")


ubicacion = "C:/Informes/PenetracionPanaderia/"
nombrePen = "MermasPanaderia.png"
nombrePenDiario = "Vtas.png"
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

df_to_image(enviosPan, ubicacion, nombrePen)
#df_to_image(ventasPan,ubicacion,nombrePenDiario)


#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)



