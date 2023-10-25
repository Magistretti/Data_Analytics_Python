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


## Ventas ACumuladas
df_VentasAcum = pd.read_sql('''
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
	select UEN, sum(VTATOTVOL) as 'Ventas Acum', CODPRODUCTO,MAX(PRECARTEL) as Precio from EmpVenta 
    where FECHASQL >= DATEADD(day, -7, CAST(GETDATE() AS date))
	and FECHASQL < @hoy 
    and CODPRODUCTO not like 'GNC'
    and CODPRODUCTO not like 'NN'
    and UEN in ('Perdriel','perdriel2','San Jose','Lamadrid','Azcuenaga','Puente Olive')
    group by UEN, CODPRODUCTO,PRECARTEL
  '''      ,db_conex)
df_VentasAcum = df_VentasAcum.convert_dtypes()
## Regalos acmulados
df_regalosTrasladosAcum = pd.read_sql("""
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
SELECT
        EmP.[UEN]
        ,EmP.[CODPRODUCTO]
        ,sum(-EmP.[VOLUMEN]) as VTATOTVOL
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL >= DATEADD(day, -7, CAST(GETDATE() AS date))
	and FECHASQL < @hoy 
        AND EmP.VOLUMEN > '0' 
        AND (EmP.[CODPROMO] = '30'
            OR P.[DESCRIPCION] like '%PRUEBA%'
            OR P.[DESCRIPCION] like '%TRASLADO%'
            OR P.[DESCRIPCION] like '%MAYORISTA%'
        )
        and emp.UEN in ('Perdriel','perdriel2','San Jose','Lamadrid','Azcuenaga','Puente Olive')
        and emp.CODPRODUCTO not like 'GNC'
        and emp.CODPRODUCTO not like 'NN'
    group by emp.UEN, emp.CODPRODUCTO
""", db_conex)
df_regalosTrasladosAcum = df_regalosTrasladosAcum.convert_dtypes()
df_VentasAcum = df_VentasAcum.merge(df_regalosTrasladosAcum,on=['UEN','CODPRODUCTO'],how='outer')
df_VentasAcum = df_VentasAcum.fillna(0)
df_VentasAcum['Ventas Acumuladas']=df_VentasAcum['Ventas Acum'] + df_VentasAcum['VTATOTVOL']
df_VentasAcum=df_VentasAcum.reindex(columns=['UEN','CODPRODUCTO','Ventas Acumuladas','Precio'])


######## Descargas de Combustible
df_Descargas = pd.read_sql('''
select UEN, CODPRODUCTO, sum(VOLPRECARTEL) as VOLPRECARTEL ,sum(VOLUMENCT) as DESCARGAS,sum(VOLUMENVR) as VOLUMENVR,sum(VOLUMENCEM) as VOLUMENCEM
from RemDetalle 
where  
codproducto <> 'GNC' 
and CODPRODUCTO not like 'NN'
AND  FECHASQL >= DATEADD(day, -7, CAST(GETDATE() AS date))
and FECHASQL < DATEADD(day, -1, CAST(GETDATE() AS date))
and UEN in ('Perdriel','perdriel2','San Jose','Lamadrid','Azcuenaga','Puente Olive')
group by UEN, CODPRODUCTO
order by UEN

  '''      ,db_conex)
df_Descargas = df_Descargas.convert_dtypes()


balance = df_VentasAcum.merge(df_Descargas,on=['UEN','CODPRODUCTO'],how='outer')
balance=balance.reindex(columns=['UEN','CODPRODUCTO','Ventas Acumuladas','Precio','DESCARGAS'])
balance['Diferencial L']=balance['Ventas Acumuladas']-balance['DESCARGAS']
balance['Diferencial $']=balance['Diferencial L']*balance['Precio']*0.86
balance=balance.reindex(columns=['UEN','CODPRODUCTO','Ventas Acumuladas','DESCARGAS','Diferencial L','Diferencial $'])

## Separo por Productos

#Ultra Diesel
go= balance.loc[balance["CODPRODUCTO"] == 'GO   ',:]
go.loc["colTOTAL"]= pd.Series(
    go.sum()
    , index=['Ventas Acumuladas','DESCARGAS','Diferencial L','Diferencial $']
)
go.fillna({"UEN":"TOTAL"}, inplace=True)
#Infinia Diesel
eu= balance.loc[balance["CODPRODUCTO"] == 'EU   ',:]
eu.loc["colTOTAL"]= pd.Series(
    eu.sum()
    , index=['Ventas Acumuladas','DESCARGAS','Diferencial L','Diferencial $']
)
eu.fillna({"UEN":"TOTAL"}, inplace=True)
#Nafta Super
ns= balance.loc[balance["CODPRODUCTO"] == 'NS   ',:]
ns.loc["colTOTAL"]= pd.Series(
    ns.sum()
    , index=['Ventas Acumuladas','DESCARGAS','Diferencial L','Diferencial $']
)
ns.fillna({"UEN":"TOTAL"}, inplace=True)
#Nafta Infinia
nu= balance.loc[balance["CODPRODUCTO"] == 'NU   ',:]
nu.loc["colTOTAL"]= pd.Series(
    nu.sum()
    , index=['Ventas Acumuladas','DESCARGAS','Diferencial L','Diferencial $']
)
nu.fillna({"UEN":"TOTAL"}, inplace=True)
#Total
total=balance.reindex(columns=['UEN','Ventas Acumuladas','DESCARGAS','Diferencial L','Diferencial $'])

total = total.groupby(
        ["UEN"]
        , as_index=False
    ).sum()

total.loc["colTOTAL"]= pd.Series(
    total.sum()
    , index=['Ventas Acumuladas','DESCARGAS','Diferencial L','Diferencial $']
)
total.fillna({"UEN":"TOTAL"}, inplace=True)

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
        .format("$ {0:,.0f}", subset=list_Col_Num) \
        .format("{:,.1f} L", subset=list_Col_Perc) \
        .hide_index() \
        .set_caption(
            titulo
            + "<br>"
            + "Semana Actual "
            + ((pd.to_datetime("today")-pd.to_timedelta(7,"days"))
            .strftime("%d/%m/%y"))
            + " al "
            + ((pd.to_datetime("today")-pd.to_timedelta(1,"days"))
            .strftime("%d/%m/%y"))
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

colum = ['Ventas Acumuladas','DESCARGAS','Diferencial L']
colnum=['Diferencial $']


go = _estiladorVtaTituloD(go,colnum, colum, "Balance GO YPF")
eu = _estiladorVtaTituloD(eu, colnum, colum , "Balance EU YPF")
ns = _estiladorVtaTituloD(ns, colnum, colum , "Balance NS YPF")
nu = _estiladorVtaTituloD(nu, colnum, colum , "Balance NU YPF")
total = _estiladorVtaTituloD(total, colnum, colum,  "Balance Total YPF")

###### DEFINO NOMBRE Y UBICACION DE DONDE SE VA A GUARDAR LA IMAGEN

ubicacion = "C:/Informes/Informe descargas y volumenes/"
nombrepngGO = "Balance_V_D_GO.png"
nombrepngEU = "Balance_V_D_EU.png"
nombrepngNS = "Balance_V_D_NS.png"
nombrepngNU = "Balance_V_D_NU.png"
nombrepngTotal = "Balance_V_D_TOTAL.png"
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

df_to_image(go, ubicacion, nombrepngGO)
df_to_image(eu, ubicacion, nombrepngEU)
df_to_image(ns, ubicacion, nombrepngNS)
df_to_image(nu, ubicacion, nombrepngNU)
df_to_image(total, ubicacion, nombrepngTotal)

#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)