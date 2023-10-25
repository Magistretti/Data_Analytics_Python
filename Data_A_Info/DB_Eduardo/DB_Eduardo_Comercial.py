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
import datetime

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

##### Fechas
# Obtener la fecha actual
fecha_actual = datetime.date.today()
# Obtener el primer día del mes actual
primer_dia_mes_actual = fecha_actual.replace(day=1)
# Obtener el último día del mes anterior
ultimo_dia_mes_anterior = primer_dia_mes_actual - datetime.timedelta(days=1)
# Obtener el primer día del mes anterior
primer_dia_mes_anterior = ultimo_dia_mes_anterior.replace(day=1)
# Obtener la fecha actual
fecha_actual = datetime.date.today()
# Obtener el mes actual
mes_actual = fecha_actual.month
# Obtener el año actual
anio_actual = fecha_actual.year
# Calcular el mes hace dos meses atrás
mes_dos_atras = mes_actual - 2
# Calcular el año dos meses atrás
if mes_dos_atras <= 0:
    mes_dos_atras += 12
    anio_dos_atras = anio_actual - 1
else:
    anio_dos_atras = anio_actual
# Obtener el primer día del mes dos meses atrás
primer_dia_dos_meses_atras = datetime.date(anio_dos_atras, mes_dos_atras, 1)


primer_dia_dos_meses_atras = primer_dia_dos_meses_atras
ultimo_dia_mes_anterior = ultimo_dia_mes_anterior

##### Dataframe
## Ventas ACumuladas
df_VentasAcum = pd.read_sql(f'''
  	select   UEN, SUM(VTAPRECARTELVOL) as 'Volumen Vendido Contado',SUM(VTAADELVOL) as 'Volumen Vendido CTACTE'
    , CODPRODUCTO,FECHASQL from EmpVenta 
	WHERE 
	FECHASQL >= '{primer_dia_dos_meses_atras.strftime("%Y-%d-%m")}'
	and	FECHASQL <= '{ultimo_dia_mes_anterior.strftime("%Y-%d-%m")}'
	and CODPRODUCTO not like 'GNC'
	group by UEN, CODPRODUCTO,FECHASQL
	ORDER BY CODPRODUCTO
  '''      ,db_conex)
df_VentasAcum = df_VentasAcum.convert_dtypes()

## Ventas ACumuladas YER
df_VentasAcumYER = pd.read_sql(f'''
	SELECT emp.UEN,emp.FECHASQL,sum(emp.VOLUMEN) as 'Volumen YER',emp.CODPRODUCTO
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL >= '{primer_dia_dos_meses_atras.strftime("%Y-%d-%m")}'
        AND FECHASQL <= '{ultimo_dia_mes_anterior.strftime("%Y-%d-%m")}'
		and emp.CODPRODUCTO <> 'GNC'
        AND  P.[DESCRIPCION] like '%ruta%'
		group by emp.FECHASQL,emp.CODPRODUCTO,emp.UEN
		order by CODPRODUCTO,UEN
  '''      ,db_conex)
df_VentasAcumYER = df_VentasAcumYER.convert_dtypes()
## Ventas Red Mercosur

df_VentasAcumREDM = pd.read_sql(f'''
	SELECT emp.UEN,emp.FECHASQL,sum(emp.VOLUMEN) as 'Volumen REDMAS',emp.CODPRODUCTO
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL >= '{primer_dia_dos_meses_atras.strftime("%Y-%d-%m")}'
        AND FECHASQL <= '{ultimo_dia_mes_anterior.strftime("%Y-%d-%m")}'
		and emp.CODPRODUCTO <> 'GNC'
        AND  (P.[DESCRIPCION] like '%PROMO%' OR P.DESCRIPCION LIKE '%MERCO%' OR P.DESCRIPCION LIKE '%MAS%')
		group by emp.FECHASQL,emp.CODPRODUCTO,emp.UEN
		order by CODPRODUCTO,UEN
  '''      ,db_conex)
df_VentasAcumREDM = df_VentasAcumREDM.convert_dtypes()

## Dataframe

df_VentasAcum = df_VentasAcum.merge(df_VentasAcumYER,on=['UEN','CODPRODUCTO','FECHASQL'],how='outer')
df_VentasAcum = df_VentasAcum.merge(df_VentasAcumREDM,on=['UEN','CODPRODUCTO','FECHASQL'],how='outer')

df_VentasAcum = df_VentasAcum.fillna(0)
df_VentasAcum['Volumen Vendido']=(df_VentasAcum['Volumen Vendido Contado']+df_VentasAcum['Volumen Vendido CTACTE']
                                +df_VentasAcum['Volumen YER']+df_VentasAcum['Volumen REDMAS'])


df_VentasAcumMix=df_VentasAcum.reindex(['UEN','FECHASQL','CODPRODUCTO','Volumen Vendido'],axis=1)

df_VentasAcumMix = df_VentasAcumMix.pivot_table(index=['UEN','FECHASQL'], columns='CODPRODUCTO', values='Volumen Vendido', aggfunc='sum')
df_VentasAcumMix = df_VentasAcumMix.reset_index()
df_VentasAcumMix = df_VentasAcumMix.fillna(0)


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
### Diferencio los meses bajo analisis
mesAnterior= df_VentasAcumMix.loc[(df_VentasAcumMix["FECHASQL"] < primer_dia_mes_anterior.strftime("%Y-%m-%d")),:]
mesActual= df_VentasAcumMix.loc[(df_VentasAcumMix["FECHASQL"] >= primer_dia_mes_anterior.strftime("%Y-%m-%d")),:]
df_VentasAcum=df_VentasAcum.loc[(df_VentasAcum["FECHASQL"] >= primer_dia_mes_anterior.strftime("%Y-%m-%d")),:]
# Create column BANDERA from column UEN

mesAnterior["BANDERA"] = mesAnterior.apply(
    lambda row: _bandera(row["UEN"])
        , axis= 1
)
mesActual["BANDERA"] = mesActual.apply(
    lambda row: _bandera(row["UEN"])
        , axis= 1
)
df_VentasAcum["BANDERA"] = df_VentasAcum.apply(
    lambda row: _bandera(row["UEN"])
        , axis= 1
)


mesAnterior = mesAnterior.groupby("BANDERA")[['EU   ','GO   ','NS   ','NU   ']].sum().reset_index()

mesActual = mesActual.groupby("BANDERA")[['EU   ','GO   ','NS   ','NU   ']].sum().reset_index()


df_VentasAcum = df_VentasAcum.loc[(df_VentasAcum["CODPRODUCTO"] == 'EU   ') | (df_VentasAcum["CODPRODUCTO"] == 'GO   '),:]

df_VentasAcum = df_VentasAcum.groupby("BANDERA")[['Volumen Vendido Contado','Volumen Vendido CTACTE','Volumen YER','Volumen REDMAS','Volumen Vendido']].sum().reset_index()

mesActual['Mix Gasoleos']=mesActual['EU   ']/(mesActual['EU   ']+mesActual['GO   '])
mesActual['Mix Naftas']=mesActual['NU   ']/(mesActual['NU   ']+mesActual['NS   '])

mesAnterior['Mix Gasoleos']=mesAnterior['EU   ']/(mesAnterior['EU   ']+mesAnterior['GO   '])
mesAnterior['Mix Naftas']=mesAnterior['NU   ']/(mesAnterior['NU   ']+mesAnterior['NS   '])



## Separo por bandera
df_VentasDapsaMes_Actual= mesActual.loc[(mesActual["BANDERA"] == 'DAPSA'),:]
df_VentasYPFMes_Actual= mesActual.loc[(mesActual["BANDERA"] == 'YPF'),:]

df_VentasDapsaMes_Anterior= mesAnterior.loc[(mesAnterior["BANDERA"] == 'DAPSA'),:]
df_VentasYPFMes_Anterior= mesAnterior.loc[(mesAnterior["BANDERA"] == 'YPF'),:]

#### CREO DF DE MIX GASOLEOS PARA EL REPORTE
fecha_mes_Actual = datetime.datetime.strptime(primer_dia_mes_anterior.strftime("%Y-%m-%d"), "%Y-%m-%d")
nombre_mes_Actual = fecha_mes_Actual.strftime("%B").capitalize()

fecha_mes_Anterior = datetime.datetime.strptime(primer_dia_dos_meses_atras.strftime("%Y-%m-%d"), "%Y-%m-%d")
nombre_mes_Anterior = fecha_mes_Anterior.strftime("%B").capitalize()

mixGasoleos = pd.DataFrame()
mixGasoleos.loc[0,'DAPSA']= mesAnterior.loc[0,'Mix Gasoleos']
mixGasoleos.loc[0,'YPF']=mesAnterior.loc[1,'Mix Gasoleos']
mixGasoleos.loc[0,'Mes']= nombre_mes_Anterior
mixGasoleos.loc[1,'DAPSA']=mesActual.loc[0,'Mix Gasoleos']
mixGasoleos.loc[1,'YPF']=mesActual.loc[1,'Mix Gasoleos']
mixGasoleos.loc[1,'Mes']= nombre_mes_Actual


##### CREO DF CUENTA/CONTADO/YER
cueta_cte_yer = pd.DataFrame()
cueta_cte_yer.loc[0,'BANDERA']='YPF'
cueta_cte_yer.loc[0,'CONTADO']=((df_VentasAcum.loc[1,'Volumen Vendido Contado']+df_VentasAcum.loc[1,'Volumen REDMAS'])
                            /df_VentasAcum.loc[1,'Volumen Vendido'])
cueta_cte_yer.loc[0,'CUENTA']=(df_VentasAcum.loc[1,'Volumen Vendido CTACTE']/df_VentasAcum.loc[1,'Volumen Vendido'])
cueta_cte_yer.loc[0,'YER']=(df_VentasAcum.loc[1,'Volumen YER']/df_VentasAcum.loc[1,'Volumen Vendido'])
cueta_cte_yer.loc[0,'VOL CDO/CTA']=((df_VentasAcum.loc[1,'Volumen Vendido Contado']+df_VentasAcum.loc[1,'Volumen REDMAS'])
                                    /df_VentasAcum.loc[1,'Volumen Vendido CTACTE'])

cueta_cte_yer.loc[1,'BANDERA']='DAPSA'
cueta_cte_yer.loc[1,'CONTADO']=((df_VentasAcum.loc[0,'Volumen Vendido Contado']+df_VentasAcum.loc[0,'Volumen REDMAS'])
                            /df_VentasAcum.loc[0,'Volumen Vendido'])
cueta_cte_yer.loc[1,'CUENTA']=(df_VentasAcum.loc[0,'Volumen Vendido CTACTE']/df_VentasAcum.loc[0,'Volumen Vendido'])
cueta_cte_yer.loc[1,'YER']=(df_VentasAcum.loc[0,'Volumen YER']/df_VentasAcum.loc[0,'Volumen Vendido'])
cueta_cte_yer.loc[1,'VOL CDO/CTA']=((df_VentasAcum.loc[0,'Volumen Vendido Contado']+df_VentasAcum.loc[0,'Volumen REDMAS'])
                                    /df_VentasAcum.loc[0,'Volumen Vendido CTACTE'])


#### Cre DF Ventas Intermensuales
varIntermensual = pd.DataFrame()
varIntermensual.loc[0,'G2 YPF']=(mesActual.loc[1,'GO   ']/mesAnterior.loc[1,'GO   '])-1
varIntermensual.loc[0,'G3 YPF']=(mesActual.loc[1,'EU   ']/mesAnterior.loc[1,'EU   '])-1
varIntermensual.loc[0,'G2 DAPSA']=(mesActual.loc[0,'GO   ']/mesAnterior.loc[0,'GO   '])-1
varIntermensual.loc[0,'G3 DAPSA']=(mesActual.loc[0,'EU   ']/mesAnterior.loc[0,'EU   '])-1
varIntermensual.loc[0,'TOTAL GASOLEOS']=((mesActual.loc[0,'GO   ']+mesActual.loc[0,'EU   ']+mesActual.loc[1,'GO   ']+mesActual.loc[1,'EU   '])
                            /(mesAnterior.loc[0,'EU   ']+mesAnterior.loc[0,'GO   ']+mesAnterior.loc[1,'EU   ']+mesAnterior.loc[1,'GO   ']))-1
volIntermensual = pd.DataFrame()
volIntermensual.loc[0,'Volumen G2 YPF']=mesActual.loc[1,'GO   ']-mesAnterior.loc[1,'GO   ']
volIntermensual.loc[0,'Volumen G3 YPF']=mesActual.loc[1,'EU   ']-mesAnterior.loc[1,'EU   ']
volIntermensual.loc[0,'Volumen G2 DAPSA']=mesActual.loc[0,'GO   ']-mesAnterior.loc[0,'GO   ']
volIntermensual.loc[0,'Volumen G3 DAPSA']=mesActual.loc[0,'EU   ']-mesAnterior.loc[0,'EU   ']
volIntermensual.loc[0,'TOTAL Volumen']=((mesActual.loc[0,'GO   ']+mesActual.loc[0,'EU   ']+mesActual.loc[1,'GO   ']+mesActual.loc[1,'EU   '])
                            -(mesAnterior.loc[0,'EU   ']+mesAnterior.loc[0,'GO   ']+mesAnterior.loc[1,'EU   ']+mesAnterior.loc[1,'GO   ']))


def _estiladorVtaTitulo(df, list_Col_Num, list_Col_Perc,colDecimal,numcolumns, titulo):
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
        .format("{:,.0f}", subset=numcolumns) \
        .format("{:,.2f}", subset=colDecimal) \
        .format("{:,.2%}", subset=list_Col_Perc) \
        .hide(axis=0) \
        .set_caption(
            "    -    "
            +"<br>"
            + titulo
            + "<br>") \
        .set_properties(subset= list_Col_Perc + list_Col_Num+numcolumns+colDecimal
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

#### Defino columnas para cada Dataframe (Numericas)

### COLUMNAS PARA INFORME mix
percColsPen = ["DAPSA","YPF"]
numcolumns=[]
numcolumnsDecimal=[]
letCols = ["Mes"]

#### COLUMNAS INFORME CONTADO/CUENTA/ YER
percColsPen1 = ["CONTADO","CUENTA","YER"]
numcolumns1=[]
numcolumnsDecimal1=["VOL CDO/CTA"]
letCols1 = ["BANDERA"]

#### COLUMNAS INFORME VarIntermensual

percColsPen2 = ["G2 YPF","G3 YPF","G2 DAPSA","G3 DAPSA",'TOTAL GASOLEOS']
numcolumns2=[]
numcolumnsDecimal2 =[]
letCols2 = []

#### COLUMNAS INFORME VolIntermensual
percColsPen3 = []
numcolumns3=['Volumen G2 YPF','Volumen G3 YPF','Volumen G2 DAPSA','Volumen G3 DAPSA','TOTAL Volumen']
numcolumnsDecimal3=[]
letCols3 = []

###### Aplico el formato elegido a la imagen

mixGasoleos = _estiladorVtaTitulo(mixGasoleos,letCols,percColsPen,numcolumnsDecimal,numcolumns, "SHARE G3/GASOLEOS")

cueta_cte_yer = _estiladorVtaTitulo(cueta_cte_yer,letCols1,percColsPen1,numcolumnsDecimal1,numcolumns1, "SHARE CUENTA/CONTADO/YER")

varIntermensual = _estiladorVtaTitulo(varIntermensual,letCols2,percColsPen2,numcolumnsDecimal2,numcolumns2, "VENTAS INTERMENSUALES VARIACION INTERMENSUAL %")

volIntermensual = _estiladorVtaTitulo(volIntermensual,letCols3,percColsPen3,numcolumnsDecimal3,numcolumns3, "VENTAS INTERMENSUALES VARIACION INTERMENSUAL/LITROS")











