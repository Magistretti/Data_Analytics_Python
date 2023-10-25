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

hoy = datetime.datetime.now()
ayer = hoy - timedelta(days=1)
primer_dia_mes = hoy.replace(day=1)

# Obtener la fecha actual
fecha_actual = hoy


##########################################
# TRABAJO CON TABLA DE EXCEL    ################
##########################################

###PRESUPUESTO
sheet_id='1ofJ38O2-dLfw7TZOxeTDYUzlWgl6hN2iMLZCMscTgGo'
hoja='Presupuesto'
gsheet_url_presupuesto = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id, hoja)
df_presupuesto=pd.read_csv(gsheet_url_presupuesto)
df_presupuesto = df_presupuesto.convert_dtypes()
df_presupuesto['FECHASQL'] = pd.to_datetime(df_presupuesto['FECHASQL'], format='%d/%m/%Y')
df_presupuesto= df_presupuesto.loc[(df_presupuesto["FECHASQL"] <= ayer.strftime('%Y-%m-%d'))& (df_presupuesto["FECHASQL"] >= primer_dia_mes.strftime('%Y-%m-%d')),:]

### COSTOS
sheet_id2='1yJlZkGWDcYa5hdlXZxY5_xbi4s3Y0AqoS-L2QgDdFTQ'
sheet_name= 'CostoGNCM3'
gsheet_url_costoNS = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id2, sheet_name)
costoGNC =pd.read_csv(gsheet_url_costoNS)
costoGNC = costoGNC.convert_dtypes()
costoGNC['FECHASQL'] = pd.to_datetime(costoGNC['FECHASQL'], format='%d/%m/%Y')
costoGNC= costoGNC.loc[costoGNC["FECHASQL"] == primer_dia_mes.strftime('%Y/%m/%d'),:]

egnctotales =df_presupuesto.loc[df_presupuesto['CODPRODUCTO']=='GNC']

egncAC=egnctotales.loc[df_presupuesto['UEN']=='ADOLFO CALLE']
egncU=egnctotales.loc[df_presupuesto['UEN']=='URQUIZA']
egncVN=egnctotales.loc[df_presupuesto['UEN']=='VILLANUEVA']
egncLH=egnctotales.loc[df_presupuesto['UEN']=='LAS HERAS']
egncM=egnctotales.loc[df_presupuesto['UEN']=='MITRE']
egncS=egnctotales.loc[df_presupuesto['UEN']=='SARMIENTO']
egncM1=egnctotales.loc[df_presupuesto['UEN']=='MERC GUAYMALLEN']
egncM2=egnctotales.loc[df_presupuesto['UEN']=='MERCADO 2']
egncP1=egnctotales.loc[df_presupuesto['UEN']=='PERDRIEL']
egncSJ=egnctotales.loc[df_presupuesto['UEN']=='SAN JOSE']
egncL=egnctotales.loc[df_presupuesto['UEN']=='LAMADRID']
egncPO=egnctotales.loc[df_presupuesto['UEN']=='PUENTE OLIVE']
egncP2=egnctotales.loc[df_presupuesto['UEN']=='PERDRIEL2']
egncA=egnctotales.loc[df_presupuesto['UEN']=='AZCUENAGA']

egncTotal = pd.concat([egncAC,egncA,egncP2,egncPO,egncL,egncSJ,egncP1,egncM2,egncM1,egncM,egncS,egncLH,egncVN,egncU])
egncTotal = egncTotal.rename({'Fecha':'FECHASQL','VENTAS':'Venta diaria'},axis=1)
egncTotal = egncTotal.reindex(columns=['UEN','FECHASQL','Venta diaria'])


################################################
############# Volumen diario GASOLEOS YPF 
################################################

df_gncYPF = pd.read_sql('''
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
        
    SELECT UEN  
        ,SUM([VTATOTVOL]) AS 'VENTA TOTAL VOLUMEN'
		,FECHASQL
		,sum(VTAPRECARTELVOL) AS 'Ventas Efectivo'
		,MAX(PRECARTEL) as 'Precio Cartel'
		,sum(VTACTACTEVOL + VTAADELVOL) AS 'Venta Cta Cte'
		,MAX(PREVTAADEL) AS 'Precio Cta Cte'
		,sum(VTAPROMOSVOL) AS 'ventas Promos pc'
    FROM [Rumaos].[dbo].[EmpVenta]
    WHERE FECHASQL >= @inicioMesActual
        AND FECHASQL < @hoy
	    AND VTATOTVOL > '0'
		and CODPRODUCTO = 'GNC'
		group BY FECHASQL,UEN
		order by UEN

  '''      ,db_conex)
df_gncYPF = df_gncYPF.convert_dtypes()

presupuestoGNC = egncTotal


######################
### VOLUMEN PROMOS ###
######################
df_GNCpromos = pd.read_sql(''' 
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
	SELECT   emp.UEN,MAX(emp.PRECIO)as 'precio promo',sum(emp.VOLUMEN) as 'volumen promo',emp.FECHASQL
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL >= @inicioMesActual
        AND FECHASQL < @hoy
		and (emp.CODPRODUCTO = 'GNC')
		and (P.[DESCRIPCION] like '%PRUEBA%')
 GROUP BY FECHASQL,emp.UEN
   '''  ,db_conex)
df_GNCpromos = df_GNCpromos.convert_dtypes()

df_gncYPF=df_gncYPF.merge(df_GNCpromos,on=['UEN','FECHASQL'],how='outer')

df_gncYPF['UEN']=df_gncYPF['UEN'].str.strip()
df_GNCpromos['UEN']=df_GNCpromos['UEN'].str.strip()
costoGNC['UEN']=costoGNC['UEN'].str.strip()



df_gncYPF=df_gncYPF.merge(presupuestoGNC,on=['UEN','FECHASQL'],how='outer')

# convertir la columna 'Precio Cartel' a un tipo de datos numérico
df_gncYPF['Precio Cartel'] = pd.to_numeric(df_gncYPF['Precio Cartel'], errors='coerce')

# reemplazar los valores 0 en la columna 'Precio Cartel' con valores nulos (NaN)
df_gncYPF['Precio Cartel'] = df_gncYPF['Precio Cartel'].replace(0, np.nan)
# llenar los valores nulos utilizando la función fillna de Pandas con el argumento method='ffill'
df_gncYPF['Precio Cartel'] = df_gncYPF['Precio Cartel'].fillna(method='ffill')
df_gncYPF['Precio Cartel'] = df_gncYPF['Precio Cartel'].fillna(method='bfill')  

df_gncYPF= df_gncYPF.loc[(df_gncYPF["FECHASQL"] <= ayer.strftime('%Y-%m-%d')) & (df_gncYPF["FECHASQL"] >= primer_dia_mes.strftime('%Y-%m-%d')),:]


df_gncYPF=df_gncYPF.fillna(0)
df_gncYPF['Total Vendido $'] = ((df_gncYPF['Ventas Efectivo']*df_gncYPF['Precio Cartel'])
                                       +(df_gncYPF['Venta Cta Cte']*df_gncYPF['Precio Cta Cte'])
                                    )
df_gncYPF['Cantidad Vendida PC'] = (df_gncYPF['Ventas Efectivo']+
                                +df_gncYPF['Venta Cta Cte']+
                                df_gncYPF['ventas Promos pc']-
                                df_gncYPF['volumen promo']
                                )
df_gncYPF=df_gncYPF.sort_values(['UEN','FECHASQL'])

def mbcypf(df,estacion):
    costo = costoGNC.loc[costoGNC["UEN"] == estacion, "Costo M3"].iloc[0]
    #costo = costoGNC.loc[(costoGNC["UEN"] == estacion)]
    df['Comision']=df['Precio Cartel']-costo
    df['Comision Promos']=df['precio promo']-costo
    df['Volumen Promos PC']= df['ventas Promos pc']-df['volumen promo']
    df['Ventas Acumuladas $ PC']= df['Total Vendido $']+(df['Volumen Promos PC']*df['Precio Cartel'])
    df['Ventas Acumuladas $']= df['Ventas Acumuladas $ PC']+(df['volumen promo']*df['precio promo'])
    df['Presupuesto Acumulado $']=df['Venta diaria']*df['Precio Cartel']
    df['MBC Presupuestado $']=df['Venta diaria']*df['Comision']
    df['MBC Acumulado $']=(df['Cantidad Vendida PC']*df['Comision'])+(df['volumen promo']*df['Comision Promos'])
    df = df.reindex(columns=['UEN','Presupuesto Acumulado $','Ventas Acumuladas $','MBC Presupuestado $','MBC Acumulado $'])
    df = df.groupby(
        ["UEN"]
        , as_index=False
    ).sum(numeric_only=True)
    return df


### Aplico la comision a cada estacion para el producto NS
perdriel1= df_gncYPF.loc[(df_gncYPF["UEN"] == 'PERDRIEL'),:]
p1=perdriel1
p1 = p1.reset_index()
perdriel2= df_gncYPF.loc[(df_gncYPF["UEN"] == 'PERDRIEL2'),:]
perdriel2=perdriel2.reset_index()
for i in perdriel2.index:
    if perdriel2.loc[i,'precio promo']==0:
        perdriel2.loc[i,'precio promo'] = p1.loc[i,'precio promo']

azcuenaga= df_gncYPF.loc[(df_gncYPF["UEN"] == 'AZCUENAGA'),:]
san_Jose= df_gncYPF.loc[(df_gncYPF["UEN"] == 'SAN JOSE'),:]
puente_Olive= df_gncYPF.loc[(df_gncYPF["UEN"] == 'PUENTE OLIVE'),:]
lamadrid= df_gncYPF.loc[(df_gncYPF["UEN"] == 'LAMADRID'),:]
las_heras= df_gncYPF.loc[(df_gncYPF["UEN"] == 'LAS HERAS'),:]
mercado1= df_gncYPF.loc[(df_gncYPF["UEN"] == 'MERC GUAYMALLEN'),:]
mercado2= df_gncYPF.loc[(df_gncYPF["UEN"] == 'MERCADO 2'),:]
sarmiento= df_gncYPF.loc[(df_gncYPF["UEN"] == 'SARMIENTO'),:]
villa_nueva= df_gncYPF.loc[(df_gncYPF["UEN"] == 'VILLANUEVA'),:]
adolfo_Calle= df_gncYPF.loc[(df_gncYPF["UEN"] == 'ADOLFO CALLE'),:]
mitre= df_gncYPF.loc[(df_gncYPF["UEN"] == 'MITRE'),:]
urquiza= df_gncYPF.loc[(df_gncYPF["UEN"] == 'URQUIZA'),:]

perdriel1 = mbcypf(perdriel1,'PERDRIEL')
perdriel2 = mbcypf(perdriel2,'PERDRIEL2')
azcuenaga = mbcypf(azcuenaga,'AZCUENAGA')
san_Jose = mbcypf(san_Jose,'SAN JOSE')
puente_Olive = mbcypf(puente_Olive,'PUENTE OLIVE')
lamadrid = mbcypf(lamadrid,'LAMADRID')
las_heras = mbcypf(las_heras,'LAS HERAS')
mercado1 = mbcypf(mercado1,'MERC GUAYMALLEN')
mercado2 = mbcypf(mercado2,'MERCADO 2')
sarmiento = mbcypf(sarmiento,'SARMIENTO')
villa_nueva = mbcypf(villa_nueva,'VILLANUEVA')
adolfo_Calle = mbcypf(adolfo_Calle,'ADOLFO CALLE')
mitre = mbcypf(mitre,'MITRE')
urquiza = mbcypf(urquiza,'URQUIZA')

mbcTOTAL=perdriel1.merge(perdriel2,on=['UEN','Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $'],how='outer')
mbcTOTAL=mbcTOTAL.merge(azcuenaga,on=['UEN','Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $'],how='outer')
mbcTOTAL=mbcTOTAL.merge(san_Jose,on=['UEN','Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $'],how='outer')
mbcTOTAL=mbcTOTAL.merge(puente_Olive,on=['UEN','Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $'],how='outer')
mbcTOTAL=mbcTOTAL.merge(las_heras,on=['UEN','Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $'],how='outer')
mbcTOTAL=mbcTOTAL.merge(lamadrid,on=['UEN','Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $'],how='outer')
mbcTOTAL=mbcTOTAL.merge(mercado1,on=['UEN','Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $'],how='outer')
mbcTOTAL=mbcTOTAL.merge(mercado2,on=['UEN','Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $'],how='outer')
mbcTOTAL=mbcTOTAL.merge(sarmiento,on=['UEN','Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $'],how='outer')
mbcTOTAL=mbcTOTAL.merge(villa_nueva,on=['UEN','Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $'],how='outer')
mbcTOTAL=mbcTOTAL.merge(adolfo_Calle,on=['UEN','Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $'],how='outer')
mbcTOTAL=mbcTOTAL.merge(mitre,on=['UEN','Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $'],how='outer')
mbcTOTAL=mbcTOTAL.merge(urquiza,on=['UEN','Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $'],how='outer')

mbcTOTAL=mbcTOTAL.sort_values(['UEN'])
###### Columnas de Desvio y Totales NS
mbcTOTAL['Desvio Presupuestado %']=(mbcTOTAL['Ventas Acumuladas $']/mbcTOTAL['Presupuesto Acumulado $'])-1
mbcTOTAL['Desvio MBC %']=(mbcTOTAL['MBC Acumulado $']/mbcTOTAL['MBC Presupuestado $'])-1
mbcTOTAL['MBC %']=mbcTOTAL['MBC Acumulado $']/mbcTOTAL['Ventas Acumuladas $']
mbcTOTAL.loc["colTOTAL"]= pd.Series(
    mbcTOTAL.sum(numeric_only=True)
    , index=['Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $']
)
mbcTOTAL.fillna({"UEN":"TOTAL"}, inplace=True)

tasa = (mbcTOTAL.loc["colTOTAL",'Ventas Acumuladas $'] /
    mbcTOTAL.loc["colTOTAL",'Presupuesto Acumulado $'])-1
mbcTOTAL.fillna({"Desvio Presupuestado %":tasa}, inplace=True)

tasa2 = (mbcTOTAL.loc["colTOTAL",'MBC Acumulado $'] /
    mbcTOTAL.loc["colTOTAL",'MBC Presupuestado $'])-1
mbcTOTAL.fillna({'Desvio MBC %':tasa2}, inplace=True)

tasa3 = (mbcTOTAL.loc["colTOTAL",'MBC Acumulado $'] /
    mbcTOTAL.loc["colTOTAL",'Ventas Acumuladas $'])
mbcTOTAL.fillna({'MBC %':tasa3}, inplace=True)

mbcTOTAL=mbcTOTAL.reindex(columns=['UEN','Presupuesto Acumulado $','Ventas Acumuladas $','Desvio Presupuestado %','MBC Presupuestado $','MBC Acumulado $','Desvio MBC %'])

# Creo Variable para margen Empresa
mbcGNC=mbcTOTAL


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
        .format("{:,.2%}", subset=list_Col_Perc) \
        .hide(axis=0) \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>") \
        .applymap(lambda x: 'color: blue' if x >= 0 else 'color: #FF0000', subset=pd.IndexSlice[:, ['Desvio MBC %']]) \
        .applymap(lambda x: 'color: blue' if x >= 0 else 'color: #FF0000', subset=pd.IndexSlice[:, ['Desvio Presupuestado %']]) \
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

#  columnas sin decimales
numCols = [ 'Ventas Acumuladas $','Presupuesto Acumulado $','MBC Presupuestado $','MBC Acumulado $']

# Columnas Porcentaje
percColsPen = ["Desvio MBC %"
            ,"Desvio Presupuestado %"
]

### APLICO EL FORMATO A LA TABLA
mbcTOTAL = _estiladorVtaTituloD(mbcTOTAL,numCols,percColsPen, "MBC GNC")

#### DEFINO EL DESTINO DONDE SE GUARDARA LA IMAGEN Y EL NOMBRE
ubicacion= str(pathlib.Path(__file__).parent)+"\\"

#ubicacion = "C:/Informes/Margen_Playa/"
nombrePen = "MBCGNC.png"
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

df_to_image(mbcTOTAL, ubicacion, nombrePen)


#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)


