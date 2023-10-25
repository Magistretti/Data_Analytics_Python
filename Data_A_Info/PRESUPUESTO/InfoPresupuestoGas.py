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

hoy = datetime.now()
ayer = hoy - timedelta(days=1)
primer_dia_mes = hoy.replace(day=1)
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
    # TRABAJO CON TABLA DE SHEET    ################
    ##########################################

sheet_id='1ofJ38O2-dLfw7TZOxeTDYUzlWgl6hN2iMLZCMscTgGo'
hoja='Presupuesto'
gsheet_url_presupuesto = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id, hoja)
df_presupuesto=pd.read_csv(gsheet_url_presupuesto)
df_presupuesto = df_presupuesto.convert_dtypes()
df_presupuesto['FECHASQL'] = pd.to_datetime(df_presupuesto['FECHASQL'], format='%d/%m/%Y')
df_presupuesto= df_presupuesto.loc[(df_presupuesto["FECHASQL"] <= ayer.strftime('%Y-%m-%d'))& (df_presupuesto["FECHASQL"] >= primer_dia_mes.strftime('%Y-%m-%d')),:]

df_presupuesto_gas= df_presupuesto.loc[df_presupuesto['CODPRODUCTO']=='GNC']
df_presupuesto_mensual= df_presupuesto_gas.groupby(
    ["UEN"]
    , as_index=False
).sum(numeric_only=True)

df_presupuesto_mensual=df_presupuesto_mensual.rename({'VENTAS':'Presupuesto Mensual'},axis=1)
egnctotales =df_presupuesto_mensual


egncAC=df_presupuesto_gas.loc[df_presupuesto['UEN']=='ADOLFO CALLE']
egncU=df_presupuesto_gas.loc[df_presupuesto['UEN']=='URQUIZA']
egncVN=df_presupuesto_gas.loc[df_presupuesto['UEN']=='VILLANUEVA']
egncLH=df_presupuesto_gas.loc[df_presupuesto['UEN']=='LAS HERAS']
egncM=df_presupuesto_gas.loc[df_presupuesto['UEN']=='MITRE']
egncS=df_presupuesto_gas.loc[df_presupuesto['UEN']=='SARMIENTO']
egncM1=df_presupuesto_gas.loc[df_presupuesto['UEN']=='MERC GUAYMALLEN']
egncM2=df_presupuesto_gas.loc[df_presupuesto['UEN']=='MERCADO 2']
egncP1=df_presupuesto_gas.loc[df_presupuesto['UEN']=='PERDRIEL']
egncSJ=df_presupuesto_gas.loc[df_presupuesto['UEN']=='SAN JOSE']
egncL=df_presupuesto_gas.loc[df_presupuesto['UEN']=='LAMADRID']
egncPO=df_presupuesto_gas.loc[df_presupuesto['UEN']=='PUENTE OLIVE']
egncP2=df_presupuesto_gas.loc[df_presupuesto['UEN']=='PERDRIEL2']
egncA=df_presupuesto_gas.loc[df_presupuesto['UEN']=='AZCUENAGA']
egncTotal = pd.concat([egncAC,egncA,egncP2,egncPO,egncL,egncSJ,egncP1,egncM2,egncM1,egncM,egncS,egncLH,egncVN,egncU])

egncTotal=egncTotal.rename({'VENTAS':'Venta diaria', 'FECHASQL':'Fecha'},axis=1)

### Volumen Acumulado proyectado Filtro datos
acumulado= egncTotal["Fecha"] <= ayer.strftime('%Y-%m-%d')
volacuProy = egncTotal[acumulado]

ac = volacuProy['UEN'] == 'ADOLFO CALLE'
ac = volacuProy[ac]
adolfo_c = sum(ac['Venta diaria'])


ac = volacuProy['UEN'] == 'AZCUENAGA'
ac = volacuProy[ac]
azcuenaga = sum(ac['Venta diaria'])


ac = volacuProy['UEN'] == 'LAMADRID'
ac = volacuProy[ac]
lamadrid = sum(ac['Venta diaria'])


ac = volacuProy['UEN'] == 'LAS HERAS'
ac = volacuProy[ac]
las_heras = sum(ac['Venta diaria'])


ac = volacuProy['UEN'] == 'MERC GUAYMALLEN'
ac = volacuProy[ac]
merc_guay = sum(ac['Venta diaria'])


ac = volacuProy['UEN'] == 'MERCADO 2'
ac = volacuProy[ac]
merc_2 = sum(ac['Venta diaria'])


ac = volacuProy['UEN'] == 'MITRE'
ac = volacuProy[ac]
mitre = sum(ac['Venta diaria'])


ac = volacuProy['UEN'] == 'PERDRIEL'
ac = volacuProy[ac]
pedriel_1 = sum(ac['Venta diaria'])


ac = volacuProy['UEN'] == 'PERDRIEL2'
ac = volacuProy[ac]
pedriel_2 = sum(ac['Venta diaria'])


ac = volacuProy['UEN'] == 'PUENTE OLIVE'
ac = volacuProy[ac]
puente_o = sum(ac['Venta diaria'])

ac = volacuProy['UEN'] == 'SAN JOSE'
ac = volacuProy[ac]
san_jose = sum(ac['Venta diaria'])


ac = volacuProy['UEN'] == 'SARMIENTO'
ac = volacuProy[ac]
sarmiento = sum(ac['Venta diaria'])


ac = volacuProy['UEN'] == 'URQUIZA'
ac = volacuProy[ac]
urquiza = sum(ac['Venta diaria'])


ac = volacuProy['UEN'] == 'VILLANUEVA'
ac = volacuProy[ac]
villa_n = sum(ac['Venta diaria'])

volaccdicc={}
volaccdicc={
    'ADOLFO CALLE': adolfo_c,
    'AZCUENAGA': azcuenaga,
    'LAMADRID': lamadrid,
    'LAS HERAS': las_heras,
    'MERC GUAYMALLEN':merc_guay,
    'MERCADO 2':merc_2,
    'MITRE':mitre,
    'PERDRIEL':pedriel_1,
    'PERDRIEL2':pedriel_2,
    'PUENTE OLIVE':puente_o,
    'SAN JOSE':san_jose,
    'SARMIENTO':sarmiento,
    'URQUIZA':urquiza,
    'VILLANUEVA':villa_n
}
volumProy = pd.DataFrame([[key, volaccdicc[key]] for key in volaccdicc.keys()], columns=['UEN', 'PRESUPUESTO ACUMULADO'])


####################    Volumen diario Proyectado filtro datos  ################################
fecha = egncTotal['Fecha']==ayer.strftime('%Y-%m-%d')
egncT = egncTotal[fecha]





##################################        VENTA DE GNC MENSUAL        ##########################

df_gncemes = pd.read_sql('''
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

        SELECT  UEN,sum([VTATOTVOL]) AS 'Volumen Acumulado' 
		from EmpVenta 
		where VTATOTVOL > '0'
        AND CODPRODUCTO = 'GNC'
		AND FECHASQL >= @inicioMesActual
        AND FECHASQL < @hoy
		group by UEN
		order by UEN
  '''      ,db_conex)
df_gncemes = df_gncemes.convert_dtypes()

######## GNC REGALOS
df_regalo_GNC = pd.read_sql(
       """
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
            EmP.[UEN], SUM(-emp.VOLUMEN) as 'Volumen Acumulado'
        FROM [Rumaos].[dbo].[EmpPromo] AS EmP
            INNER JOIN Promocio AS P 
                ON EmP.UEN = P.UEN 
                AND EmP.CODPROMO = P.CODPROMO
            WHERE FECHASQL >= @inicioMesActual
                AND FECHASQL < @hoy
                AND EmP.VOLUMEN > '0'
                AND EmP.CODPRODUCTO = 'GNC'
                AND (P.[DESCRIPCION] like '%PRUEBA%')
			group by EmP.[UEN]

        """
        , db_conex
    )
df_regalo_GNC = df_regalo_GNC.convert_dtypes()

df_gncemes = df_gncemes.merge(df_regalo_GNC,on=['UEN','Volumen Acumulado'],how='outer')

df_gncemes = df_gncemes.groupby(
    ["UEN"]
    , as_index=False
).sum(numeric_only=True)

egncT=egncT.sort_values('UEN', ascending=True)
egncT= egncT.reset_index()


#####################        VENTA DE GNC DIARIA     ##############################        

df_ventagnc = pd.read_sql("""
    SELECT  
        [UEN]
        ,[FECHASQL]
        ,[CODPRODUCTO]
        ,[VTATOTVOL] as Volumen
    FROM [Rumaos].[dbo].[EmpVenta]
    WHERE FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
	    AND EmpVenta.VTATOTVOL > '0'
        and CODPRODUCTO = 'GNC'
""", db_conex)

df_ventagnc = df_ventagnc.convert_dtypes()

#########REGALOS DE GNC DIARIOS
df_regalosTraslados = pd.read_sql("""
    SELECT
        EmP.[UEN]
        ,EmP.[FECHASQL]
        ,EmP.[CODPRODUCTO]
        ,-EmP.[VOLUMEN] as Volumen
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN [Rumaos].[dbo].[Promocio] AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
        AND EmP.VOLUMEN > '0' 
        AND EmP.CODPRODUCTO = 'GNC'
        AND (EmP.[CODPROMO] = '30'
            OR P.[DESCRIPCION] like '%PRUEBA%'
            OR P.[DESCRIPCION] like '%TRASLADO%'
            OR P.[DESCRIPCION] like '%MAYORISTA%'
        )
""", db_conex)

df_regalosTraslados = df_regalosTraslados.convert_dtypes()

df_ventagnc = df_ventagnc.merge(df_regalosTraslados,on=['UEN','Volumen'],how='outer')

df_ventagnc = df_ventagnc.groupby(
    ["UEN"]
    , as_index=False
).sum(numeric_only=True)



egncT=egncT.sort_values('UEN', ascending=True)
egncT= egncT.reset_index()

#########           ELIMINO COLUMNAS DE ALGUNAS columnas          #############
egncT = egncT.loc[:,egncT.columns!="Día"]
egncT = egncT.loc[:,egncT.columns!="Porcentaje"]
egncT = egncT.loc[:,egncT.columns!="Fecha"]
egncT = egncT.loc[:,egncT.columns!="index"]

#######      CONECTO DOS TABLAS CON MARGE POR SI ALGUNA NO LLEGA A TENER VENTAS DIARIAS     ############
volumProy['UEN']=volumProy['UEN'].str.strip()
df_gncemes['UEN']=df_gncemes['UEN'].str.strip()
df_ventagnc['UEN']=df_ventagnc['UEN'].str.strip()

egnc=df_gncemes.merge(volumProy, on="UEN", how="outer")



egnc=egnc.merge(df_ventagnc,on='UEN',how='outer')
egnc = egnc.fillna(0)


egnc['UEN']=egnc['UEN'].str.strip()
egnc1=egnc
egnc1=egnc1.merge(egnctotales,on='UEN',how='outer')
egnc1=egnc1.merge(egncT,on='UEN',how='outer')

##############          MODIFICO Y CREO COLUMNAS      ##################
egnc1['Volumen1']=egnc1['Volumen'] - egnc1["Venta diaria"]
egnc1['VolumenAcumulado'] =egnc1['Volumen Acumulado'] - egnc1['PRESUPUESTO ACUMULADO']
egnc1 = egnc1.reindex(columns=["UEN",'Venta diaria',"Presupuesto Mensual",'Volumen',"PRESUPUESTO ACUMULADO","Volumen Acumulado",'Volumen1','VolumenAcumulado'])


porcen = (egnc1["Volumen1"] 
 / egnc1['Venta diaria']
)
porcen1=(egnc1['VolumenAcumulado']/
    egnc1['PRESUPUESTO ACUMULADO']
)
egnc1['Porcentaje'] = porcen1
egnc1['Porcentual'] = porcen

#creo totales
egnc1.loc["colTOTAL"]= pd.Series(
    egnc1.sum(numeric_only=True)
    , index=["Venta diaria","Volumen1","Presupuesto Mensual","Volumen","PRESUPUESTO ACUMULADO","Volumen Acumulado","VolumenAcumulado"]
)
egnc1.fillna({"UEN":"TOTAL"}, inplace=True)

tasa = (egnc1.loc["colTOTAL","Volumen1"] /
    egnc1.loc["colTOTAL","Venta diaria"])
egnc1.fillna({"Porcentual":tasa}, inplace=True)

#Creo totales de porcentual acumulado

tasa1 = (egnc1.loc["colTOTAL","VolumenAcumulado"] /
    egnc1.loc["colTOTAL","PRESUPUESTO ACUMULADO"])
egnc1.fillna({"Porcentaje":tasa1}, inplace=True)


#########Modificacion INDICES DE columnas
egnc1 = egnc1.reindex(columns=["UEN","Presupuesto Mensual","Venta diaria","Volumen","Volumen1"
,"Porcentual","PRESUPUESTO ACUMULADO","Volumen Acumulado","VolumenAcumulado","Porcentaje"])

##### RENOMBRO COLUMNAS####

egnc1 = egnc1.rename({'UEN': 'UEN', 'Presupuesto Mensual': 'Presupuesto Mensual', 'Venta diaria': 'Presupuesto Diario','Volumen':'Venta Diaria'
                      ,'Volumen1':'Desvio Diario','Porcentual':'Porcentual Diario', 'PRESUPUESTO ACUMULADO':'Presupuesto Acumulado'
                      ,'Volumen Acumulado':'Ventas Acumuladas','VolumenAcumulado':'Desvio Acumulado','Porcentaje':'Porcentual Acumulado'}, axis=1)
##############
# STYLING of the dataframe
##############
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
        .format("{0:,.0f}", subset=list_Col_Num) \
        .format("{:,.2%}", subset=list_Col_Perc) \
        .hide(axis=0) \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            ) \
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
    evitarTotales = egnc1.index.get_level_values(0)

    resultado = resultado.background_gradient(
        cmap="RdYlGn" # Red->Yellow->Green
        ,vmin=-0.1
        ,vmax=0.05
        ,subset=pd.IndexSlice[evitarTotales[:-1],"Porcentual Acumulado"]
    )
    resultado = resultado.background_gradient(
        cmap="RdYlGn" # Red->Yellow->Green
        ,vmin=-0.1
        ,vmax=0.05
        ,subset=pd.IndexSlice[evitarTotales[:-1],"Porcentual Diario"]
    )
    
    return resultado


numCols = [
        
 'Presupuesto Diario'
        ,'Venta Diaria'
        ,'Desvio Diario'
        ,'Presupuesto Acumulado'
        ,'Presupuesto Mensual'
        ,'Ventas Acumuladas'
        ,'Desvio Acumulado'
         ]

percCols = ["Porcentual Diario"
            ,"Porcentual Acumulado"
    ]


egncFinal = _estiladorVtaTitulo(egnc1, numCols, percCols, "CÁLCULO EJECUCIÓN PRESUPUESTARIA GNC")
##############
# PRINTING dataframe as an image
##############


ubicacion= str(pathlib.Path(__file__).parent)+"\\"

nombreGNC = "Info_Presupuesto_GNC.png"
nombreGNCproy = "prueba.png"
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

df_to_image(egncFinal, ubicacion, nombreGNC)



#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)




























