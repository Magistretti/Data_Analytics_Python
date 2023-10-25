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
#################################       LEER EXCEL

    ##########################################
    # TRABAJO CON TABLA DE EXCEL    ################
    ##########################################

ubicacion = str(pathlib.Path(__file__).parent)+"\\"
aux_semanal = "Presupuesto_GNC_Noviembre.xlsx"
egnctotales =pd.read_excel(ubicacion+aux_semanal, sheet_name= 'Hoja1')
egnctotales = egnctotales.convert_dtypes()
egncAC=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"Adolfo Calle"
)
egncAC = egncAC.convert_dtypes()
egncU=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"Urquiza"
)
egncU = egncU.convert_dtypes()
egncVN=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"Villa Nueva"
)
egncVN = egncVN.convert_dtypes()
egncLH=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"Las Heras"
)
egncLH = egncLH.convert_dtypes()
egncM=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"Mitre"
)
egncM = egncM.convert_dtypes()
egncS=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"Sarmiento"
)
egncS = egncS.convert_dtypes()
egncM1=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"Mercado 1"
)
egncM1 = egncM1.convert_dtypes()
egncM2=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"Mercado 2"
)
egncM2 = egncM2.convert_dtypes()
egncP1=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"Perdriel 1"
)
egncP1 = egncP1.convert_dtypes()
egncSJ=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"San José"
)
egncSJ = egncSJ.convert_dtypes()
egncL=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"Lamadrid"
)
egncL = egncL.convert_dtypes()
egncPO=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"Puente Olive"
)
egncPO = egncPO.convert_dtypes()
egncP2=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"Perdriel 2"
)
egncP2 = egncP2.convert_dtypes()
egncA=pd.read_excel(ubicacion+aux_semanal, sheet_name= 
"Azcuenaga"
)
egncA = egncA.convert_dtypes()

egncTotal = pd.concat([egncAC,egncA,egncP2,egncPO,egncL,egncSJ,egncP1,egncM2,egncM1,egncM,egncS,egncLH,egncVN,egncU])


hoy = datetime.now()
ayer = hoy - timedelta(days=1)
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

        SELECT  
                RTRIM(T1.[UEN]) AS 'UEN'
                ,RTRIM(T1.[CODPRODUCTO]) AS 'CODPRODUCTO'
                , T2.[Volumen Acumulado]
                ,sum([VTATOTVOL]) AS 'Mes Anterior Vol Proyectado'
                , T2.[Mes Actual Vol Proyectado]
            FROM [Rumaos].[dbo].[EmpVenta] as T1
            Full Outer JOIN (SELECT
                SQ.UEN
                , SQ.CODPRODUCTO
                ,sum(SQ.[VTATOTVOL]) AS 'Volumen Acumulado'
                ,sum(SQ.[VTATOTVOL]) AS 'Mes Actual Vol Proyectado'
            FROM [Rumaos].[dbo].[EmpVenta] as SQ
            WHERE SQ.FECHASQL >= @inicioMesActual
                AND SQ.FECHASQL < @hoy
                AND SQ.VTATOTVOL > '0'
                AND SQ.CODPRODUCTO = 'GNC'
                group by SQ.UEN, SQ.CODPRODUCTO
                ) AS T2
                ON T1.UEN = T2.UEN AND T1.CODPRODUCTO = T2.CODPRODUCTO
            WHERE FECHASQL >= @inicioMesAnterior
                AND FECHASQL < @inicioMesActual
                AND T1.VTATOTVOL > '0'
                AND T1.CODPRODUCTO = 'GNC'
                group by T1.UEN
                    , T1.CODPRODUCTO
                    , T2.[Volumen Acumulado]
                    , T2.[Mes Actual Vol Proyectado]
                order by T1.UEN, T1.CODPRODUCTO
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
            RTRIM(EmP.[UEN]) AS UEN
            ,RTRIM(EmP.[CODPRODUCTO]) AS CODPRODUCTO
            ,T2.[Volumen Acumulado]
            ,sum(-EmP.[VOLUMEN]) AS 'Mes Anterior Vol Proyectado'
            ,T2.[Mes Actual Vol Proyectado]
        FROM [Rumaos].[dbo].[EmpPromo] AS EmP
            INNER JOIN Promocio AS P 
                ON EmP.UEN = P.UEN 
                AND EmP.CODPROMO = P.CODPROMO
        FULL OUTER JOIN (SELECT
                EmP.[UEN]
                ,EmP.[CODPRODUCTO]
                ,sum(-EmP.[VOLUMEN]) AS 'Volumen Acumulado'
                ,sum(-EmP.[VOLUMEN] ) AS 'Mes Actual Vol Proyectado'
            FROM [Rumaos].[dbo].[EmpPromo] AS EmP
                INNER JOIN Promocio AS P 
                    ON EmP.UEN = P.UEN 
                    AND EmP.CODPROMO = P.CODPROMO
            WHERE FECHASQL >= @inicioMesActual
                AND FECHASQL < @hoy
                AND EmP.VOLUMEN > '0'
                AND EmP.CODPRODUCTO = 'GNC'
                AND (EmP.[CODPROMO] = '30'
                    OR P.[DESCRIPCION] like '%PRUEBA%'
                    OR P.[DESCRIPCION] like '%TRASLADO%'
                    OR P.[DESCRIPCION] like '%MAYORISTA%'
                )
                group by EmP.UEN, EmP.CODPRODUCTO
                ) AS T2
                ON EmP.UEN = T2.UEN AND EmP.CODPRODUCTO = T2.CODPRODUCTO
            WHERE FECHASQL >= @inicioMesAnterior
                AND FECHASQL < @inicioMesActual
                AND EmP.VOLUMEN > '0'
                AND EmP.CODPRODUCTO = 'GNC'
                AND (EmP.[CODPROMO] = '30'
                    OR P.[DESCRIPCION] like '%PRUEBA%'
                    OR P.[DESCRIPCION] like '%TRASLADO%'
                    OR P.[DESCRIPCION] like '%MAYORISTA%'
                )
                group by EmP.UEN
                    , EmP.CODPRODUCTO
                    , T2.[Volumen Acumulado]
                    , T2.[Mes Actual Vol Proyectado]
                order by EmP.UEN, EmP.CODPRODUCTO
        """
        , db_conex
    )
df_regalo_GNC = df_regalo_GNC.convert_dtypes()

df_vtasGNC_neto = df_gncemes.set_index(["UEN","CODPRODUCTO"])
df_vtasGNC_neto = df_vtasGNC_neto.add(
    df_regalo_GNC.set_index(["UEN","CODPRODUCTO"])
    , fill_value=0
    )
df_vtas_GNC_neto = df_vtasGNC_neto.reset_index()

df_vtasGNC_neto = df_vtasGNC_neto.groupby(
        ["UEN"]
        , as_index=False
    ).sum()

#####################        VENTA DE GNC DIARIA     ##############################        

df_ventagnc = pd.read_sql("""
    SELECT  
        [UEN]
        ,[FECHASQL]
        ,[CODPRODUCTO]
        ,[VTATOTVOL]
    FROM [Rumaos].[dbo].[EmpVenta]
    WHERE FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
	    AND EmpVenta.VTATOTVOL > '0'
        and CODPRODUCTO = 'GNC'
""", db_conex)

df_ventagnc = df_ventagnc.convert_dtypes()
# Removing trailing whitespace from the UEN and CODPRODUCTO columns
df_ventagnc["UEN"] = df_ventagnc["UEN"].str.strip()
df_ventagnc["CODPRODUCTO"] = df_ventagnc["CODPRODUCTO"].str.strip()
pd.options.display.float_format = "{:20,.0f}".format 

#########REGALOS DE GNC DIARIOS
df_regalosTraslados = pd.read_sql("""
    SELECT
        EmP.[UEN]
        ,EmP.[FECHASQL]
        ,EmP.[CODPRODUCTO]
        ,-EmP.[VOLUMEN] as VTATOTVOL
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
# Removing whitespace
df_regalosTraslados["UEN"] = df_regalosTraslados["UEN"].str.strip()
df_regalosTraslados["CODPRODUCTO"] = \
    df_regalosTraslados["CODPRODUCTO"].str.strip()

#### A LAS VENTAS DIARIAS LE RESTO LOS REGALOS
#df_empVentaNeteado = df_ventagnc.append(df_regalosTraslados, ignore_index=True)
df_empVentaNeteado = pd.concat([df_ventagnc,df_regalosTraslados])

def grupo(codproducto):
    if codproducto == "GNC":
        return "GNC"
    else:
        return "no tiene que ir"

# Create column GRUPO from column CODPRODUCTO
df_empVentaNeteado["GRUPO"] = df_empVentaNeteado.apply(
    lambda row: grupo(row["CODPRODUCTO"])
        , axis= 1
)
######################


# Pivot table of data to get results of VTATOTVOL per UEN grouped by Grupo
df_resultados = pd.pivot_table(df_empVentaNeteado
    , values="VTATOTVOL"
    , index="UEN"
    , columns="GRUPO"
    , aggfunc=sum
    , fill_value=0
    
)

# Restore index UEN like a column
df_resultados = df_resultados.reset_index()



#################ORDENO TABLAS POR UEN###################



egncT=egncT.sort_values('UEN', ascending=True)
egncT= egncT.reset_index()
egnctotales = egnctotales.reset_index()



#########           ELIMINO COLUMNAS DE ALGUNAS TABLAS          #############
egncT = egncT.loc[:,egncT.columns!="Día"]
egncT = egncT.loc[:,egncT.columns!="Porcentaje"]
egncT = egncT.loc[:,egncT.columns!="Fecha"]
egncT = egncT.loc[:,egncT.columns!="index"]
df_resultados = df_resultados.loc[:,df_resultados.columns!="no tiene que ir"]
#######      CONECTO DOS TABLAS CON MARGE POR SI ALGUNA NO LLEGA A TENER VENTAS DIARIAS     ############
egnc=egncT.merge(df_resultados, on="UEN", how="outer")
egnc = egnc.fillna(0)
egnc=egnc.reset_index()
################        CONCATENO TABLAS             ##############
egnc=pd.concat([egnc[['UEN','Venta diaria','GNC']],volumProy[['PRESUPUESTO ACUMULADO']],df_vtasGNC_neto[['Volumen Acumulado']]], axis=1)
egnc = egnc.merge(egnctotales, on='UEN', how='outer')
##############          MODIFICO Y CREO COLUMNAS      ##################

egnc1 = egnc.assign(Volumen= egnc['GNC'] - egnc["Venta diaria"])
egnc1 = egnc1.assign(VolumenAcumulado=egnc1['Volumen Acumulado'] - egnc1['PRESUPUESTO ACUMULADO'])
#"{0:.0f}%".format(float(porcen))
egnc1 = egnc1.set_index(["UEN","Venta diaria","GNC","PRESUPUESTO ACUMULADO","Volumen Acumulado"])
egnc1 = egnc1.reset_index()
porcen = (egnc1["Volumen"] 
 / egnc1['Venta diaria']
)
porcen1=(egnc1['VolumenAcumulado']/
    egnc1['PRESUPUESTO ACUMULADO']
)
egnc1 = egnc1.assign(Porcentaje = porcen1)
egnc1 = egnc1.assign(Porcentual = porcen)
#creo totales
egnc1.loc["colTOTAL"]= pd.Series(
    egnc1.sum()
    , index=["Venta diaria","GNC","Presupuesto Mensual","Volumen","PRESUPUESTO ACUMULADO","Volumen Acumulado","VolumenAcumulado"]
)
egnc1.fillna({"UEN":"TOTAL"}, inplace=True)
#Creo totales de Porcentual diario
tasa = (egnc1.loc["colTOTAL","Volumen"] /
    egnc1.loc["colTOTAL","Venta diaria"])
egnc1.fillna({"Porcentual":tasa}, inplace=True)
#Creo totales de porcentual acumulado
tasa1 = (egnc1.loc["colTOTAL","VolumenAcumulado"] /
    egnc1.loc["colTOTAL","PRESUPUESTO ACUMULADO"])
egnc1.fillna({"Porcentaje":tasa1}, inplace=True)


#########Modificacion INDICES DE columnas
egnc1 = egnc1.reindex(columns=["UEN","Presupuesto Mensual","Venta diaria","GNC","Volumen"
,"Porcentual","PRESUPUESTO ACUMULADO","Volumen Acumulado","VolumenAcumulado","Porcentaje"])

##### RENOMBRO COLUMNAS####

egnc1 = egnc1.rename({'UEN': 'UEN', 'Presupuesto Mensual': 'Presupuesto Mensual', 'Venta diaria': 'Presupuesto Diario','GNC':'Venta Diaria','Volumen':'Desvio Diario','Porcentual':'Porcentual Diario', 'PRESUPUESTO ACUMULADO':'Presupuesto Acumulado','Volumen Acumulado':'Ventas Acumuladas','VolumenAcumulado':'Desvio Acumulado','Porcentaje':'Porcentual Acumulado'}, axis=1)
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
        .hide_index() \
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
        ,subset=pd.IndexSlice[evitarTotales[:-1],"Porcentual Diario"]
    )
    resultado = resultado.background_gradient(
        cmap="RdYlGn" # Red->Yellow->Green
        ,vmin=-0.1
        ,vmax=0.05
        ,subset=pd.IndexSlice[evitarTotales[:-1],"Porcentual Acumulado"]
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


ubicacion = "C:/Informes/PRESUPUESTO/"
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



























