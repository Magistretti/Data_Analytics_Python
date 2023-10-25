import os
import numpy as np
from DatosLogin import login
import pandas as pd
import dataframe_image as dfi
import pyodbc #Library to connect to Microsoft SQL Server
from DatosLogin import login #Holds connection data to the SQL Server
import sys
import pathlib
from calendar import monthrange
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

########################################
################ DESPACHOS  ############
########################################   
    
# Cantidad de Despachos Mes Actual
df_despachosMesActual = pd.read_sql("""
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
select a.UEN,sum(a.DESPACHOS) as 'Despachos' ,sum(a.VTATOTALVOL) as 'Volumen Vendido',a.CODPRODUCTO from 
(SELECT emp.UEN
    ,EmP.[CODPRODUCTO]
    ,SUM(EmP.[Volumen]) AS VTATOTALVOL
    ,count(id) AS DESPACHOS
FROM [Rumaos].[dbo].Despapro AS EmP
WHERE FECHASQL >= @inicioMesActual
    AND FECHASQL < @hoy
    and Volumen < '60'
GROUP BY UEN,CODPRODUCTO

UNION ALL

SELECT emp.UEN
    ,EmP.[CODPRODUCTO]
    ,SUM(-EmP.[VOLUMEN]) AS VTATOTALVOL
    ,SUM(-CANTDESPACHOS) AS DESPACHOS
FROM [Rumaos].[dbo].[EmpPromo] AS EmP
INNER JOIN Promocio AS P 
    ON EmP.UEN = P.UEN 
    AND EmP.CODPROMO = P.CODPROMO
WHERE FECHASQL >= @inicioMesActual
    AND FECHASQL < @hoy
    AND (EmP.[CODPROMO] = '30'
        OR P.[DESCRIPCION] like '%PRUEBA%')
	and Volumen < '60'
GROUP BY emp.UEN,EMP.CODPRODUCTO) as a
group by a.UEN,a.CODPRODUCTO
order by a.UEN
""", db_conex)
df_despachosMesActual = df_despachosMesActual.convert_dtypes()
df_despachosMesActual['Despachos Proyectados Mes Actual']=df_despachosMesActual['Despachos']/diasdelmes*num_days
df_despachosMesActual['Volumen Vendido proyectado Mes Actual']=df_despachosMesActual['Volumen Vendido']/diasdelmes*num_days

# Cantidad de Despachos Mes Anterior
df_despachosMesAnterior = pd.read_sql("""
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
select a.UEN,sum(a.DESPACHOS) as 'Despachos Mes Anterior',sum(a.VTATOTALVOL) as 'Volumen Vendido Mes Anterior',a.CODPRODUCTO from 
(SELECT emp.UEN
    ,EmP.[CODPRODUCTO]
    ,SUM(EmP.[Volumen]) AS VTATOTALVOL
    ,count(id) AS DESPACHOS
FROM [Rumaos].[dbo].Despapro AS EmP
WHERE FECHASQL >= @inicioMesAnterior
    AND FECHASQL < @inicioMesActual
    and Volumen < '60'
GROUP BY UEN,CODPRODUCTO

UNION ALL

SELECT emp.UEN
    ,EmP.[CODPRODUCTO]
    ,SUM(-EmP.[VOLUMEN]) AS VTATOTALVOL
    ,SUM(-CANTDESPACHOS) AS DESPACHOS
FROM [Rumaos].[dbo].[EmpPromo] AS EmP
INNER JOIN Promocio AS P 
    ON EmP.UEN = P.UEN 
    AND EmP.CODPROMO = P.CODPROMO
WHERE FECHASQL >= @inicioMesAnterior
    AND FECHASQL < @inicioMesActual
    AND (EmP.[CODPROMO] = '30'
        OR P.[DESCRIPCION] like '%PRUEBA%')
    and Volumen < '60'
GROUP BY emp.UEN,EMP.CODPRODUCTO) as a
group by a.UEN,a.CODPRODUCTO
order by a.UEN
""", db_conex)
df_despachosMesAnterior = df_despachosMesAnterior.convert_dtypes()

# Cantidad de Despachos Mes Actual del Año anterior
df_despachosAñoAnterior = pd.read_sql("""
DECLARE @inicioMesActualAnioAnterior DATETIME
SET @inicioMesActualAnioAnterior = DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1)

DECLARE @finMesActualAnioAnterior DATETIME
SET @finMesActualAnioAnterior = DATEADD(DAY, -1, DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()) + 1, 1))

select a.UEN,sum(a.DESPACHOS) as 'Despachos 2022',sum(a.VTATOTALVOL) as 'Volumen Vendido 2022',a.CODPRODUCTO from 
(SELECT emp.UEN
    ,EmP.[CODPRODUCTO]
    ,SUM(EmP.[Volumen]) AS VTATOTALVOL
    ,count(id) AS DESPACHOS
FROM [Rumaos].[dbo].Despapro AS EmP
WHERE FECHASQL >= @inicioMesActualAnioAnterior
    AND FECHASQL <= @finMesActualAnioAnterior
    and Volumen < '60'
GROUP BY UEN,CODPRODUCTO

UNION ALL

SELECT emp.UEN
    ,EmP.[CODPRODUCTO]
    ,SUM(-EmP.[VOLUMEN]) AS VTATOTALVOL
    ,SUM(-CANTDESPACHOS) AS DESPACHOS
FROM [Rumaos].[dbo].[EmpPromo] AS EmP
INNER JOIN Promocio AS P 
    ON EmP.UEN = P.UEN 
    AND EmP.CODPROMO = P.CODPROMO
WHERE FECHASQL >= @inicioMesActualAnioAnterior
    AND FECHASQL <= @finMesActualAnioAnterior
    AND (EmP.[CODPROMO] = '30'
        OR P.[DESCRIPCION] like '%PRUEBA%')
    and Volumen < '60'
GROUP BY emp.UEN,EMP.CODPRODUCTO) as a
group by a.UEN,a.CODPRODUCTO
order by a.UEN
""", db_conex)
df_despachosAñoAnterior = df_despachosAñoAnterior.convert_dtypes()
#########################################################################

despachos= df_despachosMesActual.merge(df_despachosMesAnterior,on=['UEN','CODPRODUCTO'],how='outer')
despachos= despachos.merge(df_despachosAñoAnterior,on=['UEN','CODPRODUCTO'],how='outer')
despachos=despachos.fillna(0)

for e in despachos.index:
    if despachos.loc[e,'Despachos Proyectados Mes Actual']<50:
        despachos.loc[e,'Despachos Proyectados Mes Actual']=1
        despachos.loc[e,'Volumen Vendido proyectado Mes Actual']=0
    elif despachos.loc[e,'Despachos Mes Anterior']<50:
        despachos.loc[e,'Despachos Mes Anterior']=1
        despachos.loc[e,'Volumen Vendido Mes Anterior']=0
    elif despachos.loc[e,'Despachos 2022']<50:
        despachos.loc[e,'Despachos 2022']=1
        despachos.loc[e,'Volumen Vendido 2022']=0
        
despachos['Volumen Promedio Mensual Proyectado']= despachos['Volumen Vendido proyectado Mes Actual']/despachos['Despachos Proyectados Mes Actual']
despachos['Volumen Promedio Mes Anterior']= despachos['Volumen Vendido Mes Anterior']/despachos['Despachos Mes Anterior']
despachos['Volumen Promedio Año 2022']= despachos['Volumen Vendido 2022']/despachos['Despachos 2022']

despachos['Variacion Intermensual L']= despachos['Volumen Promedio Mensual Proyectado']-despachos['Volumen Promedio Mes Anterior']
despachos['Variacion Intermensual %']= (despachos['Volumen Promedio Mensual Proyectado']/despachos['Volumen Promedio Mes Anterior'])-1

despachos['Variacion Interanual L']= despachos['Volumen Promedio Mensual Proyectado']-despachos['Volumen Promedio Año 2022']
despachos['Variacion Interanual %']= (despachos['Volumen Promedio Mensual Proyectado']/despachos['Volumen Promedio Año 2022'])-1

despachos['Variacion Despachos Intermensual %']= (despachos['Despachos Proyectados Mes Actual']/despachos['Despachos Mes Anterior'])-1

#despachos['Variacion Despachos Interanual %']= (despachos['Despachos Proyectados Mes Actual']/despachos['Despachos 2022'])-1

despachos['Variacion Despachos Interanual %'] = despachos.apply(lambda x: (x['Despachos Proyectados Mes Actual'] / x['Despachos 2022']) - 1 if x['Despachos 2022'] > 1 else 1, axis=1)

### TOTALES EU
###### Columnas de Desvio y Totales EU
### TOTALES EU
###### Columnas de Desvio y Totales EU
def totales(df):
    df.loc["colTOTAL"]= pd.Series(
        df.sum()
        ,index=['Despachos Proyectados Mes Actual','Despachos Mes Anterior'
                 ,'Despachos 2022','Volumen Vendido proyectado Mes Actual','Volumen Vendido Mes Anterior',
                'Volumen Vendido 2022', 'Variacion Despacho Intermensual %', 'Variacion Despachos Interanual %'])
    df.fillna({"UEN":"TOTAL"}, inplace=True)

    tasa = (df.loc["colTOTAL",'Volumen Vendido proyectado Mes Actual'] /
        df.loc["colTOTAL",'Despachos Proyectados Mes Actual'])
    df.fillna({'Volumen Promedio Mensual Proyectado':tasa}, inplace=True)

    tasa2 = (df.loc["colTOTAL",'Volumen Vendido Mes Anterior'] /
        df.loc["colTOTAL",'Despachos Mes Anterior'])
    df.fillna({'Volumen Promedio Mes Anterior':tasa2}, inplace=True)
    
    tasa3 = (df.loc["colTOTAL",'Volumen Vendido 2022'] /
        df.loc["colTOTAL",'Despachos 2022'])
    df.fillna({'Volumen Promedio Año 2022':tasa3}, inplace=True)
    
    tasa4 = (df.loc["colTOTAL",'Volumen Promedio Mensual Proyectado'] /
        df.loc["colTOTAL",'Volumen Promedio Año 2022'])-1
    df.fillna({'Variacion Interanual %':tasa4}, inplace=True)
    
    tasa5 = (df.loc["colTOTAL",'Volumen Promedio Mensual Proyectado'] /
        df.loc["colTOTAL",'Volumen Promedio Mes Anterior'])-1
    df.fillna({'Variacion Intermensual %':tasa5}, inplace=True)
    
    tasa6= (df.loc["colTOTAL", 'Despachos Proyectados Mes Actual'] /
           df.loc["colTOTAL", 'Despachos Mes Anterior'])-1
    df.fillna({'Variacion Despachos Intermensual %':tasa6}, inplace=True)
    
    tasa7= (df.loc["colTOTAL", 'Despachos Proyectados Mes Actual']/
           df.loc["colTOTAL", 'Despachos 2022'])-1
    df.fillna({'Variacion Despachos Interanual %':tasa7}, inplace=True)
     
    
    #df = df.reindex(columns=['UEN','CODPRODUCTO','Despachos Proyectados Mes Actual'
    #                         ,'Volumen Promedio Mensual Proyectado','Despachos Mes Anterior'
    #                         ,'Volumen Promedio Mes Anterior','Variacion Intermensual %','Variacion Despachos Intermensual %'
    #                         ,'Despachos 2022','Volumen Promedio Año 2022'
    #                         ,'Variacion Interanual %', 'Variacion Despachos Interanual %'])
    
    df = df.reindex(columns=['UEN','CODPRODUCTO','Despachos Proyectados Mes Actual'
                             ,'Volumen Promedio Mensual Proyectado','Despachos Mes Anterior'
                             ,'Volumen Promedio Mes Anterior','Variacion Intermensual %','Variacion Despachos Intermensual %'
                             ,'Despachos 2022','Volumen Promedio Año 2022'
                             ,'Variacion Interanual %', 'Variacion Despachos Interanual %'])
        
        # convertir la columna a un tipo de datos numérico que pueda manejar infinitos
    df['Variacion Intermensual %'] = pd.to_numeric(df['Variacion Intermensual %'], errors='coerce').astype('float64')

    # identificar y reemplazar valores infinitos
    df['Variacion Intermensual %'].replace(np.inf, 1, inplace=True)  
    
    # convertir la columna a un tipo de datos numérico que pueda manejar infinitos
    df['Variacion Despachos Intermensual %'] = pd.to_numeric(df['Variacion Despachos Intermensual %'], errors='coerce').astype('float64')

    # identificar y reemplazar valores infinitos
    df['Variacion Intermensual %'].replace(np.inf, 1, inplace=True)  
    
    # identificar y reemplazar valores infinitos
    df['Variacion Despachos Intermensual %'].replace(np.inf, 1, inplace=True)  
    
    # convertir la columna a un tipo de datos numérico que pueda manejar infinitos
    df['Variacion Interanual %'] = pd.to_numeric(df['Variacion Interanual %'], errors='coerce').astype('float64')

    # identificar y reemplazar valores infinitos
    df['Variacion Interanual %'].replace(np.inf, 1, inplace=True)
    
    # convertir la columna a un tipo de datos numérico que pueda manejar infinitos
    df['Variacion Despachos Interanual %'] = pd.to_numeric(df['Variacion Despachos Interanual %'], errors='coerce').astype('float64')
    
    # identificar y reemplazar valores infinitos
    df['Variacion Despachos Interanual %'].replace(np.inf, 1, inplace=True)
    
    
    
    return df

despachosGO=despachos.loc[(despachos['CODPRODUCTO'] == 'GO   ') & (despachos['Despachos Proyectados Mes Actual'] > 10) ,:]
despachosGO=totales(despachosGO)

despachosEU=despachos.loc[(despachos['CODPRODUCTO'] == 'EU   ') & (despachos['Despachos Proyectados Mes Actual'] > 10) ,:]
despachosEU=totales(despachosEU)

despachosNS=despachos.loc[(despachos['CODPRODUCTO'] == 'NS   ') & (despachos['Despachos Proyectados Mes Actual'] > 10) ,:]
despachosNS=totales(despachosNS)

despachosNU=despachos.loc[(despachos['CODPRODUCTO'] == 'NU   ') & (despachos['Despachos Proyectados Mes Actual'] > 10) ,:]
despachosNU=totales(despachosNU)

despachosGNC=despachos.loc[(despachos['CODPRODUCTO'] == 'GNC  ') & (despachos['Despachos Proyectados Mes Actual'] > 10) ,:]
despachosGNC=totales(despachosGNC)

def _estiladorVtaTitulo(df, list_Col_Porcentajes, titulo):
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
        .format("{:,.2%}", subset=list_Col_Porcentajes) \
        .hide(axis=0) \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio).strftime("%d/%m/%y"))
            + "<br>") \
        .set_properties(subset= list_Col_Porcentajes, **{"text-align": "center", "width": "100px"}) \
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
    
    subset_columns = pd.IndexSlice[evitarTotales[:-1], list_Col_Porcentajes]

    resultado= resultado.applymap(table_color,subset=subset_columns)

    return resultado

def table_color(val):
    """
    Takes a scalar and returns a string with
    the css property `'color: red'` for less than 60 marks, green otherwise.
    """
    color = 'blue' if val > 0 else 'red'
    return 'color: % s' % color


#### Defino columnas para cada Dataframe (Numericas)
#numCols = ['Despachos Proyectados Mes Actual','Despachos Mes Anterior'
#           ,'Despachos 2022']
#litrosCols=['Volumen Promedio Mensual Proyectado','Volumen Promedio Mes Anterior'
#           ,'Volumen Promedio Año 2022']
### COLUMNAS PARA INFORME PENETRACION

despachosGO= despachosGO.rename(columns={'Variacion Intermensual %': 'Variacion Volumen Intermensual %', 'Variacion Interanual %': 'Variacion Volumen Interanual %'})
despachosEU= despachosEU.rename(columns={'Variacion Intermensual %': 'Variacion Volumen Intermensual %', 'Variacion Interanual %': 'Variacion Volumen Interanual %'})
despachosNS= despachosNS.rename(columns={'Variacion Intermensual %': 'Variacion Volumen Intermensual %', 'Variacion Interanual %': 'Variacion Volumen Interanual %'})
despachosNU= despachosNU.rename(columns={'Variacion Intermensual %': 'Variacion Volumen Intermensual %', 'Variacion Interanual %': 'Variacion Volumen Interanual %'})
despachosGNC= despachosGNC.rename(columns={'Variacion Intermensual %': 'Variacion Volumen Intermensual %', 'Variacion Interanual %': 'Variacion Volumen Interanual %'})

percColsPorcentaje = ['Variacion Volumen Intermensual %','Variacion Volumen Interanual %', 'Variacion Despachos Intermensual %', 'Variacion Despachos Interanual %']

despachosGO= despachosGO.reindex(columns=['UEN',  'Variacion Volumen Intermensual %',  'Variacion Despachos Intermensual %','Variacion Volumen Interanual %','Variacion Despachos Interanual %'])
despachosEU= despachosEU.reindex(columns=['UEN',  'Variacion Volumen Intermensual %',  'Variacion Despachos Intermensual %','Variacion Volumen Interanual %','Variacion Despachos Interanual %'])
despachosNS= despachosNS.reindex(columns=['UEN',  'Variacion Volumen Intermensual %',  'Variacion Despachos Intermensual %','Variacion Volumen Interanual %','Variacion Despachos Interanual %'])
despachosNU= despachosNU.reindex(columns=['UEN',  'Variacion Volumen Intermensual %',  'Variacion Despachos Intermensual %','Variacion Volumen Interanual %','Variacion Despachos Interanual %'])
despachosGNC= despachosGNC.reindex(columns=['UEN','Variacion Volumen Intermensual %', 'Variacion Despachos Intermensual %','Variacion Volumen Interanual %','Variacion Despachos Interanual %'])



#### COLUMNAS INFORME EJECUCION PANADERIA PRESUPUESTADO DIARIO
#list_col_letras = ['CODPRODUCTO']
###### Aplico el formato elegido a la imagen


despachosGO = _estiladorVtaTitulo(despachosGO,percColsPorcentaje, "Evolucion Volumen Promedio por Despacho Ultra Diesel")
despachosEU = _estiladorVtaTitulo(despachosEU,percColsPorcentaje, "Evolucion Volumen Promedio por Despacho Infinia Diesel")
despachosNS = _estiladorVtaTitulo(despachosNS,percColsPorcentaje, "Evolucion Volumen Promedio por Despacho Nafta Super")
despachosNU = _estiladorVtaTitulo(despachosNU,percColsPorcentaje, "Evolucion Volumen Promedio por Despacho Infinia Nafta")
despachosGNC = _estiladorVtaTitulo(despachosGNC,percColsPorcentaje,"Evolucion Volumen Promedio por Despacho GNC")



###### DEFINO NOMBRE Y UBICACION DE DONDE SE VA A GUARDAR LA IMAGEN

ubicacion = "C:/Informes/Informe descargas y volumenes/"
nombreGO = 'VolumenPromGO.png'
nombreEU = 'VolumenPromEU.png'
nombreNS = 'VolumenPromNS.png'
nombreNU='VolumenPromNU.png'
nombreGNC='VolumenPromGNC.png'
# Creo una imagen en funcion al dataframe 
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

df_to_image(despachosGO, ubicacion, nombreGO)
df_to_image(despachosEU, ubicacion, nombreEU)
df_to_image(despachosNS, ubicacion, nombreNS)
df_to_image(despachosNU, ubicacion, nombreNU)
df_to_image(despachosGNC, ubicacion, nombreGNC)
#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)