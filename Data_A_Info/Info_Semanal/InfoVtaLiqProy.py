###################################
#
#     INFORME Ventas Liq Proyectadas
#             
#       18/11/21 - 
###################################

import os
import sys
import pathlib
import pandas as pd
import dataframe_image as dfi
import numpy as np
import logging
import os
import numpy as np
import pandas as pd
import pyodbc #Library to connect to Microsoft SQL Server
import sys
import pathlib
from DatosLogin import login
import logging
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
# Formating display of dataframes with comma separator for numbers
pd.options.display.float_format = "{:20,.0f}".format 
#########

tiempoInicio = pd.to_datetime("today")
#########
# Formating display of dataframes with comma separator for numbers
pd.options.display.float_format = "{:20,.0f}".format 
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
# Get sales of liquid fuel and GNC fuel
##########################################

    ##########################################
    # Liquid Fuel Sales
    ##########################################  
      
df_vtas_liq = pd.read_sql(
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

    DECLARE @CantidadDiasMesAnterior INT = DAY(DATEADD(DAY, -1,DATEADD(DAY, 1 - DAY(getdate()), getdate())))
    Declare @pondMesAnt Float
    SET @pondMesAnt =  CAST(DAY(EOMONTH(getdate()-1)) AS float) /@CantidadDiasMesAnterior

    --Divide por la cantidad de días cursados del mes actual y multiplica por la cant
    --de días del mes actual
     Declare @pondMesAct Float
    SET @pondMesAct = CAST(DAY(EOMONTH(getdate()-1)) AS float) /
    (CAST(DAY(getdate()-1) AS float))

    SELECT  
            RTRIM(T1.[UEN]) AS 'UEN'
            ,RTRIM(T1.[CODPRODUCTO]) AS 'CODPRODUCTO'
            , T2.[Volumen Acumulado]
            ,sum([VTATOTVOL] * @pondMesAnt) AS 'Mes Anterior Vol Proyectado'
            , T2.[Mes Actual Vol Proyectado]
        FROM [Rumaos].[dbo].[EmpVenta] as T1
        Full Outer JOIN (SELECT
            SQ.UEN
            , SQ.CODPRODUCTO
            ,sum(SQ.[VTATOTVOL]) AS 'Volumen Acumulado'
            ,sum(SQ.[VTATOTVOL] * @pondMesAct) AS 'Mes Actual Vol Proyectado'
        FROM [Rumaos].[dbo].[EmpVenta] as SQ
        WHERE SQ.FECHASQL >= @inicioMesActual
            AND SQ.FECHASQL < @hoy
            AND SQ.VTATOTVOL > '0'
            AND SQ.CODPRODUCTO <> 'GNC'
            group by SQ.UEN, SQ.CODPRODUCTO
            ) AS T2
            ON T1.UEN = T2.UEN AND T1.CODPRODUCTO = T2.CODPRODUCTO
        WHERE FECHASQL >= @inicioMesAnterior
            AND FECHASQL < @inicioMesActual
            AND T1.VTATOTVOL > '0'
            AND T1.CODPRODUCTO <> 'GNC'
            group by T1.UEN
                , T1.CODPRODUCTO
                , T2.[Volumen Acumulado]
                , T2.[Mes Actual Vol Proyectado]
            order by T1.UEN, T1.CODPRODUCTO
    """
    , db_conex
)
df_vtas_liq = df_vtas_liq.convert_dtypes()


##########################################
# Gift of Liquid Fuel
##########################################

df_regalo_liq = pd.read_sql(
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

    DECLARE @CantidadDiasMesAnterior INT = DAY(DATEADD(DAY, -1,DATEADD(DAY, 1 - DAY(getdate()), getdate())))
    Declare @pondMesAnt Float
    SET @pondMesAnt =  CAST(DAY(EOMONTH(getdate()-1)) AS float) /@CantidadDiasMesAnterior

    --Divide por la cantidad de días cursados del mes actual y multiplica por la cant
    --de días del mes actual
     Declare @pondMesAct Float
    SET @pondMesAct = CAST(DAY(EOMONTH(getdate()-1)) AS float) /
    (CAST(DAY(getdate()-1) AS float))

    SELECT
        RTRIM(EmP.[UEN]) AS UEN
        ,RTRIM(EmP.[CODPRODUCTO]) AS CODPRODUCTO
        ,T2.[Volumen Acumulado]
        ,sum(-EmP.[VOLUMEN] * @pondMesAnt) AS 'Mes Anterior Vol Proyectado'
        ,T2.[Mes Actual Vol Proyectado]
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    FULL OUTER JOIN (SELECT
            EmP.[UEN]
            ,EmP.[CODPRODUCTO]
            ,sum(-EmP.[VOLUMEN]) AS 'Volumen Acumulado'
            ,sum(-EmP.[VOLUMEN] * @pondMesAct) AS 'Mes Actual Vol Proyectado'
        FROM [Rumaos].[dbo].[EmpPromo] AS EmP
            INNER JOIN Promocio AS P 
                ON EmP.UEN = P.UEN 
                AND EmP.CODPROMO = P.CODPROMO
        WHERE FECHASQL >= @inicioMesActual
            AND FECHASQL < @hoy
            AND EmP.VOLUMEN > '0'
            AND EmP.CODPRODUCTO <> 'GNC'
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
            AND EmP.CODPRODUCTO <> 'GNC'
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
df_regalo_liq.fillna(0, inplace=True)
df_regalo_liq = df_regalo_liq.convert_dtypes()


##########################################
# Liquid Fuel Sales last year
##########################################

df_vtas_liqAnual = pd.read_sql(
    """

        -- Variables de fecha para inicio y fin de mes del año pasado
    DECLARE @inicio_mes_Añopasado DATE, @fin_mes_Añopasado DATE

    -- Obtener el primer día del mes actual del año pasado
    SET @inicio_mes_Añopasado = DATEADD(YEAR, -1, DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()-1), 0))

    -- Obtener el último día del mes actual del año pasado
    SET @fin_mes_Añopasado = DATEADD(YEAR, -1, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE()-1), 0))

    SELECT  
            RTRIM([UEN]) AS 'UEN'
            ,RTRIM([CODPRODUCTO]) AS 'CODPRODUCTO'
            ,sum([VTATOTVOL]) AS 'Año Anterior Vol Proyectado'
        FROM [Rumaos].[dbo].[EmpVenta]
        WHERE FECHASQL >= @inicio_mes_Añopasado
            AND FECHASQL < @fin_mes_Añopasado
            AND VTATOTVOL > '0'
            AND CODPRODUCTO <> 'GNC'
            group by UEN
                , CODPRODUCTO
            order by UEN, CODPRODUCTO
    """
    , db_conex
)
df_vtas_liqAnual = df_vtas_liqAnual.convert_dtypes()

##########################################
# Gift of Liquid Fuel
##########################################

df_regalo_liqAnual = pd.read_sql(
    """
        -- Variables de fecha para inicio y fin de mes del año pasado
    DECLARE @inicio_mes_Añopasado DATE, @fin_mes_Añopasado DATE

    -- Obtener el primer día del mes actual del año pasado
    SET @inicio_mes_Añopasado = DATEADD(YEAR, -1, DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()-1), 0))

    -- Obtener el último día del mes actual del año pasado
    SET @fin_mes_Añopasado = DATEADD(YEAR, -1, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE()-1), 0))

    SELECT  
        RTRIM(EmP.[UEN]) AS UEN
        ,RTRIM(EmP.[CODPRODUCTO]) AS CODPRODUCTO
        ,sum(-EmP.[VOLUMEN]) AS 'Año Anterior Vol Proyectado'
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
        WHERE FECHASQL >= @inicio_mes_Añopasado
            AND FECHASQL < @fin_mes_Añopasado
            AND VOLUMEN > '0'
            AND emp.CODPRODUCTO <> 'GNC'
            AND (EmP.CODPROMO = '30'
                OR [DESCRIPCION] like '%PRUEBA%'
                OR [DESCRIPCION] like '%TRASLADO%'
                OR [DESCRIPCION] like '%MAYORISTA%')
            group by EmP.UEN
                , EmP.CODPRODUCTO
            order by UEN, CODPRODUCTO
    """
    , db_conex
)
df_regalo_liqAnual.fillna(0, inplace=True)
df_regalo_liqAnual = df_regalo_liqAnual.convert_dtypes()
df_vtas_liqAnual = df_vtas_liqAnual.merge(df_regalo_liqAnual,on=['UEN','CODPRODUCTO','Año Anterior Vol Proyectado'],how='outer')
df_vtas_liqAnual = df_vtas_liqAnual.groupby(
    ["UEN",'CODPRODUCTO']
    , as_index=False
).sum()

df_vtas_liq=df_vtas_liq.merge(df_vtas_liqAnual,on=['UEN','CODPRODUCTO'],how='outer')





##########################################
# GNC Fuel Sales
##########################################

df_vtas_GNC = pd.read_sql(
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

    DECLARE @CantidadDiasMesAnterior INT = DAY(DATEADD(DAY, -1,DATEADD(DAY, 1 - DAY(getdate()), getdate())))
    Declare @pondMesAnt Float
    SET @pondMesAnt =  CAST(DAY(EOMONTH(getdate()-1)) AS float) /@CantidadDiasMesAnterior

    --Divide por la cantidad de días cursados del mes actual y multiplica por la cant
    --de días del mes actual
     Declare @pondMesAct Float
    SET @pondMesAct = CAST(DAY(EOMONTH(getdate()-1)) AS float) /
    (CAST(DAY(getdate()-1) AS float))

    SELECT  
            RTRIM(T1.[UEN]) AS 'UEN'
            ,RTRIM(T1.[CODPRODUCTO]) AS 'CODPRODUCTO'
            , T2.[Volumen Acumulado]
            ,sum([VTATOTVOL] * @pondMesAnt) AS 'Mes Anterior Vol Proyectado'
            , T2.[Mes Actual Vol Proyectado]
        FROM [Rumaos].[dbo].[EmpVenta] as T1
        Full Outer JOIN (SELECT
            SQ.UEN
            , SQ.CODPRODUCTO
            ,sum(SQ.[VTATOTVOL]) AS 'Volumen Acumulado'
            ,sum(SQ.[VTATOTVOL] * @pondMesAct) AS 'Mes Actual Vol Proyectado'
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
    """
    , db_conex
)
df_vtas_GNC = df_vtas_GNC.convert_dtypes()


##########################################
# Gift of GNC Fuel
##########################################

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

    DECLARE @CantidadDiasMesAnterior INT = DAY(DATEADD(DAY, -1,DATEADD(DAY, 1 - DAY(getdate()), getdate())))
    Declare @pondMesAnt Float
    SET @pondMesAnt =  CAST(DAY(EOMONTH(getdate()-1)) AS float) /@CantidadDiasMesAnterior

    --Divide por la cantidad de días cursados del mes actual y multiplica por la cant
    --de días del mes actual
     Declare @pondMesAct Float
    SET @pondMesAct = CAST(DAY(EOMONTH(getdate()-1)) AS float) /
    (CAST(DAY(getdate()-1) AS float))

    SELECT
        RTRIM(EmP.[UEN]) AS UEN
        ,RTRIM(EmP.[CODPRODUCTO]) AS CODPRODUCTO
        ,T2.[Volumen Acumulado]
        ,sum(-EmP.[VOLUMEN] * @pondMesAnt) AS 'Mes Anterior Vol Proyectado'
        ,T2.[Mes Actual Vol Proyectado]
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    FULL OUTER JOIN (SELECT
            EmP.[UEN]
            ,EmP.[CODPRODUCTO]
            ,sum(-EmP.[VOLUMEN]) AS 'Volumen Acumulado'
            ,sum(-EmP.[VOLUMEN] * @pondMesAct) AS 'Mes Actual Vol Proyectado'
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
df_regalo_GNC.fillna(0, inplace=True)
df_regalo_GNC = df_regalo_GNC.convert_dtypes()



##########################################
# Liquid Fuel Sales last year
##########################################

df_vtasGNC_liqAnual = pd.read_sql(
    """

        -- Variables de fecha para inicio y fin de mes del año pasado
    DECLARE @inicio_mes_Añopasado DATE, @fin_mes_Añopasado DATE

    -- Obtener el primer día del mes actual del año pasado
    SET @inicio_mes_Añopasado = DATEADD(YEAR, -1, DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()-1), 0))

    -- Obtener el último día del mes actual del año pasado
    SET @fin_mes_Añopasado = DATEADD(YEAR, -1, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE()-1),0))

    SELECT  
            RTRIM([UEN]) AS 'UEN'
            ,RTRIM([CODPRODUCTO]) AS 'CODPRODUCTO'
            ,sum([VTATOTVOL]) AS 'Año Anterior Vol Proyectado'
        FROM [Rumaos].[dbo].[EmpVenta]
        WHERE FECHASQL >= @inicio_mes_Añopasado
            AND FECHASQL < @fin_mes_Añopasado
            AND VTATOTVOL > '0'
            AND CODPRODUCTO = 'GNC'
            group by UEN
                , CODPRODUCTO
            order by UEN, CODPRODUCTO
    """
    , db_conex
)
df_vtasGNC_liqAnual = df_vtasGNC_liqAnual.convert_dtypes()

##########################################
# Gift of Liquid Fuel
##########################################

df_regaloGNC_liqAnual = pd.read_sql(
    """
        -- Variables de fecha para inicio y fin de mes del año pasado
    DECLARE @inicio_mes_Añopasado DATE, @fin_mes_Añopasado DATE

    -- Obtener el primer día del mes actual del año pasado
    SET @inicio_mes_Añopasado = DATEADD(YEAR, -1, DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()-1), 0))

    -- Obtener el último día del mes actual del año pasado
    SET @fin_mes_Añopasado = DATEADD(YEAR, -1, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE()-1), 0))

    SELECT  
        RTRIM(EmP.[UEN]) AS UEN
        ,RTRIM(EmP.[CODPRODUCTO]) AS CODPRODUCTO
        ,sum(-EmP.[VOLUMEN]) AS 'Año Anterior Vol Proyectado'
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
        WHERE FECHASQL >= @inicio_mes_Añopasado
            AND FECHASQL < @fin_mes_Añopasado
            AND VOLUMEN > '0'
            AND emp.CODPRODUCTO = 'GNC'
            AND (EmP.CODPROMO = '30'
                OR [DESCRIPCION] like '%PRUEBA%'
                OR [DESCRIPCION] like '%TRASLADO%'
                OR [DESCRIPCION] like '%MAYORISTA%')
            group by EmP.UEN
                , EmP.CODPRODUCTO
            order by UEN, CODPRODUCTO
    """
    , db_conex
)
df_regaloGNC_liqAnual.fillna(0, inplace=True)
df_regaloGNC_liqAnual = df_regaloGNC_liqAnual.convert_dtypes()
df_vtasGNC_liqAnual = df_vtasGNC_liqAnual.merge(df_regaloGNC_liqAnual,on=['UEN','CODPRODUCTO','Año Anterior Vol Proyectado'],how='outer')
df_vtasGNC_liqAnual = df_vtasGNC_liqAnual.groupby(
    ["UEN",'CODPRODUCTO']
    , as_index=False
).sum()


df_vtas_GNC=df_vtas_GNC.merge(df_vtasGNC_liqAnual,on=['UEN','CODPRODUCTO'],how='outer')







##########################################
# Subtract gifts from sales of fuel
##########################################

# Liquid Fuel
df_vtas_liq_neto = df_vtas_liq.set_index(["UEN","CODPRODUCTO"])
df_vtas_liq_neto = df_vtas_liq_neto.add(
    df_regalo_liq.set_index(["UEN","CODPRODUCTO"])
    , fill_value=0
)
df_vtas_liq_neto = df_vtas_liq_neto.reset_index()

# GNC Fuel
df_vtas_GNC_neto = df_vtas_GNC.set_index(["UEN","CODPRODUCTO"])
df_vtas_GNC_neto = df_vtas_GNC_neto.add(
    df_regalo_GNC.set_index(["UEN","CODPRODUCTO"])
    , fill_value=0
)
df_vtas_GNC_neto = df_vtas_GNC_neto.reset_index()



##########################################
# Create column "GRUPO" and "BANDERA"
##########################################

# GRUPO
def _grupo(codproducto):
    if codproducto == "GO":
        return "GASÓLEOS GO"
    elif codproducto == "EU":
        return "GASÓLEOS EU"
    elif codproducto == "NS":
        return "NAFTAS NS"
    elif codproducto == "NU":
        return "NAFTAS NU"
    else:
        return "GNC"


# Create column "GRUPO" from column CODPRODUCTO
# Liquid Fuel
df_vtas_liq_neto["GRUPO"] = df_vtas_liq_neto.apply(
    lambda row: _grupo(row["CODPRODUCTO"])
        , axis= 1
)
# Grouping by "UEN" and "GRUPO"
df_vtas_liq_neto = df_vtas_liq_neto.groupby(
    ["UEN","GRUPO"]
    , as_index=False
).sum()

# GNC Fuel
df_vtas_GNC_neto["GRUPO"] = df_vtas_GNC_neto.apply(
    lambda row: _grupo(row["CODPRODUCTO"])
        , axis= 1
)
# Grouping by "UEN" and "GRUPO"
df_vtas_GNC_neto = df_vtas_GNC_neto.groupby(
    ["UEN","GRUPO"]
    , as_index=False
).sum()


# BANDERA
def _bandera(uen):
    if uen in [
        "AZCUENAGA"
        , "LAMADRID"
        , "PERDRIEL"
        , "PERDRIEL2"
        , "PUENTE OLIVE"
        , "SAN JOSE"
    ]:
        return "YPF"
    else:
        return "DAPSA"

# Create column BANDERA from column UEN
# Liquid Fuel
df_vtas_liq_neto["BANDERA"] = df_vtas_liq_neto.apply(
    lambda row: _bandera(row["UEN"])
        , axis= 1
)

# GNC Fuel
df_vtas_GNC_neto["BANDERA"] = df_vtas_GNC_neto.apply(
    lambda row: _bandera(row["UEN"])
        , axis= 1
)



##########################################
# Further transform the DFs to add subtotals, totals, and sort 
# each "BANDERA" subgroup by "Intermensual %"
##########################################

def _preparador(df):
    
    # Prepping df disregarding "GRUPO"
    df = df.drop(columns="GRUPO")
    
    df=df.groupby(["UEN","BANDERA"], as_index=False).sum()

    # Separate by "BANDERA" and dropping column "BANDERA"

    df_ypf = df[df["BANDERA"] == "YPF"].drop(columns="BANDERA")
    df_dapsa = df[df["BANDERA"] == "DAPSA"].drop(columns="BANDERA")


    # Get row of totals and column "Intermensual %"
    # GRAND TOTAL
    df_tot = df[[
        "Volumen Acumulado"
        , "Mes Anterior Vol Proyectado"
        , "Mes Actual Vol Proyectado"
        , 'Año Anterior Vol Proyectado'
    ]].sum()
    df_tot["UEN"] = "TOTAL"
    df_tot["Intermensual %"] = (
        df_tot["Mes Actual Vol Proyectado"] 
        / df_tot["Mes Anterior Vol Proyectado"]
        - 1
    )
    df_tot["InterAnual %"] = (
        df_tot["Mes Actual Vol Proyectado"] 
        / df_tot['Año Anterior Vol Proyectado']
        - 1
    )    
    # YPF subtotal
    df_tot_ypf = df_ypf[[
        "Volumen Acumulado"
        , "Mes Anterior Vol Proyectado"
        , "Mes Actual Vol Proyectado"
        , 'Año Anterior Vol Proyectado'
    ]].sum()
    df_tot_ypf["UEN"] = "YPF"
    df_tot_ypf["Intermensual %"] = (
        df_tot_ypf["Mes Actual Vol Proyectado"] 
        / df_tot_ypf["Mes Anterior Vol Proyectado"]
        - 1
    )
    df_tot_ypf["InterAnual %"] = (
        df_tot_ypf["Mes Actual Vol Proyectado"] 
        / df_tot_ypf['Año Anterior Vol Proyectado']
        - 1
    )    
    # DAPSA subtotal
    df_tot_dapsa = df_dapsa[[
        "Volumen Acumulado"
        , "Mes Anterior Vol Proyectado"
        , "Mes Actual Vol Proyectado"
        ,'Año Anterior Vol Proyectado'
    ]].sum()
    df_tot_dapsa["UEN"] = "DAPSA"
    df_tot_dapsa["Intermensual %"] = (
        df_tot_dapsa["Mes Actual Vol Proyectado"] 
        / df_tot_dapsa["Mes Anterior Vol Proyectado"]
        - 1
    )
    df_tot_dapsa["InterAnual %"] = np.where(df_tot_dapsa['Año Anterior Vol Proyectado'] == 0, np.nan,(df_tot_dapsa["Mes Actual Vol Proyectado"] / df_tot_dapsa['Año Anterior Vol Proyectado']) - 1)        
    # Add column "Intermensual %" to df_ypf and df_dapsa and sort
    # YPF
    df_ypf["Intermensual %"] = (
        df_ypf["Mes Actual Vol Proyectado"] 
        / df_ypf["Mes Anterior Vol Proyectado"]
        - 1
    )
    df_ypf.sort_values("Intermensual %", inplace=True)
    try:
        df_ypf["InterAnual %"] = (
            df_ypf["Mes Actual Vol Proyectado"] 
            / df_ypf['Año Anterior Vol Proyectado']
            - 1
        )
        df_ypf.sort_values("InterAnual %", inplace=True)    
    except:
        df_ypf["InterAnual %"]
    # DAPSA
    df_dapsa["Intermensual %"] = (
        df_dapsa["Mes Actual Vol Proyectado"] 
        / df_dapsa["Mes Anterior Vol Proyectado"]
        - 1
    )
    df_dapsa.sort_values("Intermensual %", inplace=True)
    df_dapsa["InterAnual %"] = (
        df_dapsa["Mes Actual Vol Proyectado"] 
        / df_dapsa['Año Anterior Vol Proyectado']
        - 1
    )
    df_dapsa.sort_values("InterAnual %", inplace=True)       
    # Transform df_tot_ypf series to dataframe and transpose it
    df_completo = df_tot_ypf.to_frame().transpose()
    # Reorder columns
    df_completo = df_completo[[
        "UEN"
        , "Volumen Acumulado"
        , "Mes Anterior Vol Proyectado"
        , "Mes Actual Vol Proyectado"
        ,'Año Anterior Vol Proyectado'
        , "Intermensual %"
        ,"InterAnual %"
    ]]
    
    # Append all the series and dataframes to get rows in order and dont 
    # mess up the the sorting
    df_completo = df_completo.append(df_ypf, ignore_index=True)
    df_completo = df_completo.append(df_tot_dapsa, ignore_index=True)
    df_completo = df_completo.append(df_dapsa, ignore_index=True)
    df_completo = df_completo.append(df_tot, ignore_index=True)

    # Create row "Intermensual Volumen"
    df_completo["Intermensual Volumen"] = (
        df_completo["Mes Actual Vol Proyectado"]
        - df_completo["Mes Anterior Vol Proyectado"]
    )
    df_completo["InterAnual Volumen"] = (
        df_completo["Mes Actual Vol Proyectado"]
        - df_completo["Año Anterior Vol Proyectado"]
    )
    df_completo = df_completo.reindex(columns=["UEN"
        , "Volumen Acumulado"
        , "Mes Actual Vol Proyectado"
        , "Mes Anterior Vol Proyectado"
        ,"Intermensual %"
        ,'Intermensual Volumen'
        ,'Año Anterior Vol Proyectado'
        ,"InterAnual %"
        ,'InterAnual Volumen'])
    return df_completo

########### PREPARADOR INFINIAS ###########
def _preparadorINF(df):
    
    # Prepping df disregarding "GRUPO"
    df = df.drop(columns="GRUPO")
    
    df=df.groupby(["UEN","BANDERA"], as_index=False).sum()
    df = df.drop(df[df['Volumen Acumulado'] == 0].index)
    df = df[df['Volumen Acumulado'] >= 10]
    # Separate by "BANDERA" and dropping column "BANDERA"

    df_ypf = df


    # Get row of totals and column "Intermensual %"
    # GRAND TOTAL
    df_tot = df[[
        "Volumen Acumulado"
        , "Mes Anterior Vol Proyectado"
        , "Mes Actual Vol Proyectado"
        , 'Año Anterior Vol Proyectado'
    ]].sum()
    df_tot["UEN"] = "TOTAL"
    df_tot["Intermensual %"] = (
        df_tot["Mes Actual Vol Proyectado"] 
        / df_tot["Mes Anterior Vol Proyectado"]
        - 1
    )
    df_tot["InterAnual %"] = (
        df_tot["Mes Actual Vol Proyectado"] 
        / df_tot['Año Anterior Vol Proyectado']
        - 1
    )    
    # YPF subtotal
    df_tot_ypf = df_ypf[[
        "Volumen Acumulado"
        , "Mes Anterior Vol Proyectado"
        , "Mes Actual Vol Proyectado"
        , 'Año Anterior Vol Proyectado'
    ]].sum()
    df_tot_ypf["UEN"] = "YPF"
    df_tot_ypf["Intermensual %"] = (
        df_tot_ypf["Mes Actual Vol Proyectado"] 
        / df_tot_ypf["Mes Anterior Vol Proyectado"]
        - 1
    )
    df_tot_ypf["InterAnual %"] = (
        df_tot_ypf["Mes Actual Vol Proyectado"] 
        / df_tot_ypf['Año Anterior Vol Proyectado']
        - 1
    )    
    # Add column "Intermensual %" to df_ypf and df_dapsa and sort
    # YPF
    df_ypf["Intermensual %"] = (
        df_ypf["Mes Actual Vol Proyectado"] 
        / df_ypf["Mes Anterior Vol Proyectado"]
        - 1
    )
    df_ypf.sort_values("Intermensual %", inplace=True)
    df_ypf["InterAnual %"] = (
        df_ypf["Mes Actual Vol Proyectado"] 
        / df_ypf['Año Anterior Vol Proyectado']
        - 1
    )
    df_ypf.sort_values("InterAnual %", inplace=True)    
    df_ypf["InterAnual %"]

    # Transform df_tot_ypf series to dataframe and transpose it
    df_completo = df_tot_ypf.to_frame().transpose()
    # Reorder columns
    df_completo = df_completo[[
        "UEN"
        , "Volumen Acumulado"
        , "Mes Anterior Vol Proyectado"
        , "Mes Actual Vol Proyectado"
        ,"Intermensual %"
        ,'Año Anterior Vol Proyectado'
        ,"InterAnual %"
    ]]
    
    # Append all the series and dataframes to get rows in order and dont 
    # mess up the the sorting
    df_completo = df_completo.append(df_ypf, ignore_index=True)
    df_completo = df_completo.append(df_tot, ignore_index=True)

    # Create row "Intermensual Volumen"
    df_completo["Intermensual Volumen"] = (
        df_completo["Mes Actual Vol Proyectado"]
        - df_completo["Mes Anterior Vol Proyectado"]
    )
    df_completo["InterAnual Volumen"] = (
        df_completo["Mes Actual Vol Proyectado"]
        - df_completo["Año Anterior Vol Proyectado"]
    )
    
    df_completo = df_completo.drop(df_completo[df_completo.UEN == 'YPF'].index)
    df_completo = df_completo.drop(columns="BANDERA")
    df_completo = df_completo.reindex(columns=["UEN"
        , "Volumen Acumulado"
        , "Mes Actual Vol Proyectado"
        , "Mes Anterior Vol Proyectado"
        ,"Intermensual %"
        ,'Intermensual Volumen'
        ,'Año Anterior Vol Proyectado'
        ,"InterAnual %"
        ,'InterAnual Volumen'])
    return df_completo
##########################################
# STYLING of the dataframe
##########################################


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
            + "Mes Actual Proyectado "
            + pd.to_datetime("today").strftime("%Y-%M")
            ) \
        .set_properties(subset=list_Col_Num + list_Col_Perc
            , **{"text-align": "center", "width": "120px"}) \
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
                    ,("font-size", "14px")
                ]
            }
        ]) \
        .apply(lambda x: ["background: black" if x.name in [
            df.index[-1]
            , df.index[0]
            , df.index[7]
        ]
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name in [
            df.index[-1]
            , df.index[0]
            , df.index[7]
        ]
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["font-size: 15px" if x.name in [
            df.index[-1]
            , df.index[0]
            , df.index[7]
        ]
            else "" for i in x]
            , axis=1) 

    
    
    return resultado

def _estiladorVtaTituloINFINIA(df, list_Col_Num, list_Col_Perc, titulo):
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
            + "Mes Actual Proyectado "
            + pd.to_datetime("today").strftime("%Y-%M")
            ) \
        .set_properties(subset=list_Col_Num + list_Col_Perc
            , **{"text-align": "center", "width": "120px"}) \
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
                    ,("font-size", "14px")
                ]
            }
        ]) \
        .apply(lambda x: ["background: black" if x.name in [
            df.index[-1]
        ]
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name in [
            df.index[-1]
        ]
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["font-size: 15px" if x.name in [
            df.index[-1]
        ]
            else "" for i in x]
            , axis=1) 

    
    
    return resultado

##########################################
# PRINTING dataframe as an image
##########################################

# This will print the df with a unique name and will erase the old image 
# everytime the script is run

def _df_to_image(df, ubicacion, nombre):
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
        dfi.export(df, ubicacion+nombre, max_rows=-1)
    else:
        dfi.export(df, ubicacion+nombre, max_rows=-1)


##########################################
# FUNCTION TO RUN MODULE
##########################################

    '''
    Create several image ".png" of the actual and forecasted sales of
    liquid fuel and GNC fuel:
    "Info_VtaLiquido_Semanal.png"
    "Info_GrupoLiq_Semanal.png"
    "Info_VtaGNC_Semanal.png"
    '''


df_liq=df_vtas_liq_neto
df_GNC=df_vtas_GNC_neto

# Timer
tiempoInicio = pd.to_datetime("today")

gasoleos = ((df_liq["GRUPO"] == "GASÓLEOS GO") | (df_liq["GRUPO"] == "GASÓLEOS EU"))
naftas = ((df_liq["GRUPO"] == "NAFTAS NS") | (df_liq["GRUPO"] == "NAFTAS NU"))
nuNafta = ((df_liq["GRUPO"] == "NAFTAS NU") & (df_liq["BANDERA"] == "YPF"))
df_liq_GASOLEOS = df_liq.loc[gasoleos,:]
df_liq_NAFTAS = df_liq.loc[naftas]
df_liq_GO = df_liq[df_liq["GRUPO"] == "GASÓLEOS GO"]
df_liq_EU = df_liq[df_liq["GRUPO"] == "GASÓLEOS EU"]
df_liq_NS = df_liq[df_liq["GRUPO"] == "NAFTAS NS"]
df_liq_NU = df_liq[nuNafta]

df_liq_listo = _preparador(df_liq)
df_GASOLEOS_listo = _preparador(df_liq_GASOLEOS)
df_NAFTAS_listo = _preparador(df_liq_NAFTAS)
df_GOs_listo = _preparador(df_liq_GO)
df_EUs_listo = _preparadorINF(df_liq_EU)
df_NSs_listo = _preparador(df_liq_NS)
df_NUs_listo = _preparadorINF(df_liq_NU)
df_GNC_listo = _preparador(df_GNC)




numCols = ["Volumen Acumulado"
    , "Mes Actual Vol Proyectado"
    , "Mes Anterior Vol Proyectado"
    , "Intermensual Volumen"
    , 'Año Anterior Vol Proyectado'
    ,'InterAnual Volumen'
]

percCols = ["Intermensual %","InterAnual %"]


df_liq_Estilo = _estiladorVtaTitulo(
    df_liq_listo
    , numCols
    , percCols
    , "VENTA DE LÍQUIDOS"
)
df_GASOLEOS_Estilo = _estiladorVtaTitulo(
    df_GASOLEOS_listo
    , numCols
    , percCols
    , "VENTA DE GASOLEOS"
)
df_NAFTAS_Estilo = _estiladorVtaTitulo(
    df_NAFTAS_listo
    , numCols
    , percCols
    , "VENTA DE NAFTAS"
)
df_GOs_Estilo = _estiladorVtaTitulo(
    df_GOs_listo
    , numCols
    , percCols
    , "VENTA DE ULTRA DIESEL"
)
df_EUs_Estilo = _estiladorVtaTituloINFINIA(
    df_EUs_listo
    , numCols
    , percCols
    , "VENTA DE INFINIA DIESEL"
)
df_NSs_Estilo = _estiladorVtaTitulo(
    df_NSs_listo
    , numCols
    , percCols
    , "VENTA DE NAFTA SUPER"
)
df_NUs_Estilo = _estiladorVtaTituloINFINIA(
    df_NUs_listo
    , numCols
    , percCols
    , "VENTA DE INFINIA NAFTA"
)
df_GNC_Estilo = _estiladorVtaTitulo(
    df_GNC_listo
    , numCols
    , percCols
    , "VENTA DE GNC"
)

# display(df_liq_Estilo)
# display(df_GOs_Estilo)
# display(df_NSs_Estilo)
# display(df_GNC_Estilo)


# Path and name for DF images
ubicacion = str(pathlib.Path(__file__).parent)+"\\"
IMG_liq = "Info_VtaLiquido_Semanal.png"
IMG_GASOLEOS = "VtaGASOLEOS_Semanal.png"
IMG_NAFTAS = "VtaNAFTAS_Semanal.png"
IMG_GOs = "VtaGOs_Semanal.png"
IMG_EUs = "VtaEUs_Semanal.png"
IMG_NSs = "VtaNSs_Semanal.png"
IMG_NUs = "VtaNUs_Semanal.png"
IMG_GNC = "Info_VtaGNC_Semanal.png"

_df_to_image(df_liq_Estilo, ubicacion, IMG_liq)

_df_to_image(df_GASOLEOS_Estilo, ubicacion, IMG_GASOLEOS)
_df_to_image(df_NAFTAS_Estilo, ubicacion, IMG_NAFTAS)

_df_to_image(df_GOs_Estilo, ubicacion, IMG_GOs)
_df_to_image(df_EUs_Estilo, ubicacion, IMG_EUs)
_df_to_image(df_NSs_Estilo, ubicacion, IMG_NSs)
_df_to_image(df_NUs_Estilo, ubicacion, IMG_NUs)
_df_to_image(df_GNC_Estilo, ubicacion, IMG_GNC)


# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Venta Líquido Proyectada"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)