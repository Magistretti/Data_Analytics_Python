###################################
#
#     INFORME Ventas Liq Proyectadas
#             
#       18/11/21 - 
###################################

import os
import sys
import pathlib

# Allow imports from the top folder
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))

import pandas as pd
import dataframe_image as dfi
from PIL import Image

from DatosLogin import login
from Conectores import conectorMSSQL

import logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)

conexMSSQL = conectorMSSQL(login)

##########################################
# Get sales of liquid fuel
##########################################

df_vtas_Liq = pd.read_sql(
    """
    DECLARE @inicioMesActual DATETIME
    SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, CURRENT_TIMESTAMP), 0)

    DECLARE @inicioMesAnterior DATETIME
    SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

    --Divide por la cant de días del mes anterior y multiplica por la cant de días del
    --mes actual
    DECLARE @pondMesAnt decimal(18,8)--Evitar cambio de config regional por float
    SET @pondMesAnt = CAST(DAY(EOMONTH(CURRENT_TIMESTAMP)) AS float) /
        CAST(DAY(EOMONTH(@inicioMesAnterior)) AS float)

    DECLARE @hoy DATETIME
    SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

    --Divide por la cantidad de días cursados del mes actual y multiplica por la cant
    --de días del mes actual
    DECLARE @pondMesAct decimal(18,8)--Evitar cambio de config regional por float
    SET @pondMesAct = CAST(DAY(EOMONTH(CURRENT_TIMESTAMP)) AS float) /
        (CAST(DAY(CURRENT_TIMESTAMP) AS float)-1)

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
    , conexMSSQL
)

print(df_vtas_Liq.info())
print(df_vtas_Liq)