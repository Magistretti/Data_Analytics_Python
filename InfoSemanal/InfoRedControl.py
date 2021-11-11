###################################
#
#     INFORME Red Control Semanal
#             
#               11/11/21
###################################

import os
import sys
import pathlib
from numpy import clongdouble

# Allow imports from the top folder
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))

import pandas as pd
import dataframe_image as dfi
from PIL import Image

from DatosLogin import login
from Conectores import conectorMSSQL

conexMSSQL = conectorMSSQL(login)

##########################################
# Aux tables with filters
##########################################

ubicacion = str(pathlib.Path(__file__).parent)+"\\"
aux_semanal = "Auxiliar_Info_Semanal.xlsx"

df_codprod_ok = pd.read_excel(
    ubicacion + aux_semanal
    , "cod_seleccionados"
)
df_nrocli_omit = pd.read_excel(
    ubicacion + aux_semanal
    , "clientes_omitidos"
)
df_codprod_ok = df_codprod_ok.convert_dtypes()


##########################################
# Function to help filter dataframes
##########################################

def _filtrador(df, tipo_cuenta="CC"):
    '''
    df: Dataframe to be filtered
    tipo_cuenta: "CC" or "CA" (default "CC")
    
    Return: Dataframe
    '''
    # Inner merge to select only valid "CODPRODUCTO" items and dropping
    # column "CODPRODUCTO"
    df = pd.merge(
        df
        , df_codprod_ok
        , on="CODPRODUCTO"
        , how="inner"
    ).drop(columns="CODPRODUCTO")

    # Left merge with indicator to be able to filter "NROCLIENTE"
    # on both tables and dropping column "NROCLIENTE"
    df = pd.merge(
        df
        , df_nrocli_omit
        , on="NROCLIENTE"
        , how="left"
        , indicator=True
    ).drop(columns="NROCLIENTE")

    # Selecting rows "left_only" and dropping column "_merge"
    df = df.query("_merge == 'left_only'").drop(columns="_merge")

    # Filtering rows depending on "tipo_cuenta"
    if tipo_cuenta == "CC":
        df = df[df["Saldo_Cta"] < 0]
        df = df.drop(columns="Saldo_Cta")

    elif tipo_cuenta == "CA":
        df = df[df["Saldo_Cta"] >= 0]
        df = df.drop(columns="Saldo_Cta")

    else:
        raise ValueError("Arg 'tipo_cuenta' must be 'CC' or 'CA'")
    
    return df


##########################################
# Get "remitos" of previous week
##########################################

df_rem_sem_ant = pd.read_sql(
    """
    DECLARE @inicioSemanaActual DATETIME
    SET @inicioSemanaActual = DATEADD(
        DAY 
        ,DATEDIFF(DAY, 0, CURRENT_TIMESTAMP)
        , -7
    )

    DECLARE @inicioSemanaAnterior DATETIME
    SET @inicioSemanaAnterior = DATEADD(
        DAY
        , DATEDIFF(DAY, 0, CURRENT_TIMESTAMP)
        , -14
    )

    SELECT 
        RTRIM(FRD.[UEN]) AS 'UEN'
        ,RTRIM(FRD.[CODPRODUCTO]) AS 'CODPRODUCTO'
        ,FRD.[NROCLIENTE]
        ,FRD.[CANTIDAD] AS 'Semana Anterior'
        ,(FC.SALDOPREPAGO - FC.SALDOREMIPENDFACTU) as 'Saldo_Cta'
    FROM [Rumaos].[dbo].[FacRemDet] as FRD
    JOIN Rumaos.dbo.FacCli as FC
        on FRD.NROCLIENTE = FC.NROCLIPRO
    WHERE FRD.FECHASQL >= @inicioSemanaAnterior
        AND FRD.FECHASQL < @inicioSemanaActual
        AND FRD.NROCLIENTE >= '100000'
        AND FRD.CANTIDAD > 0
        AND FRD.UEN IN (
            'MERC GUAYMALLEN'
            ,'MERCADO 2'
            ,'MITRE'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
        )
        AND FRD.CODPRODUCTO NOT IN (
            'GNC'
            ,'GNC01'
            ,'GNC02'
            ,'GNC03'
            ,'GNCA'
            ,'GNCCA'
            ,'GNCCC'
            ,'GNCCO'
            ,'GNCRM'
            ,'GNCVA'
            ,'GNNC1'
        )
    """
        ,conexMSSQL)
df_rem_sem_ant = df_rem_sem_ant.convert_dtypes()


##########################################
# Get "remitos" of current week
##########################################

df_rem_sem_act = pd.read_sql(
    """
    DECLARE @inicioSemanaActual DATETIME
    SET @inicioSemanaActual = DATEADD(
        DAY
        , DATEDIFF(DAY, 0, CURRENT_TIMESTAMP)
        , -7
    )

    DECLARE @hoy DATETIME
    SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

    SELECT 
        RTRIM(FRD.[UEN]) AS 'UEN'
        ,RTRIM(FRD.[CODPRODUCTO]) AS 'CODPRODUCTO'
        ,FRD.[NROCLIENTE]
        ,FRD.[CANTIDAD] AS 'Semana Actual'
        ,(FC.SALDOPREPAGO - FC.SALDOREMIPENDFACTU) as 'Saldo_Cta'
    FROM [Rumaos].[dbo].[FacRemDet] as FRD
    JOIN Rumaos.dbo.FacCli as FC
        on FRD.NROCLIENTE = FC.NROCLIPRO
    WHERE FRD.FECHASQL >= @inicioSemanaActual
        AND FRD.FECHASQL < @hoy
        AND FRD.NROCLIENTE >= '100000'
        AND FRD.CANTIDAD > 0
        AND FRD.UEN IN (
            'MERC GUAYMALLEN'
            ,'MERCADO 2'
            ,'MITRE'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
        )
        AND FRD.CODPRODUCTO NOT IN (
            'GNC'
            ,'GNC01'
            ,'GNC02'
            ,'GNC03'
            ,'GNCA'
            ,'GNCCA'
            ,'GNCCC'
            ,'GNCCO'
            ,'GNCRM'
            ,'GNCVA'
            ,'GNNC1'
        )
    """
        ,conexMSSQL)
df_rem_sem_act = df_rem_sem_act.convert_dtypes()


##########################################
# Get "remitos" of previous month
##########################################

df_rem_sem_act = pd.read_sql(
    """
    DECLARE @inicioMesActual DATETIME
    SET @inicioMesActual = DATEADD(
        month
        , DATEDIFF(month, 0, CURRENT_TIMESTAMP)
        , 0
    )

    DECLARE @inicioMesAnterior DATETIME
    SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

    --Divide por la cant de días del mes anterior y multiplica
    --por la cant de días del mes actual
    DECLARE @pondMesAnt FLOAT
    SET @pondMesAnt = CAST(DAY(EOMONTH(CURRENT_TIMESTAMP)) AS float) /
        CAST(DAY(EOMONTH(@inicioMesAnterior)) AS float)

    SELECT 
        RTRIM(FRD.[UEN]) AS 'UEN'
        ,RTRIM(FRD.[CODPRODUCTO]) AS 'CODPRODUCTO'
        ,FRD.[NROCLIENTE]
        ,(FRD.[CANTIDAD] * @pondMesAnt) AS 'Mes Anterior'
        ,(FC.SALDOPREPAGO - FC.SALDOREMIPENDFACTU) as 'Saldo_Cta'
    FROM [Rumaos].[dbo].[FacRemDet] as FRD
    JOIN Rumaos.dbo.FacCli as FC
        on FRD.NROCLIENTE = FC.NROCLIPRO
    WHERE FRD.FECHASQL >= @inicioMesAnterior
        AND FRD.FECHASQL < @inicioMesActual
        AND FRD.NROCLIENTE >= '100000'
        AND FRD.CANTIDAD > 0
        AND FRD.UEN IN (
            'MERC GUAYMALLEN'
            ,'MERCADO 2'
            ,'MITRE'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
        )
        AND FRD.CODPRODUCTO NOT IN (
            'GNC'
            ,'GNC01'
            ,'GNC02'
            ,'GNC03'
            ,'GNCA'
            ,'GNCCA'
            ,'GNCCC'
            ,'GNCCO'
            ,'GNCRM'
            ,'GNCVA'
            ,'GNNC1'
        )
    """
        ,conexMSSQL)


##########################################
# Get "remitos" of current month
##########################################

df_rem_sem_act = pd.read_sql(
    """
    DECLARE @inicioMesActual DATETIME
    SET @inicioMesActual = DATEADD(
        month
        , DATEDIFF(month, 0, CURRENT_TIMESTAMP)
        , 0
    )

    DECLARE @hoy DATETIME
    SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

    --Divide por la cantidad de días cursados del mes actual y multiplica 
    --por la cant de días del mes actual
    DECLARE @pondMesAct FLOAT
    SET @pondMesAct = CAST(DAY(EOMONTH(CURRENT_TIMESTAMP)) AS float) /
        (CAST(DAY(CURRENT_TIMESTAMP) AS float)-1)

    SELECT 
        RTRIM(FRD.[UEN]) AS 'UEN'
        ,RTRIM(FRD.[CODPRODUCTO]) AS 'CODPRODUCTO'
        ,FRD.[NROCLIENTE]
        ,(FRD.[CANTIDAD] * @pondMesAct) AS 'Mes Actual'
        ,(FC.SALDOPREPAGO - FC.SALDOREMIPENDFACTU) as 'Saldo_Cta'
    FROM [Rumaos].[dbo].[FacRemDet] as FRD
    JOIN Rumaos.dbo.FacCli as FC
        on FRD.NROCLIENTE = FC.NROCLIPRO
    WHERE FRD.FECHASQL >= @inicioMesActual
        AND FRD.FECHASQL < @hoy
        AND FRD.NROCLIENTE >= '100000'
        AND FRD.CANTIDAD > 0
        AND FRD.UEN IN (
            'MERC GUAYMALLEN'
            ,'MERCADO 2'
            ,'MITRE'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
        )
        AND FRD.CODPRODUCTO NOT IN (
            'GNC'
            ,'GNC01'
            ,'GNC02'
            ,'GNC03'
            ,'GNCA'
            ,'GNCCA'
            ,'GNCCC'
            ,'GNCCO'
            ,'GNCRM'
            ,'GNCVA'
            ,'GNNC1'
        )
    """
        ,conexMSSQL)