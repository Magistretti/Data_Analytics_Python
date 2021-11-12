###################################
#
#     INFORME Red Control Semanal
#             
#               11/11/21
###################################

import os
import sys
import pathlib
from numpy import clongdouble, inner

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

df_rem_mes_ant = pd.read_sql(
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

df_rem_mes_ant = df_rem_mes_ant.convert_dtypes()

##########################################
# Get "remitos" of current month
##########################################

df_rem_mes_act = pd.read_sql(
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

df_rem_mes_act = df_rem_mes_act.convert_dtypes()


# ##########################################
# # Filtering DFs and getting clients with negative balance ("CC")
# ##########################################

# df_rem_sem_ant_CC = _filtrador(df_rem_sem_ant, "CC")
# df_rem_sem_act_CC = _filtrador(df_rem_sem_act, "CC")
# df_rem_mes_ant_CC = _filtrador(df_rem_mes_ant, "CC")
# df_rem_mes_act_CC = _filtrador(df_rem_mes_act, "CC")

# # Grouping by "UEN"
# df_rem_mes_ant_CC = df_rem_mes_ant_CC.groupby("UEN", as_index=False).sum()
# df_rem_mes_act_CC = df_rem_mes_act_CC.groupby("UEN", as_index=False).sum()
# # Merging 
# df_rem_mes_CC_merge = pd.merge(
#     df_rem_mes_ant_CC
#     , df_rem_mes_act_CC
#     , how="left"
#     , on="UEN"
# )
# # Total Row and NaN replace in column "UEN"
# df_rem_mes_CC_merge.loc["totalRow"] = df_rem_mes_CC_merge.sum(numeric_only=True)
# df_rem_mes_CC_merge.fillna({"UEN":"TOTAL"}, inplace=True)
# # Column "Intermensual %"
# df_rem_mes_CC_merge["Intermensual %"] = (df_rem_mes_CC_merge["Mes Actual"] /
#     df_rem_mes_CC_merge["Mes Anterior"] - 1)
# # Column "Intermensual Volumen"
# df_rem_mes_CC_merge["Intermensual Volumen"] = \
# (df_rem_mes_CC_merge["Mes Actual"] - df_rem_mes_CC_merge["Mes Anterior"])



##########################################
# Function to refine dataframes according to balance
##########################################

def _filtrador(df, balance):
    '''
    -Parameters-
    df: Dataframe to be filtered
    balance: "CC" (negative) OR "CA" (positive)
    
    -Returns-
    Dataframe
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

    # Filtering rows depending on "balance"
    if balance == "CC":
        df = df[df["Saldo_Cta"] < 0]
        df = df.drop(columns="Saldo_Cta")

    elif balance == "CA":
        df = df[df["Saldo_Cta"] >= 0]
        df = df.drop(columns="Saldo_Cta")

    else:
        raise ValueError("Arg 'balance' must be 'CC' or 'CA'")
    
    return df



##########################################
# Function that allow filtering DFs and getting values according to
# cycle and balance
##########################################

def _preparador(df_ant, df_act, cycle, balance):
    '''
    -Parameters-
    df_ant: Dataframe of previous period
    df_act: Dataframe of current period
    cycle: "weekly" OR "monthly"
    balance: "CC" (negative) OR "CA" (positive)

    -Return-
     Dataframe
    '''
    # Filtering and grouping of df_ant
    df_ant = _filtrador(df_ant, balance).groupby("UEN", as_index=False).sum()
    df_ant = df_ant.groupby("UEN", as_index=False).sum()
    # Filtering and grouping of df_act
    df_act = _filtrador(df_act, balance).groupby("UEN", as_index=False).sum()
    df_act = df_act.groupby("UEN", as_index=False).sum()
    # merge df_ant and df_act
    df_merge = pd.merge(
        df_ant
        , df_act
        , how="left"
        , on="UEN"
    )
    # Total Row and NaN replace in column "UEN"
    df_merge.loc["totalRow"] = df_merge.sum(numeric_only=True)
    df_merge.fillna({"UEN":"TOTAL"}, inplace=True)
    
    # Check "cycle"
    if cycle == "weekly":
        # Column "Intersemanal %"
        df_merge["Intersemanal %"] = \
            (df_merge["Semana Actual"] / df_merge["Semana Anterior"] - 1)
    elif cycle == "monthly":
        # Column "Intermensual %"
        df_merge["Intermensual %"] = \
            (df_merge["Mes Actual"] / df_merge["Mes Anterior"] - 1)
        # Column "Intermensual Volumen"
        df_merge["Intermensual Volumen"] = \
            (df_merge["Mes Actual"] - df_merge["Mes Anterior"])
    else:
        raise ValueError("Arg 'cycle' must be 'weekly' or 'monthly'")

    
    return df_merge


##########################################
# Cleaning, merging and sorting dataframes
##########################################

df_week_CC = _preparador(df_rem_sem_ant, df_rem_sem_act, "weekly", "CC")
df_month_CC = _preparador(df_rem_mes_ant, df_rem_mes_act, "monthly", "CC")

df_CC = pd.merge(
    df_week_CC
    , df_month_CC
    , on="UEN"
    , how="right"
)

df_week_CA = _preparador(df_rem_sem_ant, df_rem_sem_act, "weekly", "CA")
df_month_CA = _preparador(df_rem_mes_ant, df_rem_mes_act, "monthly", "CA")

df_CA = pd.merge(
    df_week_CA
    , df_month_CA
    , on="UEN"
    , how="right"
)

# Sorting by "Intermensual Volumen"
def _sorter(df, column):
    '''
    df: Dataframe
    column: Name of the sorting column (str)
    Returns a sorted Dataframe
    '''
    row_total = df.loc[df.index[-1]]
    df_sorted = df[df["UEN"] != "TOTAL"].sort_values(by=column)
    df_sorted = df_sorted.append(row_total)

    return df_sorted

df_CC = _sorter(df_CC, "Intermensual Volumen")
df_CA = _sorter(df_CA, "Intermensual Volumen")



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
            + "Semana Actual "
            + ((pd.to_datetime("today")-pd.to_timedelta(8,"days"))
            .strftime("%d/%m/%y"))
            + " al "
            + ((pd.to_datetime("today")-pd.to_timedelta(1,"days"))
            .strftime("%d/%m/%y"))
        ) \
        .set_properties(subset=list_Col_Num + list_Col_Perc
            , **{"text-align": "center", "width": "80px"}) \
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
        .apply(lambda x: ["background: black" if x.name == df.index[-1] 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == df.index[-1]
            else "" for i in x]
            , axis=1)

    
    return resultado


colnum = [
    "Semana Anterior"
    ,"Semana Actual"
    , "Mes Anterior"
    , "Mes Actual"
    , "Intermensual Volumen"
]
colperc = ["Intersemanal %", "Intermensual %"]


df_CC_Estilo = _estiladorVtaTitulo(
    df_CC
    , colnum
    , colperc
    , "Red Control Líquidos (s/ remitos) - Cuenta Corriente"
)

df_CA_Estilo = _estiladorVtaTitulo(
    df_CA
    , colnum
    , colperc
    , "Red Control Líquidos (s/ remitos) - Cuenta Adelantada"
)
