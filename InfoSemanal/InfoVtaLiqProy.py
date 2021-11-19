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


#########
# Formating display of dataframes with comma separator for numbers
pd.options.display.float_format = "{:20,.0f}".format 
#########

conexMSSQL = conectorMSSQL(login)



##########################################
# Get sales of liquid fuel
##########################################

df_vtas_liq = pd.read_sql(
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
df_vtas_liq = df_vtas_liq.convert_dtypes()


##########################################
# Get "gift" values of liquid fuel
##########################################

df_regalo_liq = pd.read_sql(
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
    , conexMSSQL
)
df_regalo_liq.fillna(0, inplace=True)
df_regalo_liq = df_regalo_liq.convert_dtypes()


##########################################
# Subtract gifts from sales of liquid fuel
##########################################

df_vtas_liq_neto = df_vtas_liq.set_index(["UEN","CODPRODUCTO"])
df_vtas_liq_neto = df_vtas_liq_neto.add(
    df_regalo_liq.set_index(["UEN","CODPRODUCTO"])
    , fill_value=0
)
df_vtas_liq_neto = df_vtas_liq_neto.reset_index()

# print(df_vtas_liq_neto)


##########################################
# Create column "GRUPO" and "BANDERA"
##########################################

# GRUPO
def _grupo(codproducto):
    if codproducto == "GO" or codproducto == "EU":
        return "GASÓLEOS"
    elif codproducto == "NS" or codproducto == "NU":
        return "NAFTAS"
    else:
        return "GNC"

# Create column _ from column CODPRODUCTO
df_vtas_liq_neto["GRUPO"] = df_vtas_liq_neto.apply(
    lambda row: _grupo(row["CODPRODUCTO"])
        , axis= 1
)

df_vtas_liq_neto = df_vtas_liq_neto.groupby(["UEN","GRUPO"], as_index=False).sum()

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
df_vtas_liq_neto["BANDERA"] = df_vtas_liq_neto.apply(
    lambda row: _bandera(row["UEN"])
        , axis= 1
)



def _prepLiq(df):
    # Prepping df disregarding "GRUPO"
    df_liq = df.drop(columns="GRUPO")
    df_liq=df_liq.groupby(["UEN","BANDERA"], as_index=False).sum()

    # Separate by "BANDERA" and dropping column "BANDERA"
    df_liq_ypf = df_liq[df_liq["BANDERA"] == "YPF"].drop(columns="BANDERA")
    df_liq_dapsa = df_liq[df_liq["BANDERA"] == "DAPSA"].drop(columns="BANDERA")


    # Get row of totals and column "Intermensual %"
    # GRAND TOTAL
    df_tot = df_liq[[
        "Volumen Acumulado"
        , "Mes Anterior Vol Proyectado"
        , "Mes Actual Vol Proyectado"
    ]].sum()
    df_tot["UEN"] = "TOTAL"
    df_tot["Intermensual %"] = (
        df_tot["Mes Actual Vol Proyectado"] 
        / df_tot["Mes Anterior Vol Proyectado"]
        - 1
    )
    
    # YPF subtotal
    df_tot_ypf = df_liq_ypf[[
        "Volumen Acumulado"
        , "Mes Anterior Vol Proyectado"
        , "Mes Actual Vol Proyectado"
    ]].sum()
    df_tot_ypf["UEN"] = "YPF"
    df_tot_ypf["Intermensual %"] = (
        df_tot_ypf["Mes Actual Vol Proyectado"] 
        / df_tot_ypf["Mes Anterior Vol Proyectado"]
        - 1
    )

    # DAPSA subtotal
    df_tot_dapsa = df_liq_dapsa[[
        "Volumen Acumulado"
        , "Mes Anterior Vol Proyectado"
        , "Mes Actual Vol Proyectado"
    ]].sum()
    df_tot_dapsa["UEN"] = "DAPSA"
    df_tot_dapsa["Intermensual %"] = (
        df_tot_dapsa["Mes Actual Vol Proyectado"] 
        / df_tot_dapsa["Mes Anterior Vol Proyectado"]
        - 1
    )
    
    # Add column "Intermensual %" to df_liq_ypf and df_liq_dapsa and sort
    # YPF
    df_liq_ypf["Intermensual %"] = (
        df_liq_ypf["Mes Actual Vol Proyectado"] 
        / df_liq_ypf["Mes Anterior Vol Proyectado"]
        - 1
    )
    df_liq_ypf.sort_values("Intermensual %", inplace=True)
    
    # DAPSA
    df_liq_dapsa["Intermensual %"] = (
        df_liq_dapsa["Mes Actual Vol Proyectado"] 
        / df_liq_dapsa["Mes Anterior Vol Proyectado"]
        - 1
    )
    df_liq_dapsa.sort_values("Intermensual %", inplace=True)
    
    # Transform df_tot_ypf series to dataframe and transpose it
    df_completo = df_tot_ypf.to_frame().transpose()
    # Reorder columns
    df_completo = df_completo[[
        "UEN"
        , "Volumen Acumulado"
        , "Mes Anterior Vol Proyectado"
        , "Mes Actual Vol Proyectado"
        , "Intermensual %"
    ]]
    
    # Append all the series and dataframes to get rows in order and dont 
    # mess up the the sorting
    df_completo = df_completo.append(df_liq_ypf, ignore_index=True)
    df_completo = df_completo.append(df_tot_dapsa, ignore_index=True)
    df_completo = df_completo.append(df_liq_dapsa, ignore_index=True)
    df_completo = df_completo.append(df_tot, ignore_index=True)

    # Create row "Intermensual Volumen"
    df_completo["Intermensual Volumen"] = (
        df_completo["Mes Actual Vol Proyectado"]
        - df_completo["Mes Anterior Vol Proyectado"]
    )

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
            + pd.to_datetime("today").month_name("ES")
            + "-"
            + pd.to_datetime("today").strftime("%Y")
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

# def vtaMensualLiq():
#     '''
#     Create image ".png"
#     '''

#     # Timer
#     tiempoInicio = pd.to_datetime("today")

#     conexMSSQL = conectorMSSQL(login)

#     df = pre_VtaProyGranClient(conexMSSQL)

numCols = ["Volumen Acumulado"
    , "Mes Anterior Vol Proyectado"
    , "Mes Actual Vol Proyectado"
    , "Intermensual Volumen"
]

percCols = ["Intermensual %"]

a = _prepLiq(df_vtas_liq_neto)
a = _estiladorVtaTitulo(a, numCols, percCols, "VENTA DE LÍQUIDOS")
# display(a)

# Path and name for DF image
ubicacion = str(pathlib.Path(__file__).parent)+"\\"
nombreIMG = "Info_VtaLiquido_Semanal.png"

_df_to_image(a, ubicacion, nombreIMG)
