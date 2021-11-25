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

#conexMSSQL = conectorMSSQL(login)



##########################################
# Get sales of liquid fuel and GNC fuel
##########################################

def _get_df(conexMSSQL):

    ##########################################
    # Liquid Fuel Sales
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
    # Gift of Liquid Fuel
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
    # GNC Fuel Sales
    ##########################################

    df_vtas_GNC = pd.read_sql(
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
        , conexMSSQL
    )
    df_vtas_GNC = df_vtas_GNC.convert_dtypes()


    ##########################################
    # Gift of GNC Fuel
    ##########################################

    df_regalo_GNC = pd.read_sql(
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
        , conexMSSQL
    )
    df_regalo_GNC.fillna(0, inplace=True)
    df_regalo_GNC = df_regalo_GNC.convert_dtypes()


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
    df_vtas_GNC_neto = df_vtas_liq.set_index(["UEN","CODPRODUCTO"])
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
        if codproducto == "GO" or codproducto == "EU":
            return "GASÓLEOS"
        elif codproducto == "NS" or codproducto == "NU":
            return "NAFTAS"
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

    return df_vtas_liq_neto, df_vtas_GNC_neto




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
    ]].sum()
    df_tot["UEN"] = "TOTAL"
    df_tot["Intermensual %"] = (
        df_tot["Mes Actual Vol Proyectado"] 
        / df_tot["Mes Anterior Vol Proyectado"]
        - 1
    )
    
    # YPF subtotal
    df_tot_ypf = df_ypf[[
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
    df_tot_dapsa = df_dapsa[[
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
    
    # Add column "Intermensual %" to df_ypf and df_dapsa and sort
    # YPF
    df_ypf["Intermensual %"] = (
        df_ypf["Mes Actual Vol Proyectado"] 
        / df_ypf["Mes Anterior Vol Proyectado"]
        - 1
    )
    df_ypf.sort_values("Intermensual %", inplace=True)
    
    # DAPSA
    df_dapsa["Intermensual %"] = (
        df_dapsa["Mes Actual Vol Proyectado"] 
        / df_dapsa["Mes Anterior Vol Proyectado"]
        - 1
    )
    df_dapsa.sort_values("Intermensual %", inplace=True)
    
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
    df_completo = df_completo.append(df_ypf, ignore_index=True)
    df_completo = df_completo.append(df_tot_dapsa, ignore_index=True)
    df_completo = df_completo.append(df_dapsa, ignore_index=True)
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
# MERGING images
##########################################

def _append_images(listOfImages, direction='horizontal',
                  bg_color=(255,255,255), alignment='center'):
    """
    Appends images in horizontal/vertical direction.

    Args:
        listOfImages: List of images with complete path
        direction: direction of concatenation, 'horizontal' or 'vertical'
        bg_color: Background color (default: white)
        alignment: alignment mode if images need padding;
           'left', 'right', 'top', 'bottom', or 'center'

    Returns:
        Concatenated image as a new PIL image object.
    """
    images = [Image.open(x) for x in listOfImages]
    widths, heights = zip(*(i.size for i in images))

    if direction=='horizontal':
        new_width = sum(widths)
        new_height = max(heights)
    else:
        new_width = max(widths)
        new_height = sum(heights)

    new_im = Image.new('RGB', (new_width, new_height), color=bg_color)

    offset = 0
    for im in images:
        if direction=='horizontal':
            y = 0
            if alignment == 'center':
                y = int((new_height - im.size[1])/2)
            elif alignment == 'bottom':
                y = new_height - im.size[1]
            new_im.paste(im, (offset, y))
            offset += im.size[0]
        else:
            x = 0
            if alignment == 'center':
                x = int((new_width - im.size[0])/2)
            elif alignment == 'right':
                x = new_width - im.size[0]
            new_im.paste(im, (x, offset))
            offset += im.size[1]

    return new_im



##########################################
# FUNCTION TO RUN MODULE
##########################################

def vtaSemanalProy_Liq_GNC():
    '''
    Create several image ".png" of the actual and forecasted sales of
    liquid fuel and GNC fuel:
    "Info_VtaLiquido_Semanal.png"
    "Info_GrupoLiq_Semanal.png"
    "Info_VtaGNC_Semanal.png"
    '''

    # Timer
    tiempoInicio = pd.to_datetime("today")

    conexMSSQL = conectorMSSQL(login)

    df_liq, df_GNC = _get_df(conexMSSQL)

    df_liq_GO = df_liq[df_liq["GRUPO"] == "GASÓLEOS"]
    df_liq_NS = df_liq[df_liq["GRUPO"] == "NAFTAS"]

    df_liq_listo = _preparador(df_liq)
    df_GOs_listo = _preparador(df_liq_GO)
    df_NSs_listo = _preparador(df_liq_NS)
    df_GNC_listo = _preparador(df_GNC)


    numCols = ["Volumen Acumulado"
        , "Mes Anterior Vol Proyectado"
        , "Mes Actual Vol Proyectado"
        , "Intermensual Volumen"
    ]

    percCols = ["Intermensual %"]

    df_liq_Estilo = _estiladorVtaTitulo(
        df_liq_listo
        , numCols
        , percCols
        , "VENTA DE LÍQUIDOS"
    )
    df_GOs_Estilo = _estiladorVtaTitulo(
        df_GOs_listo
        , numCols
        , percCols
        , "VENTA DE GASÓLEOS"
    )
    df_NSs_Estilo = _estiladorVtaTitulo(
        df_NSs_listo
        , numCols
        , percCols
        , "VENTA DE NAFTAS"
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
    IMG_GOs = "VtaGOs_Semanal.png"
    IMG_NSs = "VtaNSs_Semanal.png"
    IMG_GNC = "Info_VtaGNC_Semanal.png"

    _df_to_image(df_liq_Estilo, ubicacion, IMG_liq)
    _df_to_image(df_GOs_Estilo, ubicacion, IMG_GOs)
    _df_to_image(df_NSs_Estilo, ubicacion, IMG_NSs)
    _df_to_image(df_GNC_Estilo, ubicacion, IMG_GNC)

    listaImg = [ubicacion + IMG_GOs, ubicacion + IMG_NSs]

    # Merge DFs images horizontally and save it as a .png
    fusionImg = _append_images(listaImg, direction="horizontal")
    fusionImg.save(ubicacion + "Info_GrupoLiq_Semanal.png")

    # Timer
    tiempoFinal = pd.to_datetime("today")
    logger.info(
        "\nInfo Venta Líquido Proyectada"
        + "\nTiempo de Ejecucion Total: "
        + str(tiempoFinal-tiempoInicio)
    )





if __name__ == "__main__":
    vtaSemanalProy_Liq_GNC()