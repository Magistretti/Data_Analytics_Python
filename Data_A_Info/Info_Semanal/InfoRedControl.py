###################################
#
#     INFORME Red Control Semanal
#             
#        11/11/21 - 15/11/21
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


def pre_RedControlSemanal(conexMSSQL):
    '''
    Will calculate and return 2 Dataframes and the order will be df_CC, df_CA

    conexMSSQL: conection to SQL Server
    '''
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
     Declare @pondMesAct Float
    SET @pondMesAct = CAST(DAY(EOMONTH(getdate()-1)) AS float) /
    (CAST(DAY(getdate()-1) AS float))

        SELECT 
            RTRIM(FRD.[UEN]) AS 'UEN'
            ,RTRIM(FRD.[CODPRODUCTO]) AS 'CODPRODUCTO'
            ,FRD.[NROCLIENTE]
            ,(FRD.[CANTIDAD] * @pondMesAct) AS 'Mes Anterior'
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
     Declare @pondMesAct Float
    SET @pondMesAct = CAST(DAY(EOMONTH(getdate()-1)) AS float) /
    (CAST(DAY(getdate()-1) AS float))
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
        df_ant = _filtrador(df_ant, balance)
        df_ant = df_ant.groupby("UEN", as_index=False).sum()
        # Filtering and grouping of df_act
        df_act = _filtrador(df_act, balance)
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

    return df_CC, df_CA



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
            + ((pd.to_datetime("today")-pd.to_timedelta(7,"days"))
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
        dfi.export(df, ubicacion+nombre)
    else:
        dfi.export(df, ubicacion+nombre)



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

def redControlSemanal():
    '''
    Create image "Info_RedControlLiq_Semanal.png"
    '''

    # Timer
    tiempoInicio = pd.to_datetime("today")

    conexMSSQL = conectorMSSQL(login)

    # Run pre_RedControlSemanal
    df_CC, df_CA = pre_RedControlSemanal(conexMSSQL)

    # List of numeric columns
    colnum = [
    "Semana Anterior"
    ,"Semana Actual"
    , "Mes Anterior"
    , "Mes Actual"
    , "Intermensual Volumen"
    ]
    # List of percentage column
    colperc = ["Intersemanal %", "Intermensual %"]

    # Give style to df_CC and df_CA
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

    # Path and name for DFs images
    ubicacion = str(pathlib.Path(__file__).parent)+"\\"
    nombreIMG_CC = "redControlLiq_CC.png"
    nombreIMG_CA = "redControlLiq_CA.png"

    _df_to_image(df_CC_Estilo, ubicacion, nombreIMG_CC)
    _df_to_image(df_CA_Estilo, ubicacion, nombreIMG_CA)

    listaImg = [ubicacion + nombreIMG_CC, ubicacion + nombreIMG_CA]

    # Merge DFs images vertically and save it as a .png
    fusionImg = _append_images(listaImg, direction="vertical")
    fusionImg.save(ubicacion + "Info_RedControlLiq_Semanal.png")

    # Timer
    tiempoFinal = pd.to_datetime("today")
    logger.info(
        "\nInfo Red Control Liq"
        + "\nTiempo de Ejecucion Total: "
        + str(tiempoFinal-tiempoInicio)
    )
    



if __name__ == "__main__":
    redControlSemanal()