###################################
#
#     INFORME ARQUEOS SGFIN
#               28/12/21
###################################

import os
import sys
import pathlib

# Allow imports from the top folder
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))

import pandas as pd
import dataframe_image as dfi

from DatosLogin import login, loginSGFin
from Conectores import conectorMSSQL

import logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)



def _get_df(conexSGES, conexSGFin):

    # Get state of the treasuries in a df from a SQL query
    df_arqueos = pd.read_sql(
        """
        -- EN ESTA CONSULTA SE REALIZA UN APPEND DE LAS TABLAS [SGFIN_IngresoCaja] Y 
        -- [SGFIN_EgresoCaja] FILTRADAS PARA MOSTRAR SOLO MOVIMIENTOS DE FONDOS ENTRE
        -- UENS, LUEGO SE REALIZA UNA CONSULTA A LA TABLA [SGFIN_Arqueo] Y SE REALIZA
        -- UN JOIN A LAS SUBCONSULTAS DE LA TABLA [SGFIN_IngresoCaja] Y DE LA TABLA
        -- [SGFIN_EgresoCaja]

        --------------------------
        DECLARE @FECHA NVARCHAR(20)
        SET @FECHA = CAST(getdate() as date);
        --------------------------

        --NOMBRAMOS LA UNION (CONCATENADO) DE LAS TABLAS REALIZANDO UN CTE(COMMON
        -- TABLE EXPRESSION)
        WITH uniontable AS
        (
            SELECT --PRIMERA TABLA FILTRADA Y AGRUPADA
                CAST(Arq.[Fecha] as date) as 'Fecha'
                ,RTRIM(box.UEN) as 'UEN'
                ,ROUND(SUM(DIng.[Importe]),0) as 'Traslados' --ENTRADAS EN POSITIVO
            FROM [Sgfin].[dbo].[SGFIN_IngresoCaja] as Ing
            Inner JOIN dbo.SGFIN_DetalleIngreso as DIng
                ON Ing.Id = DIng.IdIngreso --Vinculando con el detalle de Egreso
            Left Outer JOIN dbo.SGFIN_Arqueo as arq
                ON Ing.[IdArqueo] = arq.Id --Vinculando con la fecha de la caja
            Left Outer JOIN dbo.SGFIN_Box as box
                ON arq.IdBox = box.Id --Vinculando con el nombre de la caja
            where Ing.IdProveedor in (
                '344','345','394','395','396','397','398','399','400'
                ,'401','402','403','404','471','534','1730','4081','4083'
            ) --IdProveedor de las transferencias con relevamiento 17/12/21
                AND Ing.IdReferencia not IN ('108') --Sin Banco
                AND DIng.IdCartera is NULL --Solo Efectivo
                And CAST(Arq.[Fecha] as date) = @FECHA
            group by CAST(Arq.[Fecha] as date), box.UEN
            --order by box.UEN

            UNION ALL --CONCATENAR TODOS LOS DATOS

            SELECT --SEGUNDA TABLA FILTRADA Y AGRUPADA
                CAST(Arq.[Fecha] as date) as 'Fecha'
                ,RTRIM(box.UEN) as 'UEN'
                ,ROUND(SUM(-DEgr.[Importe]),0) as 'Traslados' --SALIDAS EN NEGATIVO
            FROM [Sgfin].[dbo].[SGFIN_EgresoCaja] as Egr
            Inner JOIN dbo.SGFIN_DetalleEgreso as DEgr
                ON Egr.Id = DEgr.IdEgreso --Vinculando con el detalle de Egreso
            Left Outer JOIN dbo.SGFIN_Arqueo as arq
                ON Egr.[IdArqueo] = arq.Id --Vinculando con la fecha de la caja
            Left Outer JOIN dbo.SGFIN_Box as box
                ON arq.IdBox = box.Id --Vinculando con el nombre de la caja
            where Egr.IdProveedor in (
                '344','345','394','395','396','397','398','399','400'
                ,'401','402','403','404','471','534','1730','4081','4083'
            ) --IdProveedor de las transferencias con relevamiento 17/12/21
                AND Egr.IdReferencia not IN ('108') --Sin Banco
                AND (IdCartera is null and IdTipoPago = '1'
                OR IdCartera is null and IdTipoPago is null) --Solo Efectivo
                And CAST(Arq.[Fecha] as date) = @FECHA
            group by CAST(Arq.[Fecha] as date), box.UEN
            --order by box.UEN
        )

        -- INICIO CONSULTA
        SELECT
            cast(arq.[Fecha] as date) as 'Fecha' --NOS REGIMOS POR LA FECHA DE LOS ARQUEOS
            ,RTRIM(box.UEN) as 'UEN'
            ,ISNULL(ROUND(Max(arq.[FondoInicial]),0),0) as 'Saldo Inicial'
            ,ISNULL(Max(ig.INGRESOS),0) as 'INGRESOS'
            ,ISNULL(sum(ut.Traslados),0) as 'Traslados'
            ,ISNULL(Min(eg.EGRESOS),0) as 'EGRESOS'
            ,ISNULL(ROUND(Max(arq.[FondoReal]),0),0) as 'Saldo Final'
        FROM [Sgfin].[dbo].[SGFIN_Arqueo] as arq

        LEFT OUTER JOIN dbo.[SGFIN_Box] as box
            ON arq.IdBox = box.Id

        Left OUTER JOIN uniontable as ut
            ON ut.UEN = box.UEN

        -- SUBCONSULTA INGRESOS
        LEFT OUTER JOIN (
            SELECT
                CAST(Arq.[Fecha] as date) as 'Fecha'
                ,RTRIM(box.UEN) as 'UEN'
                ,ROUND(SUM(DIng.[Importe]),0) as 'INGRESOS'
            FROM [Sgfin].[dbo].[SGFIN_IngresoCaja] as Ing
            Inner JOIN dbo.SGFIN_DetalleIngreso as DIng
                ON Ing.Id = DIng.IdIngreso --Vinculando con el detalle de Egreso
            Left Outer JOIN dbo.SGFIN_Arqueo as arq
                ON Ing.[IdArqueo] = arq.Id --Vinculando con la fecha de la caja
            Left Outer JOIN dbo.SGFIN_Box as box
                ON arq.IdBox = box.Id --Vinculando con el nombre de la caja
            where Ing.IdProveedor not in (
                '344','345','394','395','396','397','398','399','400'
                ,'401','402','403','404','471','534','1730','4081','4083'
            ) --IdProveedor de las transferencias con relevamiento 17/12/21
                AND Ing.IdReferencia not IN ('108') --Sin Banco
                AND DIng.IdCartera is NULL --Solo Efectivo
                And CAST(Arq.[Fecha] as date) = @FECHA
            group by CAST(Arq.[Fecha] as date), box.UEN
        ) as ig
            ON ig.UEN = box.UEN

        -- SUBCONSULTA EGRESOS
        LEFT OUTER JOIN (
            SELECT
                CAST(Arq.[Fecha] as date) as 'Fecha'
                ,RTRIM(box.UEN) as 'UEN'
                ,ROUND(SUM(-DEgr.[Importe]),0) as 'EGRESOS'
            FROM [Sgfin].[dbo].[SGFIN_EgresoCaja] as Egr
            Inner JOIN dbo.SGFIN_DetalleEgreso as DEgr
                ON Egr.Id = DEgr.IdEgreso --Vinculando con el detalle de Egreso
            Left Outer JOIN dbo.SGFIN_Arqueo as arq
                ON Egr.[IdArqueo] = arq.Id --Vinculando con la fecha de la caja
            Left Outer JOIN dbo.SGFIN_Box as box
                ON arq.IdBox = box.Id --Vinculando con el nombre de la caja
            where Egr.IdProveedor not in (
                '344','345','394','395','396','397','398','399','400'
                ,'401','402','403','404','471','534','1730','4081','4083'
            ) --IdProveedor de las transferencias con relevamiento 17/12/21
                AND Egr.IdReferencia not IN ('108') --Sin Banco
                AND (IdCartera is null and IdTipoPago = '1'
                OR IdCartera is null and IdTipoPago is null) --Solo Efectivo
                And CAST(Arq.[Fecha] as date) = @FECHA
            group by CAST(Arq.[Fecha] as date), box.UEN
        ) as eg
            ON eg.UEN = box.UEN


        where box.UEN not IN ('RENDICIONES')
            AND cast(arq.Fecha as date) = @FECHA
        group by cast(arq.[Fecha] as date), box.UEN
        order by box.UEN
        """
        , conexSGFin
    )

    df_arqueos = df_arqueos.convert_dtypes()

    df_arqueos.drop(columns=["Fecha"], inplace=True)
    
    # Get "TOTAL" row
    df_arqueos.loc[df_arqueos.index[-1]+1] = df_arqueos.sum(numeric_only=True)
    # Rename the NA in the "UEN" column to "TOTAL"
    df_arqueos.fillna("TOTAL", inplace=True)


    # Get pending collections of each treasury into a df from a SQL query
    df_RecPendiente = pd.read_sql(
        """
        -- UNION de las recaudaciones pendientes de los distintos negocios
        -- de cada UEN en una sola tabla agrupada por UEN

        WITH uniontable AS
        (
            SELECT -- Rec Pend Playa
                RTRIM([UEN]) as 'UEN'
                ,ROUND(SUM([VTATOTIMP]-[VTACTACTEIMP]-[VTAADELIMP]), 0) as 'Rec Pendiente'

            FROM [Rumaos].[dbo].[VtaTurno] with (NOLOCK)
            WHERE NRORECA = '0'
                AND VTATOTIMP > '0'
            Group By UEN

            UNION ALL

            SELECT -- Rec Pend SC
                RTRIM([UEN]) as 'UEN'
                ,ROUND(SUM(
                    ([VTATOTIMP] + IIF(VTATOTIMP = 0 and RECTEORICA <> 0, RECTEORICA, 0))
                    ), 0
                ) as 'Rec Pend SC'

            FROM [Rumaos].[dbo].[SCTurnos] with (NOLOCK)
            WHERE NRORECA = '0'
                AND (VTATOTIMP > '0' or RECTEORICA > '0')
            Group By UEN

            UNION ALL

            SELECT -- Rec Pend Pana
                RTRIM([UEN]) as 'UEN'
                ,ROUND(SUM([VTATOTAL]), 0) as 'Rec Pend Pana'

            FROM [Rumaos].[dbo].[PanTurnos] with (NOLOCK)
            WHERE NRORECA = '0'
                AND VTATOTAL > '0'
            Group By UEN

            UNION ALL

            SELECT -- Rec Pend BA
                RTRIM([UEN]) as 'UEN'
                ,ROUND(SUM([VTATOTIMP]), 0) as 'Rec Pend BA'

            FROM [Rumaos].[dbo].[BATurnos] with (NOLOCK)
            WHERE NRORECA = '0'
                AND VTATOTIMP > '0'
            Group By UEN
        )


        SELECT -- Se agrega fila de totales con una unión sobre la misma tabla
            total.UEN
            ,total.[Rec Pendiente]
        FROM(
            SELECT
                uniontable.UEN
                ,CAST(SUM(uniontable.[Rec Pendiente]) as int) as 'Rec Pendiente'
            FROM uniontable
            Group By UEN

            UNION ALL

            SELECT
                'UEN' = 'TOTAL'
                ,CAST(SUM(uniontable.[Rec Pendiente]) as int) as 'Rec Pendiente'
            FROM uniontable
        ) as total
        """
        , conexSGES
    )

    # Merge df_RecPendiente with df_arqueos
    df_arqueos = pd.merge(
        df_arqueos,
        df_RecPendiente,
        how="left",
        on=["UEN"]
    )

    # Replace the NaNs with zeroes
    df_arqueos.fillna(0, inplace=True)


    return df_arqueos



##########################################
# STYLING of the dataframe
##########################################

def _estiladorVtaTitulo(
    df:pd.DataFrame
    , list_Col_Num=[]
    , list_Col_Perc=[]
    , titulo=""
):
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
        .format("$ {0:,.0f}", subset=list_Col_Num) \
        .format("{:,.2%}", subset=list_Col_Perc) \
        .hide_index() \
        .set_caption(
            titulo
            + " "
            + (pd.to_datetime("today")
            .strftime("%d/%m/%y"))
        ) \
        .set_properties(subset=list_Col_Num
            , **{"text-align": "right", "width": "100px"}) \
        .set_properties(subset=list_Col_Perc
            , **{"text-align": "center", "width": "90px"}) \
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
        .apply(lambda x: ["background-color: black" if x.name == df.index[-1] 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == df.index[-1]
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["font-size: 15px" if x.name == df.index[-1]
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
# FUNCTION TO RUN MODULE
##########################################

def arqueos():
    """
    This function will generate an image report of the actual state of treasuries
    """
    
    # Timer
    tiempoInicio = pd.to_datetime("today")

    # Connection to SGES DB
    conexSGES = conectorMSSQL(login)
    # Connection to SGFin DB
    conexSGFin = conectorMSSQL(loginSGFin)

    # Get DF
    df_arqueos = _get_df(conexSGES, conexSGFin)

    # Styling of DF
    df_arqueos_Estilo = _estiladorVtaTitulo(
        df_arqueos
        , [
            "Saldo Inicial"
            , "INGRESOS"
            , "Traslados"
            , "EGRESOS"
            , "Saldo Final"
            , "Rec Pendiente"
        ], titulo="ARQUEOS"
    )

    # Files location
    ubicacion = str(pathlib.Path(__file__).parent)+"\\"

    # Get image of df_arqueos_Estilo
    _df_to_image(df_arqueos_Estilo, ubicacion, "Arqueos.png")


    # Timer
    tiempoFinal = pd.to_datetime("today")
    logger.info(
        "Info Arqueos"
        + "\nTiempo de Ejecucion Total: "
        + str(tiempoFinal-tiempoInicio)
    )




if __name__ == "__main__":
    arqueos()