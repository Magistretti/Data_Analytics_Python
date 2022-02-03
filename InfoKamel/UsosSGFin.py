###################################
#
#     INFORME USOS SGFIN
#               03/02/22
###################################

import os
import sys
import pathlib

# Allow imports from the top folder
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))

import pandas as pd
import dataframe_image as dfi

from DatosLogin import loginSGFin
from Conectores import conectorMSSQL

import logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)



####################################################################
# Get DF function
####################################################################

def _get_df(conexSGFin):
    df_usos = pd.read_sql(
        """
        -- Egresos SGFin y bancos agrupados por USOS

        SET NOCOUNT ON;

        --------------------------
        DECLARE @FECHA date
        SET @FECHA = CAST(getdate() as date);
        --------------------------

        WITH cajas as (
        -- Listado de egresos de cajas agrupados por nombre
        SELECT 
            -sum(egr.[Importe]) as EGRESOS
            ,prov.Nombre

        FROM [Sgfin].[dbo].[SGFIN_EgresoCaja] as egr

        JOIN  Sgfin.dbo.SGFIN_PlanProveedor as prov
            ON egr.IdProveedor = prov.Id
        JOIN Sgfin.dbo.SGFIN_PlanReferencia as ref
            ON egr.IdReferencia = ref.Id
        JOIN dbo.SGFIN_Arqueo as arq
            ON Egr.[IdArqueo] = arq.Id --Vinculando con la fecha de la caja

        WHERE CAST(Arq.[Fecha] as date) = @FECHA
            AND egr.IdReferencia <> 106 -- Filtrar Transferencias
            AND egr.IdReferencia <> 41 -- Filtrar Combustible a Granel
            AND egr.IdReferencia <> 108 -- Filtrar Depositos Bancos
            AND egr.IdProveedor <> 4092 -- Filtrar Ajustes de Arqueo
            AND egr.IdProveedor <> 343 -- Filtrar Mov Cheq de 3ros
            AND egr.IdProveedor <> 9840 -- Filtrar Transf de Dólares
            AND egr.Importe > 0

        GROUP BY Nombre

        )

        , bancos as (
        -- Listado de egresos de bancos agrupados por nombre
        SELECT
            prov.Nombre
            ,sum([Importe]) as EGRESOS

        FROM [Sgfin].[dbo].[SGFIN_TRANSFERENCIABANCARIADETALLE] as ban

        JOIN Sgfin.dbo.SGFIN_TRANSFERENCIABANCARIA as id
            on ban.IdTransferenciaBancaria = id.Id
        JOIN  Sgfin.dbo.SGFIN_PlanProveedor as prov
            ON ban.IdProveedor = prov.Id
        JOIN Sgfin.dbo.SGFIN_PlanReferencia as ref
            ON ban.IdReferencia = ref.Id

        WHERE ban.Fecha >= @FECHA
            AND id.IdBanco <> 71
            AND ban.Importe < 0
            AND ban.IdReferencia <> 106 -- Filtrar Transferencias
            AND ban.IdReferencia <> 41 -- Filtrar Combustible a Granel
            --AND ban.IdReferencia <> 108 -- Filtrar Depositos Bancos

        GROUP BY prov.Nombre
        )

        , uniontable as (
        -- Union de las tablas cajas y bancos
        SELECT
            Nombre
            ,EGRESOS
        FROM cajas

        UNION ALL

        SELECT
            Nombre
            ,EGRESOS
        FROM bancos

        )

        -- Tabla final que agrupa egresos > -3000 en 'VARIOS'
        SELECT
            filtro.USOS
            ,ROUND(sum(EGRESOS), 0) as EGRESOS
        FROM uniontable

        OUTER APPLY

        (SELECT
            CASE
                WHEN EGRESOS > -3000 THEN 'VARIOS'
                ELSE Nombre
            END as USOS
        ) as filtro

        GROUP BY filtro.USOS
        ORDER BY EGRESOS
        """
        , conexSGFin
    )

    df_usos = df_usos.convert_dtypes()

    # Get "TOTAL" row
    df_usos.loc[df_usos.index[-1]+1] = df_usos.sum(numeric_only=True)
    # Rename the NA in the "UEN" column to "TOTAL"
    df_usos.fillna("TOTAL", inplace=True)

    return df_usos



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

def usos_SGFin():
    """
    This function will generate an image report of the payments of the day
    """
    
    # Timer
    tiempoInicio = pd.to_datetime("today")

    # Connection to SGFin DB
    conexSGFin = conectorMSSQL(loginSGFin)

    # Get DF
    df_usos = _get_df(conexSGFin)

    # Styling of DF
    df_estilo = _estiladorVtaTitulo(
        df_usos
        , ["EGRESOS"]
        , titulo="Egresos Por Uso"
    )

    # Files location
    ubicacion = str(pathlib.Path(__file__).parent)+"\\"

    # Get image of df_estilo
    _df_to_image(df_estilo, ubicacion, "Usos_SGFin.png")


    # Timer
    tiempoFinal = pd.to_datetime("today")
    logger.info(
        "Info Arqueos"
        + "\nTiempo de Ejecucion Total: "
        + str(tiempoFinal-tiempoInicio)
    )



if __name__ == "__main__":
    usos_SGFin()