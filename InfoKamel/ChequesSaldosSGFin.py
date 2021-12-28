###################################
#
#     INFORME CHEQUES SALDOS SGFIN
#               28/12/21
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



def _get_df(conexMSSQL):
    df_cheques = pd.read_sql(
        """
        -- SALDO INICIAL, ENTRADAS, SALIDAS Y SALDO FINAL DE CHEQUES EN CARTERA

        -------------------------------
        DECLARE @FechaAnalisis as date
        SET @FechaAnalisis = GETDATE()
        DECLARE @FechaAnterior as date
        SET @FechaAnterior = DateADD(day,-1,@FechaAnalisis)
        DECLARE @FechaLimVenc as date 
        SET @FechaLimVenc = DateADD(day,-35,@FechaAnalisis)
        ;
        -------------------------------

        SELECT
            @FechaAnalisis as 'Fecha'
            ,'PASO' as 'UEN'
            ,ISNULL(CAST(ROUND(SUM(Car.[Importe]), 0) as numeric(18,0)),0) as 'Saldo Inicial'

            ,( --Subquery Ingreso de Cheques
                SELECT
                ISNULL(ROUND(SUM(DIng.[Importe]),0),0) as 'INGRESOS'
                FROM [Sgfin].[dbo].[SGFIN_IngresoCaja] as Ing
                Inner JOIN dbo.SGFIN_DetalleIngreso as DIng
                    ON Ing.Id = DIng.IdIngreso --Vinculando con el detalle de Egreso
                Left Outer JOIN dbo.SGFIN_Arqueo as arq
                    ON Ing.[IdArqueo] = arq.Id --Vinculando con la fecha de la caja
                where DIng.IdCartera is not NULL --Solo Cheques
                    And CAST(Arq.[Fecha] as date) = @FechaAnalisis
            ) as 'Ingreso Cheq'

            ,( --Subquery Egreso de Cheques
                SELECT
                ISNULL(ROUND(SUM(-DEgr.[Importe]),0),0) as 'EGRESOS'
                FROM [Sgfin].[dbo].[SGFIN_EgresoCaja] as Egr
                Inner JOIN dbo.SGFIN_DetalleEgreso as DEgr
                    ON Egr.Id = DEgr.IdEgreso --Vinculando con el detalle de Egreso
                Left Outer JOIN dbo.SGFIN_Arqueo as arq
                    ON Egr.[IdArqueo] = arq.Id --Vinculando con la fecha de la caja
                where DEgr.IdCartera is not NULL --Solo Cheques
                    And CAST(Arq.[Fecha] as date) = @FechaAnalisis
            ) as 'Egreso Cheq'
            
            ,( --Saldo Final como suma de las variables anteriores
                CAST(ROUND(SUM(Car.[Importe]), 0) as numeric(18,0)) 
                +(
                    SELECT
                    ISNULL(ROUND(SUM(DIng.[Importe]),0),0) as 'INGRESOS'
                    FROM [Sgfin].[dbo].[SGFIN_IngresoCaja] as Ing
                    Inner JOIN dbo.SGFIN_DetalleIngreso as DIng
                        ON Ing.Id = DIng.IdIngreso --Vinculando con el detalle de Egreso
                    Left Outer JOIN dbo.SGFIN_Arqueo as arq
                        ON Ing.[IdArqueo] = arq.Id --Vinculando con la fecha de la caja
                    where DIng.IdCartera is not NULL --Solo Cheques
                        And CAST(Arq.[Fecha] as date) = @FechaAnalisis
                )
                +(
                    SELECT
                    ISNULL(ROUND(SUM(-DEgr.[Importe]),0),0) as 'EGRESOS'
                    FROM [Sgfin].[dbo].[SGFIN_EgresoCaja] as Egr
                    Inner JOIN dbo.SGFIN_DetalleEgreso as DEgr
                        ON Egr.Id = DEgr.IdEgreso --Vinculando con el detalle de Egreso
                    Left Outer JOIN dbo.SGFIN_Arqueo as arq
                        ON Egr.[IdArqueo] = arq.Id --Vinculando con la fecha de la caja
                    where DEgr.IdCartera is not NULL --Solo Cheques
                        And CAST(Arq.[Fecha] as date) = @FechaAnalisis
                )
            ) as 'Saldo Final'

        FROM [Sgfin].[dbo].[SGFIN_Cartera] AS Car
        -- Joins con las tablas [SGFIN_BoletaDeposito_Cartera] y [SGFIN_BoletaDeposito]
        -- para obtener la fecha de depósito
        LEFT OUTER JOIN [dbo].[SGFIN_BoletaDeposito_Cartera] AS BDCar
            ON Car.Id = BDCar.IdCartera
        LEFT OUTER JOIN dbo.[SGFIN_BoletaDeposito] AS BDep
            ON BDCar.IdBoletaDeposito = BDep.Id
            
            --FILTRANDO PARA OBTENER SALDO INICIAL DE CHEQUES
        WHERE CAST(Car.FechaIngreso as date) < @FechaAnalisis--Todos los documentos ingresados hasta el día anterior
            -- y con fecha de salida mayor a la del día anterior o nula
            AND (CAST(FechaSalida as date) > @FechaAnterior OR Car.FechaSalida is NULL)
            -- y con fecha de depósito mayor a la del día anterior o nula
            AND (CAST(BDep.Fecha as date) > @FechaAnterior OR BDep.Fecha is NULL)
            -- y con fecha de cobro mayor a la fecha de análisis menos 35 días
            AND CAST(Car.FechaCobro as date) > @FechaLimVenc
            -- y que el estado no sea "Rechazado"
            AND Car.Estado <> '4'
        """
    , conexMSSQL
    )

    df_cheques = df_cheques.convert_dtypes()

    df_cheques.drop(columns=["Fecha"], inplace=True)

    # print(df_cheques)

    return df_cheques


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
        # .apply(lambda x: ["background-color: black" if x.name == df.index[-1] 
        #     else "" for i in x]
        #     , axis=1) \
        # .apply(lambda x: ["color: white" if x.name == df.index[-1]
        #     else "" for i in x]
        #     , axis=1) \
        # .apply(lambda x: ["font-size: 15px" if x.name == df.index[-1]
        #     else "" for i in x]
        #     , axis=1)

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

def chequesSaldos():
    """
    This function will generate an image report of the actual state of the
    check wallet
    """
    
    # Timer
    tiempoInicio = pd.to_datetime("today")

    # Connection to SGFin DB
    conexMSSQL = conectorMSSQL(loginSGFin)

    # Get DF
    df_cheques = _get_df(conexMSSQL)

    # Styling of DF
    df_cheques_Estilo = _estiladorVtaTitulo(
        df_cheques
        , [
            "Saldo Inicial"
            , "Ingreso Cheq"
            , "Egreso Cheq"
            , "Saldo Final"
        ], titulo="CHEQUES (SALDOS)"
    )

    # Files location
    ubicacion = str(pathlib.Path(__file__).parent)+"\\"

    # Get image of df_arqueos_Estilo
    _df_to_image(df_cheques_Estilo, ubicacion, "ChequesSaldos.png")


    # Timer
    tiempoFinal = pd.to_datetime("today")
    logger.info(
        "Info Cheques Saldos"
        + "\nTiempo de Ejecucion Total: "
        + str(tiempoFinal-tiempoInicio)
    )




if __name__ == "__main__":
    chequesSaldos()