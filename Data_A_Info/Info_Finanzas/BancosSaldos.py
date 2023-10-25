###################################
#
#     INFORME BANCOS SALDOS 
#               31/12/21
###################################

import os
import sys
import pathlib

# Allow imports from the top folder
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))

import pandas as pd
import dataframe_image as dfi
from googleapiclient.discovery import build
from google.oauth2 import service_account

from DatosLogin import loginSGFin, googleSheet_InfoKamel
from Conectores import conectorMSSQL

import logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)



####################################################################
# Get bank balances in a df from SQL Server
####################################################################

def _get_df(conexSGFin):
    
    df_bancos = pd.read_sql(
        """
        --------------------------
        DECLARE @FECHA date
        SET @FECHA = CAST(getdate() as date);
        --------------------------


        WITH Saldo_Ini as (
            SELECT
                nombre.Nombre

                /* Saldo Inicial será la sumatoria de todos los movimientos desde el 01/02/22
                al día anterior bajo estudio. A la sumatoria de importes se le agrega el valor
                de las cuentas al momento 0 que sería el saldo al 31/01/22 */

                , ROUND(CASE
                    WHEN nombre.Nombre = 'BANCO NACION' THEN (sum([Importe]))
                    WHEN nombre.Nombre = 'BBVA FRANCES' THEN (sum([Importe]))
                    WHEN nombre.Nombre = 'MERCADO PAGO' THEN (sum([Importe]))
                    WHEN nombre.Nombre = 'Santander Rio' THEN (sum([Importe]))
                    ELSE sum([Importe])
                END, 0) as 'Saldo Inicial'
            
            FROM [Sgfin].[dbo].[SGFIN_TRANSFERENCIABANCARIADETALLE] as banco

            JOIN Sgfin.dbo.SGFIN_TRANSFERENCIABANCARIA as id
                on banco.IdTransferenciaBancaria = id.Id
            JOIN  Sgfin.dbo.SGFIN_Banco as nombre
                on id.IdBanco = nombre.Id

            WHERE banco.Fecha > '20000131'
                AND banco.Fecha < @FECHA
                AND id.IdBanco <> 71

            GROUP BY nombre.Nombre
        )

        , Ingresos as (
            SELECT
                nombre.Nombre
                , ISNULL(ROUND(sum([Importe]), 0), 0) as INGRESOS

            FROM [Sgfin].[dbo].[SGFIN_TRANSFERENCIABANCARIADETALLE] as banco

            JOIN Sgfin.dbo.SGFIN_TRANSFERENCIABANCARIA as id
                on banco.IdTransferenciaBancaria = id.Id
            JOIN  Sgfin.dbo.SGFIN_Banco as nombre
                on id.IdBanco = nombre.Id

            WHERE banco.Fecha = @FECHA
                AND id.IdBanco <> 71
                AND banco.Importe > 0

            GROUP BY nombre.Nombre
        )

        , Egresos as (
            SELECT
                nombre.Nombre
                , ISNULL(ROUND(sum([Importe]), 0), 0) as EGRESOS
            
            FROM [Sgfin].[dbo].[SGFIN_TRANSFERENCIABANCARIADETALLE] as banco

            JOIN Sgfin.dbo.SGFIN_TRANSFERENCIABANCARIA as id
                on banco.IdTransferenciaBancaria = id.Id
            JOIN  Sgfin.dbo.SGFIN_Banco as nombre
                on id.IdBanco = nombre.Id

            WHERE banco.Fecha = @FECHA
                AND id.IdBanco <> 71
                AND banco.Importe < 0

            GROUP BY nombre.Nombre
        )

        Select
            Saldo_Ini.Nombre
            , Saldo_Ini.[Saldo Inicial]
            , ISNULL(Ingresos.INGRESOS, 0) as 'Ingresos'
            , ISNULL(Egresos.EGRESOS, 0) as 'Egresos'
            , ROUND((Saldo_Ini.[Saldo Inicial]
                + ISNULL(Ingresos.INGRESOS, 0)
                + ISNULL(Egresos.EGRESOS, 0)
            ), 0) as 'Saldo Final'

        FROM Saldo_Ini

        LEFT OUTER JOIN Ingresos
            ON Saldo_Ini.Nombre = Ingresos.Nombre
        LEFT OUTER JOIN Egresos
            ON Saldo_Ini.Nombre = Egresos.Nombre

        ORDER BY Nombre
        """
        , conexSGFin
    )

    
    return df_bancos



# ####################################################################
# # Get state of banks in a df from a Google Sheet
# ####################################################################

# def _get_df_GSheet(spreadsheetID, range):
#     """
#     Will read the selected range of a sheet from GoogleSheet and will return
#     a dataframe. NOTE: dates will be imported as formatted strings and should
#     be transformed accordingly.
#     ARGS: \\
#     spreadsheetID: can be obtained from the share link. Example: 
#     https://docs.google.com/spreadsheets/d/<SpreadSheetID>/edit?usp=sharing \\
#     range: range of a sheet to read in A1 notation. Example: "Dólar!A:E"
#     """

#     # Scopes will limit what we can do with the sheet
#     SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly'] # Read Only
#     SERVICE_ACCOUNT_FILE = \
#         str(pathlib.Path(__file__).parent.parent) + "\\quickstart.json"

#     # Credentials and service for the Sheets API
#     creds = service_account.Credentials.from_service_account_file(
#                 SERVICE_ACCOUNT_FILE, scopes=SCOPES)

#     service = build('sheets', 'v4', credentials=creds, cache_discovery=False)

#     # Call the Sheets API
#     sheet = service.spreadsheets()


#     request = sheet.values().get(
#         spreadsheetId=spreadsheetID # Spreadsheet ID
#         , range=range
#             # # valueRenderOption default to "FORMATTED_VALUE", it get strings
#         , valueRenderOption="UNFORMATTED_VALUE" # Will get numbers like numbers
#             # # dateTimeRenderOption default to "SERIAL_NUMBER" unless 
#             # # valueRenderOption is "FORMATTED_VALUE"
#         , dateTimeRenderOption="FORMATTED_STRING" # Will get dates as string
#     )

#     # Run the request
#     response = request.execute()

#     # Get the values of the sheet from the Json. This will be a list of lists
#     response = response.get("values")
    
#     # Transform response into a DF, use the first row has header
#     df_gSheetData = pd.DataFrame(
#         response[1:] # Row values
#         , columns=response[0] # Headers
#     )

#     # Cast string of dates as datetime
#     df_gSheetData["Fecha"] = pd.to_datetime(
#         df_gSheetData["Fecha"]
#         , dayfirst=True # Specify that strings are in the ddmmyyyy format
#     )

#     df_gSheetData = df_gSheetData.convert_dtypes()
    
#     # Get stock of today, today date is normalized to reset time part of date
#     df_checkData = df_gSheetData[
#         df_gSheetData["Fecha"] == pd.to_datetime("today").normalize()
#     ].copy() # .copy() will avoid raising "SettingWithCopyWarning"

#     # In case of empty values ("") or hyphen ("-"), replace them with zero
#     df_checkData.replace(
#         {
#             "Saldo Inicial": {"": 0, "-": 0}
#             , "Ingresos": {"": 0, "-": 0}
#             , "Egresos": {"": 0, "-": 0}
#             , "Saldo Final": {"": 0, "-": 0}
#         }
#         , inplace=True
#     )

#     # Fill NaN with zero in case of missing data
#     df_checkData.fillna({
#         "Saldo Inicial": 0
#         , "Ingresos": 0
#         , "Egresos": 0
#         , "Saldo Final": 0
#     }, inplace=True)

#     # If we have data today, use today data and remove date column
#     if len(df_checkData.index) > 0:
#         df_gSheetData = df_checkData
#         df_gSheetData = df_gSheetData.drop(columns=["Fecha"])

#     # If we dont have data today, get a DF with zeroes
#     elif len(df_checkData.index) == 0:
#         df_zeroValues = pd.DataFrame({
#             "Bancos": ["BBVA", "Santander", "SPV"]
#             , "Saldo Inicial": [0, 0, 0]
#             , "Ingresos": [0, 0, 0]
#             , "Egresos": [0, 0, 0]
#             , "Saldo Final": [0, 0, 0]
#         })
#         df_gSheetData = df_zeroValues
        


#     return df_gSheetData


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

def bancosSaldos():
    """
    This function will generate an image report of the actual state of the
    banks according to data in a Google Sheet
    """
    
    # Timer
    tiempoInicio = pd.to_datetime("today")

    # # Get DF GSheet
    # df_bancos = _get_df_GSheet(googleSheet_InfoKamel, "Bancos!A:F")

    # Get DF from SQL
    df_bancos = _get_df(conectorMSSQL(loginSGFin))

    # Get "TOTAL" row
    df_bancos.loc[df_bancos.index[-1]+1] = df_bancos.sum(numeric_only=True)

    # Renaming NA in "Bancos" column to "TOTAL"
    df_bancos.fillna({"Nombre": "TOTAL"}, inplace=True)

    # Styling of DF
    df_bancos_Estilo = _estiladorVtaTitulo(
        df_bancos
        , [
            "Saldo Inicial"
            , "Ingresos"
            , "Egresos"
            , "Saldo Final"
        ], titulo="BANCOS (SALDOS)"
    )

    # Files location
    ubicacion = str(pathlib.Path(__file__).parent)+"\\"

    # Get image of df_arqueos_Estilo
    _df_to_image(df_bancos_Estilo, ubicacion, "BancosSaldos.png")


    # Timer
    tiempoFinal = pd.to_datetime("today")
    logger.info(
        "Info Bancos Saldos"
        + "\nTiempo de Ejecucion Total: "
        + str(tiempoFinal-tiempoInicio)
    )




if __name__ == "__main__":
    bancosSaldos()