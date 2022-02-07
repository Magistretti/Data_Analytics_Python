###################################
#
#     INFORME ActivosCorrientes
#               04/02/22
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

from DatosLogin import login, loginSGFin, googleSheet_InfoKamel
from Conectores import conectorMSSQL

import logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)



####################################################################
# Get pesos stock of each treasury into a DF from sql
####################################################################
def _get_df_pesos(conexSGFin):
    df_pesos = pd.read_sql(
        """
        --------------------------
        DECLARE @FECHA NVARCHAR(20)
        SET @FECHA = '20220203';
        --------------------------


        -- Saldo Final TOTAL de los arqueos de la fecha
        SELECT
            'STOCK EFECTIVO UENS PESOS' as 'ACTIVOS CORRIENTES'
            ,ISNULL(ROUND(SUM(arq.[FondoReal]),0),0) as 'Saldo Final'
        FROM [Sgfin].[dbo].[SGFIN_Arqueo] as arq

        LEFT OUTER JOIN dbo.[SGFIN_Box] as box
            ON arq.IdBox = box.Id

        where box.UEN not IN ('RENDICIONES')
            AND cast(arq.Fecha as date) = @FECHA
        group by cast(arq.[Fecha] as date)
        """
        , conexSGFin
    )

    return df_pesos



# df = _get_df_pesos(conectorMSSQL(loginSGFin))

# print(df)




####################################################################
# Get dolar stock of each treasury into a DF from a Google Sheet
####################################################################

def _get_df_dolar(spreadsheetID, range):
    """
    Will read the selected range of a sheet from GoogleSheet and will return
    a dataframe. NOTE: dates will be imported as formatted strings and should
    be transformed accordingly.
    ARGS: \\
    spreadsheetID: can be obtained from the share link. Example: 
    https://docs.google.com/spreadsheets/d/<SpreadSheetID>/edit?usp=sharing \\
    range: range of a sheet to read in A1 notation. Example: "Dólar!A:E"
    """

    # Scopes will limit what we can do with the sheet
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly'] # Read Only
    SERVICE_ACCOUNT_FILE = \
        str(pathlib.Path(__file__).parent.parent) + "\\quickstart.json"

    # Credentials and service for the Sheets API
    creds = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    service = build('sheets', 'v4', credentials=creds, cache_discovery=False)

    # Call the Sheets API
    sheet = service.spreadsheets()


    request = sheet.values().get(
        spreadsheetId=spreadsheetID # Spreadsheet ID
        , range=range
            # # valueRenderOption default to "FORMATTED_VALUE", it get strings
        , valueRenderOption="UNFORMATTED_VALUE" # Will get numbers like numbers
            # # dateTimeRenderOption default to "SERIAL_NUMBER" unless 
            # # valueRenderOption is "FORMATTED_VALUE"
        , dateTimeRenderOption="FORMATTED_STRING" # Will get dates as string
    )

    # Run the request
    response = request.execute()

    # Get the values of the sheet from the Json. This will be a list of lists
    response = response.get("values")
    
    # Transform response into a DF, use the first row has header
    df_gSheetData = pd.DataFrame(
        response[1:] # Row values
        , columns=response[0] # Headers
    )

    # Cast string of dates as datetime
    df_gSheetData["Fecha"] = pd.to_datetime(
        df_gSheetData["Fecha"]
        , dayfirst=True # Specify that strings are in the ddmmyyyy format
    )

    df_gSheetData = df_gSheetData.convert_dtypes()

    # Get stock of today, today date is normalized to reset time part of date
    df_checkData = df_gSheetData[
        df_gSheetData["Fecha"] == pd.to_datetime("today").normalize()
    ].copy() # .copy() will avoid raising "SettingWithCopyWarning"

    # In case of empty values ("") or hyphen ("-"), replace them with zero
    df_checkData.replace(
        {
            "Dólares": {"": 0, "-": 0}
            , "Dólares Pesificados": {"": 0, "-": 0}
            , "Tipo de Cambio": {"": 1, "-": 1}
        }
        , inplace=True
    )
    
    # Fill NaN with zero in case of missing data
    df_checkData.fillna({
        "Dólares": 0
        , "Dólares Pesificados": 0
        , "Tipo de Cambio": 1
    }, inplace=True)

    # If we have data today, use today data and remove date column
    if len(df_checkData.index) > 0:
        df_gSheetData = df_checkData
        df_gSheetData = df_gSheetData.drop(columns=["Fecha"])

    # If we dont have data today, get a DF with zeroes
    elif len(df_checkData.index) == 0:
        df_zeroValues = pd.DataFrame({
            "UEN": df_gSheetData["UEN"].unique()
            , "Dólares": 0
            , "Dólares Pesificados": 0
            , "Tipo de Cambio": 1
        })
        df_gSheetData = df_zeroValues
        

    # Cast "Tipo de Cambio" column as string to avoid total
    df_gSheetData = df_gSheetData.astype({"Tipo de Cambio": "string"})

    # Get "TOTAL" row
    df_gSheetData.loc[df_gSheetData.index[-1]+1] = \
        df_gSheetData.sum(numeric_only=True)

    ##########################
    # Dropping "Tipo de Cambio" until someone register the data
    df_gSheetData = df_gSheetData.drop(columns=["Tipo de Cambio"])
    ##########################

    # Rename the NA
    df_gSheetData.fillna({
        "UEN":"TOTAL"
        #, "Tipo de Cambio":""
    }, inplace=True)


    
    return df_gSheetData



# df = _get_df_dolar(googleSheet_InfoKamel, "Dólar!A:E")

# df_dolar = df[["Dólares Pesificados"]].loc[df.index[-1]]

# print(df_dolar)



####################################################################
# Get state of banks in a df from a Google Sheet
####################################################################

def _get_df_bank(spreadsheetID, range):
    """
    Will read the selected range of a sheet from GoogleSheet and will return
    a dataframe. NOTE: dates will be imported as formatted strings and should
    be transformed accordingly.
    ARGS: \\
    spreadsheetID: can be obtained from the share link. Example: 
    https://docs.google.com/spreadsheets/d/<SpreadSheetID>/edit?usp=sharing \\
    range: range of a sheet to read in A1 notation. Example: "Dólar!A:E"
    """

    # Scopes will limit what we can do with the sheet
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly'] # Read Only
    SERVICE_ACCOUNT_FILE = \
        str(pathlib.Path(__file__).parent.parent) + "\\quickstart.json"

    # Credentials and service for the Sheets API
    creds = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    service = build('sheets', 'v4', credentials=creds, cache_discovery=False)

    # Call the Sheets API
    sheet = service.spreadsheets()


    request = sheet.values().get(
        spreadsheetId=spreadsheetID # Spreadsheet ID
        , range=range
            # # valueRenderOption default to "FORMATTED_VALUE", it get strings
        , valueRenderOption="UNFORMATTED_VALUE" # Will get numbers like numbers
            # # dateTimeRenderOption default to "SERIAL_NUMBER" unless 
            # # valueRenderOption is "FORMATTED_VALUE"
        , dateTimeRenderOption="FORMATTED_STRING" # Will get dates as string
    )

    # Run the request
    response = request.execute()

    # Get the values of the sheet from the Json. This will be a list of lists
    response = response.get("values")
    
    # Transform response into a DF, use the first row has header
    df_gSheetData = pd.DataFrame(
        response[1:] # Row values
        , columns=response[0] # Headers
    )

    # Cast string of dates as datetime
    df_gSheetData["Fecha"] = pd.to_datetime(
        df_gSheetData["Fecha"]
        , dayfirst=True # Specify that strings are in the ddmmyyyy format
    )

    df_gSheetData = df_gSheetData.convert_dtypes()
    
    # Get stock of today, today date is normalized to reset time part of date
    df_checkData = df_gSheetData[
        df_gSheetData["Fecha"] == pd.to_datetime("today").normalize()
    ].copy() # .copy() will avoid raising "SettingWithCopyWarning"

    # In case of empty values ("") or hyphen ("-"), replace them with zero
    df_checkData.replace(
        {
            "Saldo Inicial": {"": 0, "-": 0}
            , "Ingresos": {"": 0, "-": 0}
            , "Egresos": {"": 0, "-": 0}
            , "Saldo Final": {"": 0, "-": 0}
        }
        , inplace=True
    )

    # Fill NaN with zero in case of missing data
    df_checkData.fillna({
        "Saldo Inicial": 0
        , "Ingresos": 0
        , "Egresos": 0
        , "Saldo Final": 0
    }, inplace=True)

    # If we have data today, use today data and remove date column
    if len(df_checkData.index) > 0:
        df_gSheetData = df_checkData
        df_gSheetData = df_gSheetData.drop(columns=["Fecha"])

    # If we dont have data today, get a DF with zeroes
    elif len(df_checkData.index) == 0:
        df_zeroValues = pd.DataFrame({
            "Bancos": ["BBVA", "Santander", "SPV"]
            , "Saldo Inicial": [0, 0, 0]
            , "Ingresos": [0, 0, 0]
            , "Egresos": [0, 0, 0]
            , "Saldo Final": [0, 0, 0]
        })
        df_gSheetData = df_zeroValues

    df_gSheetData.loc[df_gSheetData.index[-1]+1] = \
        df_gSheetData.sum(numeric_only=True)
    
    # Naming blank in last row as "SALDO BANCOS"
    df_gSheetData.fillna({"Bancos": "SALDO BANCOS"}, inplace=True)

    # Getting last row of column "Saldo Final"
    df_gSheetData = df_gSheetData[["Saldo Final"]].loc[df_gSheetData.index[-1]]

    return df_gSheetData



# df = _get_df_bank(googleSheet_InfoKamel, "Bancos!A:F")

# print(df)