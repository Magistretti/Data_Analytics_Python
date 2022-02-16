###################################
#
#     INFORME DEVOLUCIONES YER
#               15/02/2022
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

from DatosLogin import login, googleSheet_DevolucionesYER
from Conectores import conectorMSSQL

import logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)




####################################################################
# Get YER Returns into a DF from a Google Sheet
####################################################################

def _get_df_GSheet(spreadsheetID, range):
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

    # Drop unused column
    df_gSheetData = df_gSheetData.drop(columns=["F_DOC"])

    # Cast string of dates as datetime
    df_gSheetData["F_OPER"] = pd.to_datetime(
        df_gSheetData["F_OPER"]
        , dayfirst=True # Specify that strings are in the ddmmyyyy format
    )

    df_gSheetData = df_gSheetData.convert_dtypes()
    
    # Get returns of the month
    this_month = (pd.to_datetime("today").to_period("M") - 0).to_timestamp("M")
    prev_month = (pd.to_datetime("today").to_period("M") - 1).to_timestamp("M")

    df_checkData = df_gSheetData[
        (df_gSheetData["F_OPER"] > prev_month)
        & (df_gSheetData["F_OPER"] <= this_month)
    ].copy() # .copy() will avoid raising "SettingWithCopyWarning"

    # In case of empty values ("") or hyphen ("-"), replace them with zero
    # or NA
    df_checkData.replace(
        {
            "VOL": {"": 0, "-": 0}
            ,"UEN": {"": pd.NA}
        }, inplace=True
    )
    
    # Fill NaN with zero in case of missing data
    df_checkData.fillna({"VOL": 0}, inplace=True)

    # Check page and fill null accordingly
    if range == "P1!A:E":
        df_checkData.fillna({"UEN": "PERDRIEL"}, inplace=True)
    elif range == "SJ!A:E":
        df_checkData.fillna({"UEN": "SAN JOSE"}, inplace=True)
    elif range == "PO!A:E":
        df_checkData.fillna({"UEN": "PUENTE OLIVE"}, inplace=True)
    elif range == "P2!A:E":
        df_checkData.fillna({"UEN": "PERDRIEL2"}, inplace=True)
    elif range == "LM!A:E":
        df_checkData.fillna({"UEN": "LAMADRID"}, inplace=True)
    elif range == "AZ!A:E":
        df_checkData.fillna({"UEN": "AZCUENAGA"}, inplace=True)

    # If we have data today, use it and remove date column
    if len(df_checkData.index) > 0:
        df_gSheetData = df_checkData
        df_gSheetData = df_gSheetData.drop(columns=["F_OPER"])

    # If we dont have data today, get a DF with zeroes
    elif len(df_checkData.index) == 0:

        # Check page
        if range == "P1!A:E":
            df_zeroValues = pd.DataFrame({
                "UEN": ["PERDRIEL","PERDRIEL","PERDRIEL","PERDRIEL"]
                , "CODPROD": ["EU", "GO", "NS", "NU"]
                , "VOL": [0,0,0,0]
            })
        elif range == "SJ!A:E":
            df_zeroValues = pd.DataFrame({
                "UEN": ["SAN JOSE","SAN JOSE","SAN JOSE","SAN JOSE"]
                , "CODPROD": ["EU", "GO", "NS", "NU"]
                , "VOL": [0,0,0,0]
            })
        elif range == "PO!A:E":
            df_zeroValues = pd.DataFrame({
                "UEN": ["PUENTE OLIVE","PUENTE OLIVE","PUENTE OLIVE","PUENTE OLIVE"]
                , "CODPROD": ["EU", "GO", "NS", "NU"]
                , "VOL": [0,0,0,0]
            })
        elif range == "P2!A:E":
            df_zeroValues = pd.DataFrame({
                "UEN": ["PERDRIEL2","PERDRIEL2","PERDRIEL2","PERDRIEL2"]
                , "CODPROD": ["EU", "GO", "NS", "NU"]
                , "VOL": [0,0,0,0]
            })
        elif range == "LM!A:E":
            df_zeroValues = pd.DataFrame({
                "UEN": ["LAMADRID","LAMADRID","LAMADRID","LAMADRID"]
                , "CODPROD": ["EU", "GO", "NS", "NU"]
                , "VOL": [0,0,0,0]
            })
        elif range == "AZ!A:E":
            df_zeroValues = pd.DataFrame({
                "UEN": ["AZCUENAGA","AZCUENAGA","AZCUENAGA","AZCUENAGA"]
                , "CODPROD": ["EU", "GO", "NS", "NU"]
                , "VOL": [0,0,0,0]
            })
        

        df_gSheetData = df_zeroValues
    
    # Group by "UEN" and "CODPROD" and sum
    df_gSheetData = df_gSheetData.groupby(["UEN","CODPROD"],as_index=False).sum()
    
    # Get "TOTAL" row
    df_gSheetData.loc[df_gSheetData.index[-1]+1] = \
        df_gSheetData.sum(numeric_only=True)

    # Replace NaNs in "UEN" and "CODPROD"
    df_gSheetData.fillna({"CODPROD": "SUBTOTAL"}, inplace=True)

    # Check page and fill null accordingly
    if range == "P1!A:E":
        df_gSheetData.fillna({"UEN": "PERDRIEL"}, inplace=True)
    elif range == "SJ!A:E":
        df_gSheetData.fillna({"UEN": "SAN JOSE"}, inplace=True)
    elif range == "PO!A:E":
        df_gSheetData.fillna({"UEN": "PUENTE OLIVE"}, inplace=True)
    elif range == "P2!A:E":
        df_gSheetData.fillna({"UEN": "PERDRIEL2"}, inplace=True)
    elif range == "LM!A:E":
        df_gSheetData.fillna({"UEN": "LAMADRID"}, inplace=True)
    elif range == "AZ!A:E":
        df_gSheetData.fillna({"UEN": "AZCUENAGA"}, inplace=True)

    # Rename col "CODPROD" to "PRODUCTO"
    df_gSheetData.rename(columns={
        "CODPROD": "PRODUCTO"
        , "VOL": "VOLUMEN RV"
    }, inplace=True)

    # Make "VOLUMEN RV" a negative number
    df_gSheetData["VOLUMEN RV"] = df_gSheetData["VOLUMEN RV"] * -1

    return df_gSheetData



df_AZ = _get_df_GSheet(googleSheet_DevolucionesYER, "AZ!A:E")
df_LM = _get_df_GSheet(googleSheet_DevolucionesYER, "LM!A:E")
df_P1 = _get_df_GSheet(googleSheet_DevolucionesYER, "P1!A:E")
df_P2 = _get_df_GSheet(googleSheet_DevolucionesYER, "P2!A:E")
df_PO = _get_df_GSheet(googleSheet_DevolucionesYER, "PO!A:E")
df_SJ = _get_df_GSheet(googleSheet_DevolucionesYER, "SJ!A:E")

# Concat DFs
df_dev = pd.concat([
    df_AZ
    , df_LM
    , df_P1
    , df_P2
    , df_PO
    , df_SJ
], ignore_index=True)



####################################################################
# Get YER Sales into a DF from SQL
####################################################################

def _get_df_SQL(conexMSSQL):
    
    df = pd.read_sql(
        """
        /* Esta consulta permite extraer las ventas YER de la tabla EmpPromo 
            ordenadas por UEN y por PRODUCTO */

        --------------------------
        DECLARE @FECHA date
        SET @FECHA = getdate()

        DECLARE @FinMesActual date
        SET @FinMesActual = EOMONTH(@FECHA) --Último día mes actual

        DECLARE @FinMesAnterior date
        SET @FinMesAnterior = EOMONTH(@FECHA, -1); --Último día mes anterior
        --------------------------


        SELECT
            RTRIM([UEN]) as 'UEN'
            --,[FECHASQL]
            , RTRIM([CODPRODUCTO]) as 'PRODUCTO'
            , sum([VOLUMEN]) as 'VOLUMEN VTA'

        FROM [Rumaos].[dbo].[EmpPromo] WITH (NOLOCK)
        WHERE UEN IN (
            'AZCUENAGA'
            ,'LAMADRID'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
            ,'SAN JOSE'
        )
            AND CODPROMO IN (
                '1','2','4','5','7','8','9','10','11','12','14'
            )
            AND CODPRODUCTO <> 'GNC'
            AND FECHASQL > @FinMesAnterior --Último día mes anterior
            AND FECHASQL <= @FinMesActual --Último día mes actual

        GROUP BY UEN, CODPRODUCTO
        --ORDER BY UEN, CODPRODUCTO

        UNION ALL

        SELECT
            RTRIM([UEN]) as 'UEN'
            , 'SUBTOTAL' as 'PRODUCTO'
            , sum([VOLUMEN]) as 'VOLUMEN VTA'

        FROM [Rumaos].[dbo].[EmpPromo] WITH (NOLOCK)
        WHERE UEN IN (
            'AZCUENAGA'
            ,'LAMADRID'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
            ,'SAN JOSE'
        )
            AND CODPROMO IN (
                '1','2','4','5','7','8','9','10','11','12','14'
            )
            AND CODPRODUCTO <> 'GNC'
            AND FECHASQL > @FinMesAnterior --Último día mes anterior
            AND FECHASQL <= @FinMesActual --Último día mes actual

        GROUP BY UEN
        ORDER BY RTRIM(UEN), RTRIM(CODPRODUCTO)
        """
        , conexMSSQL
    )

    return df



df_vta = _get_df_SQL(conectorMSSQL(login))
df_vta = df_vta.convert_dtypes()

df_merge = pd.merge(
    left=df_vta
    , right=df_dev
    , how="left"
    , on=["UEN", "PRODUCTO"]
)

df_merge.fillna({"VOLUMEN RV": 0}, inplace=True)

df_merge = df_merge.convert_dtypes()

df_merge["LITROS PEND."] = df_merge["VOLUMEN VTA"] + df_merge["VOLUMEN RV"]

# Get "TOTAL" row
    # Filtering "SUBTOTAL" rows
df_total = df_merge[df_merge["PRODUCTO"] == "SUBTOTAL"].copy()
df_total.loc[df_total.index[-1]+1] = df_total.sum(numeric_only=True)
df_total = df_total.tail(1)

    # Fill NaNs in "UEN" and "PRODUCTO"
df_total.fillna({
    "UEN": ""
    , "PRODUCTO": "TOTAL"
}, inplace=True)

# Concat df_merge and df_total
df_merge_tot = pd.concat([df_merge, df_total], ignore_index=True)


print(df_merge_tot)