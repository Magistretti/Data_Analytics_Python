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
        DECLARE @FECHA as date
        SET @FECHA = GETDATE();
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

    ##########################
    # Dropping "Tipo de Cambio" until someone register the data
    df_gSheetData = df_gSheetData.drop(columns=["Tipo de Cambio"])
    ##########################


    # Prepare DF for report, use a copy to avoid spilling to df_gSheetData
    df_stockUSD = df_gSheetData.copy()

    # Get "TOTAL" row
    df_stockUSD.loc[df_stockUSD.index[-1]+1] = \
        df_stockUSD.sum(numeric_only=True)
    # Rename the NA
    df_stockUSD.fillna({
        "UEN":"TOTAL"
        #, "Tipo de Cambio":""
    }, inplace=True)
    # Add column "ACTIVOS CORRIENTES"
    df_stockUSD.insert(0, "ACTIVOS CORRIENTES", "STOCK EFECTIVO UENS DOLARES")
    # Select useful columns
    df_stockUSD = df_stockUSD[["ACTIVOS CORRIENTES", "Dólares Pesificados"]]
    # Rename column "Dólares Pesificados"
    df_stockUSD.rename(columns={"Dólares Pesificados": "Saldo Final"}
        , inplace=True
    )
    # Select "TOTAL" row
    df_stockUSD = df_stockUSD.tail(1)


    # Get value to subtract from df_pesos to avoid duplicated stock value in
    # treasuries not named "SAN JOSE"
    df_resta = df_gSheetData[df_gSheetData["UEN"] != "SAN JOSE"].copy()
    
    # Get "TOTAL" row
    df_resta.loc[df_resta.index[-1]+1] = \
        df_resta.sum(numeric_only=True)
    # Select useful value at last place of column "Dólares Pesificados"
    v_resta = df_resta.at[df_resta.index[-1], "Dólares Pesificados"]
    
    
    return df_stockUSD, v_resta




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

    # Insert column "Activos Corrientes"
    df_gSheetData.insert(0,"ACTIVOS CORRIENTES", "SALDO EN BANCOS")

    # Filtering columns
    df_gSheetData = df_gSheetData[["ACTIVOS CORRIENTES", "Saldo Final"]]

    # Getting last row
    df_gSheetData = df_gSheetData.tail(1)

    return df_gSheetData




####################################################################
# Get ending balance of checks wallet in a df from a SQL query
####################################################################

def _get_df_checks(conexSGFin):
    df_cheque = pd.read_sql(
        """
        -- SALDO FINAL DE CHEQUES FISICOS EN CARTERA

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
            'CHEQUES EN CARTERA' as 'ACTIVOS CORRIENTES'
                    
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
        , conexSGFin
    )

    return df_cheque



####################################################################
# Get ECheq stock into a DF from a Google Sheet
####################################################################

def _get_df_Echecks(spreadsheetID, range):
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
        {"Saldo Final": {"": 0, "-": 0}}
        , inplace=True
    )

    # Fill NaN with zero in case of missing data
    df_checkData.fillna(
        {
            "UEN": "ECheq"
            ,"Saldo Final": 0
        }, inplace=True
    )

    # If we have data today, use today data and remove date column
    if len(df_checkData.index) > 0:
        df_gSheetData = df_checkData
        df_gSheetData = df_gSheetData.drop(columns=["Fecha"])

    # If we dont have data today, get a DF with zeroes
    elif len(df_checkData.index) == 0:
        df_zeroValues = pd.DataFrame({
            "UEN": ["ECheq"]
            , "Saldo Final": [0]
        })
        df_gSheetData = df_zeroValues

    
    df_gSheetData.insert(0, "ACTIVOS CORRIENTES", "ECHEQS EN CARTERA")
    df_gSheetData = df_gSheetData.drop(columns="UEN")
        

    return df_gSheetData



####################################################################
# Get total debt of debtors in a df from a SQL query
####################################################################

def _get_df_debt(conexMSSQL):
    df_deudores = pd.read_sql(
        """
        SELECT

        'CUENTAS CORRIENTES POR COBRAR' as 'ACTIVOS CORRIENTES'
        ,-ROUND(SUM(Cli.SALDOPREPAGO - Cli.SALDOREMIPENDFACTU),0) as 'Saldo Final'

        FROM dbo.FacCli as Cli with (NOLOCK)

        WHERE Cli.NROCLIPRO > '100000'
            AND (Cli.SALDOPREPAGO - Cli.SALDOREMIPENDFACTU) < -1000
            and Cli.ListaSaldoCC = 1
        """
        , conexMSSQL
    )

    return df_deudores



####################################################################
# Get cards pending crediting to a DF from a Google Sheet
####################################################################

def _get_df_cards(spreadsheetID, range):
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
            "ACTIVOS CORRIENTES": {"": "TARJETAS POR ACREDITAR 24 HS"}
            ,"Saldo Final": {"": 0, "-": 0}
        }, inplace=True
    )

    # Fill NaN with zero in case of missing data
    df_checkData.fillna(
        {
            "ACTIVOS CORRIENTES": "TARJETAS POR ACREDITAR 24 HS"
            ,"Saldo Final": 0
        }, inplace=True
    )

    # If we have data today, use today data and remove date column
    if len(df_checkData.index) > 0:
        df_gSheetData = df_checkData
        df_gSheetData = df_gSheetData.drop(columns=["Fecha"])

    # If we dont have data today, get a DF with zeroes
    elif len(df_checkData.index) == 0:
        df_zeroValues = pd.DataFrame({
            "ACTIVOS CORRIENTES": ["TARJETAS POR ACREDITAR 24 HS"]
            , "Saldo Final": [0]
        })
        df_gSheetData = df_zeroValues

    
    # df_gSheetData.insert(0, "ACTIVOS CORRIENTES", "ECHEQS EN CARTERA")
    # df_gSheetData = df_gSheetData.drop(columns="UEN")
        

    return df_gSheetData




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

def activosCorrientes():
    """
    This function will generate an image report of current assets
    """

    # Timer
    tiempoInicio = pd.to_datetime("today")

    # Connections to DBs
    conexCentral = conectorMSSQL(login)
    conexSGFin = conectorMSSQL(loginSGFin)

    # Get DFs
    df_pesos = _get_df_pesos(conexSGFin)
    df_dolar, v_resta = _get_df_dolar(googleSheet_InfoKamel, "Dólar!A:E")
    df_bancos = _get_df_bank(googleSheet_InfoKamel, "Bancos!A:F")
    df_cards = _get_df_cards(googleSheet_InfoKamel,"Tarjetas!A:C")
    df_debt = _get_df_debt(conexCentral)
    df_checks = _get_df_checks(conexSGFin)
    df_Echecks = _get_df_Echecks(googleSheet_InfoKamel,"ECheq!A:C")
    
    # Remove dolars from df_pesos
    df_pesos = df_pesos.set_index("ACTIVOS CORRIENTES")
    df_pesos = df_pesos - v_resta
    df_pesos = df_pesos.reset_index()
    
    # Concatenate the DFs
    df_union = pd.concat([
        df_pesos
        , df_dolar
        , df_bancos
        , df_cards
        , df_debt
        , df_checks
        , df_Echecks
    ])

    # Reset index
    df_union.reset_index(drop=True, inplace=True)

    # Infer data types to avoid sum error
    df_union = df_union.convert_dtypes()
    
    # Get "TOTAL" row
    df_union.loc[df_union.index[-1]+1] = df_union.sum(numeric_only=True)

    # Fill the NA with the word "TOTAL" to identify the row
    df_union.fillna({"ACTIVOS CORRIENTES": "TOTAL"}, inplace=True)
    
    # Styling dataframe
    df_union_Estilo = _estiladorVtaTitulo(
        df_union
        , list_Col_Num=["Saldo Final"]
        , titulo= "ACTIVOS CORRIENTES"
    )

    # Files location
    ubicacion = str(pathlib.Path(__file__).parent)+"\\"

    # Get image of DF
    _df_to_image(df_union_Estilo, ubicacion, "activosCorrientes.png")


    # Timer
    tiempoFinal = pd.to_datetime("today")
    logger.info(
        "Info Cheques Saldos"
        + "\nTiempo de Ejecucion Total: "
        + str(tiempoFinal-tiempoInicio)
    )



if __name__ == "__main__":
    activosCorrientes()