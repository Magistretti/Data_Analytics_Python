###################################
#
#     INFORME PASIVOS CORRIENTES
#               10/02/22
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

from DatosLogin import googleSheet_InfoKamel
# from Conectores import conectorMSSQL

import logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)



####################################################################
# Get current passives into a DF from a Google Sheet
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

    # Infer data types
    df_gSheetData = df_gSheetData.convert_dtypes()
    
    # In case of empty values ("") or hyphen ("-"), replace them with zero
    df_gSheetData.replace(
        {"IMPORTE": {"": 0, "-": 0}}
        , inplace=True
    )

    # Fill NaN with zero in case of missing data
    df_gSheetData.fillna({"IMPORTE": 0}, inplace=True)


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

def pasivosCorrientes():
    """
    This function will generate an image report of the current passives
    """

    # Timer
    tiempoInicio = pd.to_datetime("today")

    # Get DF
    df_PC = _get_df_GSheet(googleSheet_InfoKamel, "PasivosCorrientes!A:C")

    # Styling of DF
    df_PC_Estilo = _estiladorVtaTitulo(
        df_PC
        , ["IMPORTE"]
        , titulo="PASIVOS CORRIENTES"
    )

    # Files location
    ubicacion = str(pathlib.Path(__file__).parent)+"\\"

    # Get image of df
    _df_to_image(df_PC_Estilo, ubicacion, "PasivosCorrientes.png")


    # Timer
    tiempoFinal = pd.to_datetime("today")
    logger.info(
        "Info Pasivos Corrientes"
        + "\nTiempo de Ejecucion Total: "
        + str(tiempoFinal-tiempoInicio)
    )



if __name__ == "__main__":
    pasivosCorrientes()