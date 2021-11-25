###################################
#
#     INFORME Penetración RedMás Semanal
#             
#               25/11/21 - 25/11/21
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




##########################################
# Get data from SQL Server
##########################################

def _get_df(conexMSSQL):
    '''
    Get the basic dataframes required for InfoPenetracionRMSemanal.py
    args:
    conexMSSQL: required connection object to database
    '''

    df_desp_liq = pd.read_sql(
        """
            SELECT
            RTRIM(Despapro.UEN) AS 'UEN',
            COUNT(Despapro.VOLUMEN) AS 'Despachos RedMas',
            (SELECT
                COUNT(D.VOLUMEN) AS 'Despachos'
            FROM Rumaos.dbo.Despapro as D
            WHERE D.uen = Despapro.uen
                AND	D.TARJETA NOT LIKE 'cc%'
                AND D.FECHASQL >= DATEADD(DAY,-7,CAST(GETDATE() AS date))
                AND D.FECHASQL < CAST(GETDATE() AS date)
                AND D.VOLUMEN > '0'
                AND D.CODPRODUCTO <> 'GNC'
            GROUP BY D.UEN
            ) AS 'Despachos'
            FROM Rumaos.dbo.Despapro
            WHERE Despapro.TARJETA like 'i%'
                AND Despapro.FECHASQL >= DATEADD(DAY,-7,CAST(GETDATE() AS date))
                AND Despapro.FECHASQL < CAST(GETDATE() AS date)
                AND Despapro.VOLUMEN > '0'
                AND Despapro.CODPRODUCTO <> 'GNC'
            GROUP BY Despapro.UEN
        """
        , conexMSSQL
    )
    df_desp_liq = df_desp_liq.convert_dtypes()


    df_desp_GNC = pd.read_sql(
        """
            SELECT
            RTRIM(Despapro.UEN) AS 'UEN',
            COUNT(Despapro.VOLUMEN) AS 'Despachos RedMas',
            (SELECT
                COUNT(D.VOLUMEN) AS 'Despachos'
            FROM Rumaos.dbo.Despapro as D
            WHERE D.uen = Despapro.uen
                AND	D.TARJETA NOT LIKE 'cc%'
                AND D.FECHASQL >= DATEADD(DAY,-7,CAST(GETDATE() AS date))
                AND D.FECHASQL < CAST(GETDATE() AS date)
                AND D.VOLUMEN > '0'
                AND D.CODPRODUCTO = 'GNC'
            GROUP BY D.UEN
            ) AS 'Despachos'
            FROM Rumaos.dbo.Despapro
            WHERE Despapro.TARJETA like 'i%'
                AND Despapro.FECHASQL >= DATEADD(DAY,-7,CAST(GETDATE() AS date))
                AND Despapro.FECHASQL < CAST(GETDATE() AS date)
                AND Despapro.VOLUMEN > '0'
                AND Despapro.CODPRODUCTO = 'GNC'
            GROUP BY Despapro.UEN
        """
        , conexMSSQL
    )
    df_desp_GNC = df_desp_GNC.convert_dtypes()


    return df_desp_liq, df_desp_GNC



##########################################
# Function to create row "TOTAL" and column "Penetración %".
# Will sort dataframe by column "Penetración %"
##########################################

def _preparador(df:pd.DataFrame):
    '''
    This function will transform basic DFs of this module in DFs 
    ready for styling
    '''

    # Total Row and NaN replace in column "UEN"
    df.loc[df.index[-1] + 1] = df.sum(numeric_only=True)
    df.fillna({"UEN":"TOTAL"}, inplace=True)

    # Create column "Penetración %"
    df["Penetración %"] = df["Despachos RedMas"] / df["Despachos"]
    # Drop columns "Despachos RedMas" and "Despachos"
    df.drop(columns=["Despachos RedMas", "Despachos"], inplace=True)

    # Sorting DF by column "Penetración %" without affecting row "TOTAL"
    row_total = df.loc[df.index[-1]]
    df_sorted = df[df["UEN"] != "TOTAL"].sort_values(by="Penetración %")
    df_sorted = df_sorted.append(row_total)

    return df_sorted



##############
# STYLING of the dataframe
##############

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
                    ,("font-size", "14px")
                ]
            }
        ]) \
        .apply(lambda x: ["background: black" if x.name == df.index[-1] 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == df.index[-1]
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["font-size: 15px" if x.name == df.index[-1]
            else "" for i in x]
            , axis=1)


    #Gradient color for column "Penetración %" without affecting row "TOTAL"

    evitarTotales = df.index.get_level_values(0) # Get list of row names (index)
    resultado = resultado.background_gradient(
        cmap="RdYlGn" # Red->Yellow->Green
        ,vmin=0
        ,vmax=1
        ,subset=pd.IndexSlice[
            evitarTotales[:-1] # Get all the rows except the last one
            , "Penetración %" # of this column
        ]
    )

    
    return resultado



##############
# PRINTING dataframe as an image
##############

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


##############
# MERGING images
##############

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




##############
# FUNCTION TO RUN MODULE
##############

def penetracionRMSemanal():
    '''
    Create several image ".png" of last week percentage of use of RedMás card
    for liquid fuel and GNC fuel
    '''

    # Timer
    tiempoInicio = pd.to_datetime("today")

    # Get Connection to database
    conexMSSQL = conectorMSSQL(login)

    # Get DFs
    df_liq, df_GNC = _get_df(conexMSSQL)

    # Clean and transform DFs
    df_liq_listo = _preparador(df_liq)
    df_GNC_listo = _preparador(df_GNC)

    percCols = ["Penetración %"]

    df_liq_Estilo = _estiladorVtaTitulo(
        df_liq_listo
        , list_Col_Perc= percCols
        , titulo="REDMÁS LÍQUIDOS"
    )
    df_GNC_Estilo = _estiladorVtaTitulo(
        df_GNC_listo
        , list_Col_Perc= percCols
        , titulo="REDMÁS GNC"
    )

    # Path and name for DF images
    ubicacion = str(pathlib.Path(__file__).parent)+"\\"
    img_liq = "Penetracion_Liquido_Semanal.png"
    img_GNC = "Penetracion_GNC_Semanal.png"

    _df_to_image(df_liq_Estilo, ubicacion, img_liq)
    _df_to_image(df_GNC_Estilo, ubicacion, img_GNC)

    listaImg = [ubicacion + img_liq, ubicacion + img_GNC]

    # Merge DFs images horizontally and save it as a .png
    fusionImg = _append_images(listaImg, direction="horizontal")
    fusionImg.save(ubicacion + "Info_Penetración_Semanal.png")

    # Timer
    tiempoFinal = pd.to_datetime("today")
    logger.info(
        "\nInfo Penetración RedMás Semanal"
        + "\nTiempo de Ejecucion Total: "
        + str(tiempoFinal-tiempoInicio)
    )




if __name__ == "__main__":
    penetracionRMSemanal()