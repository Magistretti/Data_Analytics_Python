###################################
#
#     INFORME Lubricantes Ayer
#             
#               05/11/21
###################################

import os
import sys
import pathlib

# Allow imports from the top folder
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))

import pandas as pd
import dataframe_image as dfi

from DatosLogin import login
from Conectores import conectorMSSQL


def datosLubri(conexMSSQL):

    ########################################
    # Get yesterday sales of lubricants by code
    ########################################

    df_lubri = pd.read_sql(
        '''
            SELECT
                [CODPRODUCTO] as 'CODIGO'
                ,CAST(sum(-[CANTIDAD]) AS smallint) as 'UNIDADES'
            FROM [Rumaos].[dbo].[VMovDet]
            WHERE VMovDet.TIPOMOVIM = '3'
                AND VMovDet.CODPRODUCTO BETWEEN 100 AND 5000
                --Filtramos azul32 y agua destilada
                AND VMovDet.CODPRODUCTO NOT IN ('1027','1146', '1175', '2000')
                AND VMovDet.FECHASQL = DATEADD(DAY,-1,CAST(GETDATE() AS date))
            GROUP BY VMovDet.CODPRODUCTO
        '''
        , conexMSSQL)
    df_lubri = df_lubri.convert_dtypes()
    df_lubri["CODIGO"] = df_lubri["CODIGO"].str.strip()


    ########################################
    # Aux table of lubricant liters per item
    ########################################

    ubicacion = str(pathlib.Path(__file__).parent)+"\\"
    aux_lubri = "Auxiliar_Objetivos_y_Envases.xlsx"

    df_lts_x_envase = pd.read_excel(
        ubicacion + aux_lubri
        , "Lts_x_Envase"
        , dtype={"CODIGO": str}
    )
    df_lts_x_envase.convert_dtypes()
    df_lts_x_envase["CODIGO"] = df_lts_x_envase["CODIGO"].str.strip()


    ########################################
    # Merge df_lubri and df_lts_x_envase
    ########################################

    df_lubri_lts = pd.merge(df_lubri, df_lts_x_envase, how="left")
    # Filter cans with less than a liter
    df_lubri_lts = df_lubri_lts[df_lubri_lts["Lts_x_Envase"] >= 1]
    # Create column "LITROS"
    df_lubri_lts["LITROS"] = df_lubri_lts["UNIDADES"] * df_lubri_lts["Lts_x_Envase"]
    # Filter unused columns
    df_lubri_lts = df_lubri_lts[["Lts_x_Envase","LITROS"]]
    # Group by capacity of cans
    df_lubri_lts = df_lubri_lts.groupby(by=["Lts_x_Envase"], as_index=False).sum()
    # Cast as integer to avoid trailing zeroes
    df_lubri_lts = df_lubri_lts.astype({"Lts_x_Envase": "int", "LITROS": "int"})
    # Rename column "Ltx_x_Envase" as "ENVASE (lts)"
    df_lubri_lts = df_lubri_lts.rename(columns={"Lts_x_Envase": "ENVASE (lts)"})


    ########################################
    # Aux table of sales targets 
    ########################################

    df_objetivos = pd.read_excel(
        ubicacion + aux_lubri
        , "Objetivos_x_Envase"
        , dtype={"CODIGO": str}
    )


    ########################################
    # Merge df_lubri_lts and df_objetivos
    ########################################

    df_lubri_lts_obj = pd.merge(df_lubri_lts, df_objetivos)
    # Create column "CUMPLIMIENTO"
    df_lubri_lts_obj["CUMPLIMIENTO"] = \
        (df_lubri_lts_obj["LITROS"] / df_lubri_lts_obj["OBJETIVO (lts)"])

    return df_lubri_lts_obj


########################################
# STYLING of the dataframe
########################################

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
        .set_caption(titulo
            +"\n"
            +((pd.to_datetime("today")-pd.to_timedelta(1,"days"))
            .strftime("%d/%m/%y"))
        ) \
        .set_properties(subset=list_Col_Num + list_Col_Perc
            , **{"text-align": "center", "width": "50px"}) \
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
        .apply(lambda x: ["background: black" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1)

    
    return resultado


########################################
# PRINTING dataframe as an image
########################################

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




########################################
# FUNCTION TO RUN MODULE
########################################

def ventaLubri():
    '''
    This function will create a .png file at the module 
    folder ("Info_VentaLubri") and will display total time elapsed
    '''
    # Timer
    tiempoInicio = pd.to_datetime("today")

    conexMSSQL = conectorMSSQL(login)

    df_lubri_lts_obj_Estilo = _estiladorVtaTitulo(
        datosLubri(conexMSSQL)
        , ["ENVASE (lts)", "LITROS", "OBJETIVO (lts)"]
        , ["CUMPLIMIENTO"]
        , "Venta de Lubricante"
    )

    ubicacion = str(pathlib.Path(__file__).parent)+"\\"
    nombreIMG = "Info_VentaLubri.png"

    _df_to_image(df_lubri_lts_obj_Estilo, ubicacion, nombreIMG)

    # Timer
    tiempoFinal = pd.to_datetime("today")
    print("\nInfo Venta Lubricantes"+"\nTiempo de Ejecucion Total:")
    print(tiempoFinal-tiempoInicio)




if __name__ == "__main__":
    ventaLubri()