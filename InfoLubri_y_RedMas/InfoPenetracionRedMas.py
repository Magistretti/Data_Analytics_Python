###################################
#
#     INFORME Penetración RedMás AYER
#             
#               07/10/21
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


def penetracion_liq(conexMSSQL):

    ##########################################
    # Get yesterday "PENETRACIÓN" for "Líquidos"
    ##########################################

    df_desp_x_turno_liq = pd.read_sql(
        """
            SELECT
                Despapro.UEN,
                Despapro.TURNO,
                (SELECT
                    COUNT(D.VOLUMEN) AS 'Despachos RedMas'
                FROM Rumaos.dbo.Despapro as D
                WHERE D.uen = Despapro.uen
                    AND D.TURNO = Despapro.TURNO
                    AND	D.TARJETA like 'i%'
                    AND D.FECHASQL = DATEADD(DAY,-1,CAST(GETDATE() AS date))
                    AND D.VOLUMEN > '0'
                    AND D.CODPRODUCTO <> 'GNC'
                GROUP BY D.UEN ,D.TURNO
                ) AS 'Despachos RedMas',
                COUNT(Despapro.VOLUMEN) AS 'Despachos'
            FROM Rumaos.dbo.Despapro
            WHERE Despapro.TARJETA NOT LIKE 'cc%'
                AND Despapro.FECHASQL = DATEADD(DAY,-1,CAST(GETDATE() AS date))
                AND Despapro.VOLUMEN > '0'
                AND Despapro.CODPRODUCTO <> 'GNC'
            GROUP BY Despapro.UEN,Despapro.TURNO
        """
        ,conexMSSQL)
    df_desp_x_turno_liq = df_desp_x_turno_liq.convert_dtypes()
    df_desp_x_turno_liq["UEN"] = df_desp_x_turno_liq["UEN"].str.strip()
    df_desp_x_turno_liq["TURNO"] = df_desp_x_turno_liq["TURNO"].str.strip()

    # Create a Pivot Table of "Despachos RedMas" and "Despachos" grouped by "TURNO"
    pivot_desp_totales_x_turno_liq = pd.pivot_table(df_desp_x_turno_liq
        , values=["Despachos RedMas","Despachos"]
        , columns="TURNO"
        , aggfunc="sum"
    )
    # Create column "TOTAL" with the total of each row
    pivot_desp_totales_x_turno_liq = \
        pivot_desp_totales_x_turno_liq.assign(TOTAL= lambda row: row.sum(1))
    # Create a row with the values of "Despachos RedMas" divided by "Despachos"
    pivot_desp_totales_x_turno_liq = \
        pivot_desp_totales_x_turno_liq.append(
            pivot_desp_totales_x_turno_liq.loc["Despachos RedMas"] /
            pivot_desp_totales_x_turno_liq.loc["Despachos"]
            , ignore_index=True
        )
    # Get the row with the results of the last calculation 
    total_penet_x_turno_liq = pivot_desp_totales_x_turno_liq.loc[[2]]
    # Add column UEN with value = "TOTAL"
    total_penet_x_turno_liq.insert(0,"UEN",["TOTAL"])
    # Rename index as "colTOTAL"
    total_penet_x_turno_liq.rename({2:"colTOTAL"}, inplace=True)

    #Get "PENETRACIÓN" by "UEN" and "TURNO"
    df_desp_x_turno_liq["PENETRACIÓN"] = df_desp_x_turno_liq.apply(
        lambda row: (row["Despachos RedMas"])/(row["Despachos"])
        , axis= 1
    )
    pivot_desp_x_turno_liq = pd.pivot_table(df_desp_x_turno_liq
        , values="PENETRACIÓN"
        , index="UEN"
        , columns="TURNO"
        , aggfunc="sum"
        , fill_value=0
    )
    pivot_desp_x_turno_liq.reset_index(inplace=True)


    # Get "TOTAL" of "PENETRACIÓN" by "UEN"
    pivot_desp_liq_total = pd.pivot_table(df_desp_x_turno_liq
        , values=["Despachos RedMas","Despachos"]
        , index="UEN"
        , aggfunc="sum"
        , fill_value=0
    )
    pivot_desp_liq_total.reset_index(inplace=True)
    # Create the column "TOTAL" with the totals per row
    pivot_desp_liq_total["TOTAL"] = pivot_desp_liq_total.apply(
        lambda row: (row["Despachos RedMas"])/(row["Despachos"])
        , axis= 1
    )
    pivot_desp_liq_total = pivot_desp_liq_total[["UEN","TOTAL"]]

    # Merge the column "TOTAL" to the pivot_desp_x_turno_liq
    df_penetRM_liq_x_turno = pd.merge(
        pivot_desp_x_turno_liq,
        pivot_desp_liq_total,
        on="UEN",
        how="inner"
    )
    df_penetRM_liq_x_turno.sort_values(by=["TOTAL"],inplace=True)

    # Add the row with the totals per column
    df_penetRM_liq_x_turno = pd.concat(
        [df_penetRM_liq_x_turno
            , total_penet_x_turno_liq
        ]
    )

    # print("REDMAS PENETRACION LIQ")
    # print(df_penetRM_liq_x_turno)

    return df_penetRM_liq_x_turno


def penetracion_GNC(conexMSSQL):

    ##########################################
    # Get yesterday "PENETRACIÓN" for "GNC"
    ##########################################

    df_desp_x_turno_GNC = pd.read_sql(
        """
            SELECT
                Despapro.UEN,
                Despapro.TURNO,
                (SELECT
                    COUNT(D.VOLUMEN) AS 'Despachos RedMas'
                FROM Rumaos.dbo.Despapro as D
                WHERE D.uen = Despapro.uen
                    AND D.TURNO = Despapro.TURNO
                    AND	D.TARJETA like 'i%'
                    AND D.FECHASQL = DATEADD(DAY,-1,CAST(GETDATE() AS date))
                    AND D.VOLUMEN > '0'
                    AND D.CODPRODUCTO = 'GNC'
                GROUP BY D.UEN ,D.TURNO
                ) AS 'Despachos RedMas',
                COUNT(Despapro.VOLUMEN) AS 'Despachos'
            FROM Rumaos.dbo.Despapro
            WHERE Despapro.TARJETA NOT LIKE 'cc%'
                AND Despapro.FECHASQL = DATEADD(DAY,-1,CAST(GETDATE() AS date))
                AND Despapro.VOLUMEN > '0'
                AND Despapro.CODPRODUCTO = 'GNC'
            GROUP BY Despapro.UEN,Despapro.TURNO
        """
        ,conexMSSQL)
    df_desp_x_turno_GNC = df_desp_x_turno_GNC.convert_dtypes()
    df_desp_x_turno_GNC["UEN"] = df_desp_x_turno_GNC["UEN"].str.strip()
    df_desp_x_turno_GNC["TURNO"] = df_desp_x_turno_GNC["TURNO"].str.strip()

    # Create a Pivot Table of "Despachos RedMas" and "Despachos" grouped by "TURNO"
    pivot_desp_totales_x_turno_GNC = pd.pivot_table(df_desp_x_turno_GNC
        , values=["Despachos RedMas","Despachos"]
        , columns="TURNO"
        , aggfunc="sum"
    )
    # Create column "TOTAL" with the total of each row
    pivot_desp_totales_x_turno_GNC = \
        pivot_desp_totales_x_turno_GNC.assign(TOTAL= lambda row: row.sum(1))
    # Create a row with the values of "Despachos RedMas" divided by "Despachos"
    pivot_desp_totales_x_turno_GNC = \
        pivot_desp_totales_x_turno_GNC.append(
            pivot_desp_totales_x_turno_GNC.loc["Despachos RedMas"] /
            pivot_desp_totales_x_turno_GNC.loc["Despachos"]
            , ignore_index=True
        )
    # Get the row with the results of the last calculation 
    total_penet_x_turno_GNC = pivot_desp_totales_x_turno_GNC.loc[[2]]
    # Add column UEN with value = "TOTAL"
    total_penet_x_turno_GNC.insert(0,"UEN",["TOTAL"])
    # Rename index as "colTOTAL"
    total_penet_x_turno_GNC.rename({2:"colTOTAL"}, inplace=True)

    # Get "PENETRACIÓN" by "UEN" and "TURNO"
    df_desp_x_turno_GNC["PENETRACIÓN"] = df_desp_x_turno_GNC.apply(
        lambda row: (row["Despachos RedMas"])/(row["Despachos"])
        , axis= 1
    )
    pivot_desp_x_turno_GNC = pd.pivot_table(df_desp_x_turno_GNC
        , values="PENETRACIÓN"
        , index="UEN"
        , columns="TURNO"
        , aggfunc="sum"
        , fill_value=0
    )
    pivot_desp_x_turno_GNC.reset_index(inplace=True)


    # Get "TOTAL" of "PENETRACIÓN" by "UEN"
    pivot_desp_GNC_total = pd.pivot_table(df_desp_x_turno_GNC
        , values=["Despachos RedMas","Despachos"]
        , index="UEN"
        , aggfunc="sum"
        , fill_value=0
    )
    pivot_desp_GNC_total.reset_index(inplace=True)
    # Create the column "TOTAL" with the totals per row
    pivot_desp_GNC_total["TOTAL"] = pivot_desp_GNC_total.apply(
        lambda row: (row["Despachos RedMas"])/(row["Despachos"])
        , axis= 1
    )
    pivot_desp_GNC_total = pivot_desp_GNC_total[["UEN","TOTAL"]]

    # Merge the column "TOTAL" to the pivot_desp_x_turno_GNC
    df_penetRM_GNC_x_turno = pd.merge(
        pivot_desp_x_turno_GNC,
        pivot_desp_GNC_total,
        on="UEN",
        how="inner"
    )
    df_penetRM_GNC_x_turno.sort_values(by=["TOTAL"],inplace=True)

    # Add the row with the totals per column
    df_penetRM_GNC_x_turno = pd.concat(
        [df_penetRM_GNC_x_turno
            , total_penet_x_turno_GNC
        ]
    )

    # print("\nREDMAS PENETRACION GNC")
    # print(df_penetRM_GNC_x_turno)

    return df_penetRM_GNC_x_turno


##############
# STYLING of the dataframe
##############

def _estiladorVtaTitulo(df,listaColNumericas,titulo):
    """
This function will return a styled dataframe that must be assign to a variable.
ARGS:
    df: Dataframe that will be styled.
    listaColNumericas: List of numeric columns that will be formatted with
    zero decimals and thousand separator.
    titulo: String for the table caption.
    """
    resultado = df.style \
        .format("{:,.2%}", subset=listaColNumericas) \
        .hide_index() \
        .set_caption(titulo
            +"\n"
            +((pd.to_datetime("today")-pd.to_timedelta(1,"days"))
            .strftime("%d/%m/%y"))
        ) \
        .set_properties(subset=listaColNumericas
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

    #Gradient color for column "TOTAL" without affecting row "TOTAL"
    evitarTotales = df.index.get_level_values(0)
    resultado = resultado.background_gradient(
        cmap="summer_r"
        ,vmin=0
        ,vmax=1
        ,subset=pd.IndexSlice[evitarTotales[:-1],"TOTAL"]
    )
    return resultado


##############
# PRINTING dataframe as an image
##############

# This will print the df with a unique name and will erase the old image 
# everytime the script is run

def _df_to_image(df, nombre):
    """
    Esta función usa las biblioteca "dataframe_Image as dfi" y "os" para 
    generar un archivo .png de un dataframe. Si el archivo ya existe, este será
    reemplazado por el nuevo archivo.

    Args:
        df: dataframe a convertir
         nombre: nombre del archivo incluyendo extensión .png (ej: "hello.png")

    """
    ubicacion = str(pathlib.Path(__file__).parent)+"\\"
    
    if os.path.exists(ubicacion+nombre):
        os.remove(ubicacion+nombre)
        dfi.export(df, ubicacion+nombre)
    else:
        dfi.export(df, ubicacion+nombre)



##############
# FUNCTION TO RUN MODULE
##############

def penetracionRedMas():
    # Timer
    tiempoInicio = pd.to_datetime("today")

    conexMSSQL = conectorMSSQL(login)

    df_penetRM_liq_x_turno_Estilo = _estiladorVtaTitulo(
        penetracion_liq(conexMSSQL)
        , ["1","2","3","TOTAL"]
        , "Penetración RedMas: Líquidos"
    )
    
    df_penetRM_GNC_x_turno_Estilo = _estiladorVtaTitulo(
        penetracion_GNC(conexMSSQL)
        , ["1","2","3","TOTAL"]
        , "Penetración RedMas: GNC"
    )

    _df_to_image(df_penetRM_liq_x_turno_Estilo,"penetracionRedMas_liq.png")
    _df_to_image(df_penetRM_GNC_x_turno_Estilo,"penetracionRedMas_GNC.png")

    # Timer
    tiempoFinal = pd.to_datetime("today")
    print("\nInfo Penetración RedMas"+"\nTiempo de Ejecucion Total:")
    print(tiempoFinal-tiempoInicio)




if __name__ == "__main__":
    penetracionRedMas()