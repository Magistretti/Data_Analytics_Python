###################################
#
#     INFORME DEUDA COMERCIAL (DEUDORES MOROSOS)
#               22/12/21
###################################

import os
import sys
import pathlib

# Allow imports from the top folder
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))

import pandas as pd
import dataframe_image as dfi
from openpyxl import load_workbook

from DatosLogin import login
from Conectores import conectorMSSQL

import logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)


#########
# Formating display of dataframes with comma separator for numbers
pd.options.display.float_format = "{:20,.2f}".format 
#########


#########
# Convert dbo.FacRemDet SQL table to a Dataframe
# 
# Requirements:
# -Filter unused columns
# -Get account numbers above '100000'
# -Use "ListaSaldoCC = 1" filter to avoid old or frozen accounts
# -Use a filter by total debt (SALDOPREPAGO - SALDOREMIPENDFACTU) 
#   to get only debt below -1000
# -Get data from 2021 to today
######### 

def _get_df(conexMSSQL):
    df_cuentasDeudoras = pd.read_sql(
        """
            SELECT
            CAST(FRD.[NROCLIENTE] as VARCHAR) as 'NROCLIENTE'
            ,RTRIM(Cli.NOMBRE) as 'NOMBRE'

            --,MIN(CAST(FRD.[FECHASQL] as date)) as 'FECHA_1erRemito'
            --,MAX(CAST(FRD.[FECHASQL] as date)) as 'FECHA_UltRemito'

            ----Días Entre Remitos
            --,IIF(MIN(CAST(FRD.[FECHASQL] as date)) = MAX(CAST(FRD.[FECHASQL] as date))
            --	, -1
            --	, DATEDIFF(DAY,MAX(CAST(FRD.[FECHASQL] as date)),MIN(CAST(FRD.[FECHASQL] as date)))
            --) as 'Días Entre Remitos'

            --,sum(FRD.[IMPORTE]) as 'ConsumoHistorico'

            ----Consumo Diario
            --,sum(FRD.[IMPORTE])/IIF(MIN(CAST(FRD.[FECHASQL] as date)) = MAX(CAST(FRD.[FECHASQL] as date))
            --	, -1
            --	, DATEDIFF(DAY,MAX(CAST(FRD.[FECHASQL] as date)),MIN(CAST(FRD.[FECHASQL] as date)))
            --) as 'Consumo Diario'

            ,CAST(ROUND(MIN(Cli.SALDOPREPAGO - Cli.SALDOREMIPENDFACTU),0) as int) as 'SALDOCUENTA'

            ,CAST(MIN(Cli.SALDOPREPAGO - Cli.SALDOREMIPENDFACTU)
                /(sum(FRD.[IMPORTE])/IIF(MIN(CAST(FRD.[FECHASQL] as date)) = MAX(CAST(FRD.[FECHASQL] as date))
                    , -1
                    , DATEDIFF(DAY,MAX(CAST(FRD.[FECHASQL] as date)),MIN(CAST(FRD.[FECHASQL] as date)))
                )) as int) as 'Días Venta Adeud'

            , DATEDIFF(DAY, MAX(CAST(FRD.[FECHASQL] as date)), CAST(GETDATE() as date)) as 'Días Desde Última Compra'

            , ISNULL(RTRIM(Vend.NOMBREVEND),'') as 'Vendedor'

            FROM [Rumaos].[dbo].[FacRemDet] as FRD with (NOLOCK)
            INNER JOIN dbo.FacCli as Cli with (NOLOCK)
                ON FRD.NROCLIENTE = Cli.NROCLIPRO
            LEFT OUTER JOIN dbo.Vendedores as Vend with (NOLOCK)
	            ON Cli.NROVEND = Vend.NROVEND

            where FRD.NROCLIENTE > '100000'
                AND (Cli.SALDOPREPAGO - Cli.SALDOREMIPENDFACTU) < -1000
                and Cli.ListaSaldoCC = 1
                and FECHASQL >= '20180101' and FECHASQL <= CAST(GETDATE() as date)

            group by FRD.NROCLIENTE, Cli.NOMBRE, Vend.NOMBREVEND
            order by MIN(Cli.SALDOPREPAGO - Cli.SALDOREMIPENDFACTU)
        """, conexMSSQL
    )

    df_cuentasDeudoras = df_cuentasDeudoras.convert_dtypes()
    
    return df_cuentasDeudoras

#print(df_cuentasDeudoras.head(20))


# Get auxiliary table of debtors from Excel to a series
def _get_series_excel(ubicacion, nombre, hoja):
    df = pd.read_excel(
        ubicacion + nombre
        , hoja
        , header=0
        , usecols=["NROCLIENTE"]
        , squeeze=True
    )

    return df


#######################################
# -Create column "Cond Deuda Cliente"
#######################################

def _categorias(dias):
    """
    This function manage the category assigned according to quantity of days
    """
    if dias < 20:
        return "1-Normal"
    elif dias < 40:
        return "2-Excedido"
    elif dias < 60:
        return "3-Moroso"
    else:
        return "4-Moroso Grave"

def _condDeuda(nrocliente, ref_incobrable, diasAdeudados, diasUltCompra):
    """
    This function apply the logic between "Días Venta Adeud" and
    "Días Desde Última Compra" to choose which is going to be use to
    determine the category of debtor, it also add the category "5-INCOBRABLE"
    acording to a list of client numbers
    """
    if nrocliente in ref_incobrable.values.astype(str):
        return "5-INCOBRABLE"
    elif diasUltCompra < 20:
        return _categorias(diasAdeudados)
    elif diasUltCompra >= diasAdeudados:
        return _categorias(diasUltCompra)
    else:
        return _categorias(diasAdeudados)



##########################################
# STYLING of the dataframe
##########################################

def _fondoColor(dataframe):
    return ["background-color: green" if valor == "1-Normal" 
        else "background-color: yellow" if valor == "2-Excedido"
        else "background-color: orange" if valor == "3-Moroso"
        else "background-color: red" if valor == "4-Moroso Grave"
        else "background-color: black" if valor == "5-INCOBRABLE"
        else "background-color: black" for valor in dataframe]

def _letraColor(dataframe):
    return ["color: white" if valor == "5-INCOBRABLE"
        else "color: default" for valor in dataframe]


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
            + " "
            + (pd.to_datetime("today")
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

def condicionDeudores():
    """
    Create an image of the debtors grouped by state of debt and an Excel file
    with each debtor and debt
    """

    # Timer
    tiempoInicio = pd.to_datetime("today")

    # Files location
    ubicacion = str(pathlib.Path(__file__).parent)+"\\"

    # Get Connection to database
    conexMSSQL = conectorMSSQL(login)

    # Get DFs
    df_cuentasDeudoras = _get_df(conexMSSQL)
    sr_incobrables = _get_series_excel(
        ubicacion
        , "Incobrables.xlsx"
        , "Incobrables"
    )

    # Clean and transform DFs
    df_cuentasDeudoras["Cond Deuda Cliente"] = \
        df_cuentasDeudoras.apply(
            lambda row: _condDeuda(
                row["NROCLIENTE"]
                ,sr_incobrables
                ,row["Días Venta Adeud"]
                , row["Días Desde Última Compra"]
            ), axis= 1
        )


    #############################
    # Transform DF to get a list of clients in Excel
    #############################

    nombreExcel = "ClientesDeudores.xlsx"


    df_ctas_Para_Excel = df_cuentasDeudoras

    # Will cast both "Días" columns as a string to be able to fill with ""
    df_ctas_Para_Excel= df_ctas_Para_Excel\
        .astype({"Días Venta Adeud": "string"})
    df_ctas_Para_Excel= df_ctas_Para_Excel\
        .astype({"Días Desde Última Compra": "string"})

    # Add TOTAL row
    df_ctas_Para_Excel.loc[df_ctas_Para_Excel.index[-1]+1]= \
        pd.Series(df_ctas_Para_Excel["SALDOCUENTA"].sum()
            , index= ["SALDOCUENTA"]
        )

    df_ctas_Para_Excel = df_ctas_Para_Excel.fillna({"NOMBRE":"TOTAL"}).fillna("")

    df_ctas_Para_Excel_Estilo = _estiladorVtaTitulo(
        df_ctas_Para_Excel,["SALDOCUENTA"]
    ).apply(_fondoColor, subset=["Cond Deuda Cliente"])

    # Print list of clients to Excel file
    writer = pd.ExcelWriter(ubicacion + nombreExcel
        , engine="xlsxwriter"
    )

    df_ctas_Para_Excel_Estilo.to_excel(
        writer
        , sheet_name="ClientesDeudores"
        , header=True
        , index=False
    )

    worksheet = writer.sheets["ClientesDeudores"]

    # Auto adjust cell lenght
    for column in df_ctas_Para_Excel:
        column_length = max(
            df_ctas_Para_Excel[column].astype(str).map(len).max()
            , len(column)
        )
        col_idx = df_ctas_Para_Excel.columns.get_loc(column)
        worksheet.set_column(
            col_idx
            , col_idx
            , column_length + 1
        )

    writer.save()

    # Improve format of "SALDOCUENTA" column in Excel
    wBook = load_workbook(ubicacion + nombreExcel)
    wSheet = wBook.active
    col = wSheet["C"] # Column "SALDOCUENTA"
    for cell in col:
        # Will add thousand separator, negatives in red and "$" symbol
        cell.number_format='"$"\ #,##0;[Red]\-"$"\ #,##0'
    wBook.save(ubicacion + nombreExcel)
    wBook.close()



    ##########################################
    # Grouped by "Cond Deuda Cliente" version
    ##########################################

    df_saldosCondicion = df_cuentasDeudoras[["Cond Deuda Cliente","SALDOCUENTA"]]
    df_saldosCondicion = df_saldosCondicion.groupby(["Cond Deuda Cliente"]).sum()
    df_saldosCondicion.reset_index(inplace=True)

    # Creating row "TOTAL"
    df_saldosCondicion.loc[df_saldosCondicion.index[-1]+1]= \
        pd.Series(
            df_saldosCondicion["SALDOCUENTA"].sum()
            , index= ["SALDOCUENTA"]
        )
    # Fill the NaN in column "Cond Deuda Cliente"
    df_saldosCondicion.fillna("TOTAL", inplace=True)

    df_saldosCondicion["Participación"] = (
        df_saldosCondicion[["SALDOCUENTA"]]
        / df_saldosCondicion[["SALDOCUENTA"]].sum()
        * 2 # Added "TOTAL" value requires multiply by 2 to get correct percentages
    )

    df_saldosCondicion.rename(columns={
        "Cond Deuda Cliente": "Condición"
        , "SALDOCUENTA": "Saldos"
    }, inplace=True)


    df_saldosCondicion_Estilo = _estiladorVtaTitulo(
        df_saldosCondicion
        ,list_Col_Num=["Saldos"]
        ,list_Col_Perc=["Participación"]
        ,titulo="CLIENTES DEUDORES"
    ).apply(_fondoColor, subset=["Condición"]) \
    .apply(_letraColor, subset=["Condición"])

    #display(df_saldosCondicion_Estilo)

    # Get image from df_saldosCondicion_Estilo
    _df_to_image(
        df_saldosCondicion_Estilo
        ,ubicacion
        ,"DeudaComercial.png"
    )



    ##########################################
    # List of debtors with filter 2-Excedido
    ##########################################

    def _totalRow(df:pd.DataFrame):
        # Creating row "TOTAL"
        df.loc[df.index[-1]+1] = pd.Series(
            df["SALDOCUENTA"].sum()
            , index= ["SALDOCUENTA"]
        )
        # Fill the NaN in column "Cond Deuda Cliente"
        df.fillna({
            "NOMBRE": "TOTAL"
            , "Cond Deuda Cliente": ""
        }, inplace=True)

        return df


    try:
        # Filter
        df_saldosExcedido = df_cuentasDeudoras[
            df_cuentasDeudoras["Cond Deuda Cliente"] == "2-Excedido"
        ].copy() # Avoid warning of view vs copy

        df_saldosExcedido = _totalRow(df_saldosExcedido)

        df_saldosExcedido.rename(columns={
            "Cond Deuda Cliente": "Condición"
            , "SALDOCUENTA": "Saldos"
        }, inplace=True)


        df_saldosExcedido_Estilo = _estiladorVtaTitulo(
            df_saldosExcedido
            ,list_Col_Num=["Saldos"]
            ,titulo="CLIENTES DEUDORES EXCEDIDOS"
        ).apply(_fondoColor, subset=["Condición"])

        #display(df_saldosExcedido_Estilo)

        # Get image from df_saldosExcedido_Estilo
        _df_to_image(
            df_saldosExcedido_Estilo
            ,ubicacion
            ,"DeudaExcedida.png"
        )

    except IndexError:
        logger.info("Empty DataFrame, no debtors in category 2-Excedido")


    ##########################################
    # List of debtors with filter 3-Morosos
    ##########################################

    try:
        # Filter
        df_saldosMorosos = df_cuentasDeudoras[
            df_cuentasDeudoras["Cond Deuda Cliente"] == "3-Moroso"
        ].copy() # Avoid warning of view vs copy

        df_saldosMorosos = _totalRow(df_saldosMorosos)

        df_saldosMorosos.rename(columns={
            "Cond Deuda Cliente": "Condición"
            , "SALDOCUENTA": "Saldos"
        }, inplace=True)

        df_saldosMorosos_Estilo = _estiladorVtaTitulo(
            df_saldosMorosos
            ,list_Col_Num=["Saldos"]
            ,titulo="CLIENTES DEUDORES MOROSOS"
        ).apply(_fondoColor, subset=["Condición"])

        #display(df_saldosMorosos_Estilo)

        # Get image from df_saldosMorosos_Estilo
        _df_to_image(
            df_saldosMorosos_Estilo
            ,ubicacion
            ,"DeudaMorosa.png"
        )

    except IndexError:
        logger.info("Empty DataFrame, no debtors in category 3-Moroso")

    ##########################################
    # List of debtors with filter 4-Moroso Grave
    ##########################################


    try:
        # Filter
        df_saldosMorosoGrave = df_cuentasDeudoras[
            df_cuentasDeudoras["Cond Deuda Cliente"] == "4-Moroso Grave"
        ].copy() # Avoid warning of view vs copy

        df_saldosMorosoGrave = _totalRow(df_saldosMorosoGrave)

        df_saldosMorosoGrave.rename(columns={
            "Cond Deuda Cliente": "Condición"
            , "SALDOCUENTA": "Saldos"
        }, inplace=True)

        df_saldosMorosoGrave_Estilo = _estiladorVtaTitulo(
            df_saldosMorosoGrave
            ,list_Col_Num=["Saldos"]
            ,titulo="CLIENTES DEUDORES MOROSOS GRAVES"
        ).apply(_fondoColor, subset=["Condición"])

        #display(df_saldosMorosoGrave_Estilo)

        # Get image from df_saldosMorosoGrave_Estilo
        _df_to_image(
            df_saldosMorosoGrave_Estilo
            ,ubicacion
            ,"DeudaMorosaGrave.png"
        )

    except IndexError:
        logger.info("Empty DataFrame, no debtors in category 4-Moroso Grave")




        
    # Timer
    tiempoFinal = pd.to_datetime("today")
    logger.info(
        "Info Condición Deudores"
        + "\nTiempo de Ejecucion Total: "
        + str(tiempoFinal-tiempoInicio)
    )




if __name__ == "__main__":
    condicionDeudores()