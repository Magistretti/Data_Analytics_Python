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

tiempoInicio = pd.to_datetime("today")


conexMSSQL = conectorMSSQL(login)


#########
# Convert dbo.FacRemDet SQL table to a Dataframe
# 
# Requirements:
# -Filter unused columns
# -Get account numbers above '100000'
# -Use "ListaSaldoCC = 1" filter to avoid old or frozen accounts
# -Use a filter by total debt (SALDOPREPAGO - SALDOREMIPENDFACTU) 
#   to get only debt below -1000
# -Get data from 2018 to yesterday
######### 

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

        , DATEDIFF(DAY, MAX(CAST(FRD.[FECHASQL] as date)), CAST(GETDATE()-1 as date)) as 'Días Desde Última Compra'

        FROM [Rumaos].[dbo].[FacRemDet] as FRD with (NOLOCK)
        INNER JOIN dbo.FacCli as Cli with (NOLOCK)
            ON FRD.NROCLIENTE = Cli.NROCLIPRO

        where FRD.NROCLIENTE > '100000'
            AND (Cli.SALDOPREPAGO - Cli.SALDOREMIPENDFACTU) < -1000
            and Cli.ListaSaldoCC = 1
            and FECHASQL >= '20180101' and FECHASQL < CAST(GETDATE() as date)

        group by FRD.NROCLIENTE, Cli.NOMBRE
        order by MIN(Cli.SALDOPREPAGO - Cli.SALDOREMIPENDFACTU)
    """, conexMSSQL
)

df_cuentasDeudoras = df_cuentasDeudoras.convert_dtypes()

#print(df_cuentasDeudoras.head(20))


#############
# -Create column "Cond Deuda Cliente"
# 
#############

def _categorias(dias):
    """
    This function manage the category assigned according to quantity of days
    """
    if dias < 20:
        return "Normal"
    elif dias < 40:
        return "Excedido"
    elif dias < 60:
        return "Moroso"
    else:
        return "PREJUDICIAL"

def _condDeuda(diasAdeudados, diasUltCompra):
    """
    This function apply the logic between "Días Venta Adeud" and
    "Días Desde Última Compra" to choose which is going to be use to
    determine the category of debtor
    """
    if diasUltCompra < 20:
        return _categorias(diasAdeudados)
    elif diasUltCompra >= diasAdeudados:
        return _categorias(diasUltCompra)
    else:
        return _categorias(diasAdeudados)
            

df_cuentasDeudoras["Cond Deuda Cliente"] = \
    df_cuentasDeudoras.apply(
        lambda row: _condDeuda(
            row["Días Venta Adeud"]
            , row["Días Desde Última Compra"]
        ), axis= 1
    )


df_cuentasDeudoras = \
    df_cuentasDeudoras[df_cuentasDeudoras["Cond Deuda Cliente"] != "Normal"]

#print(df_cuentasDeudoras)


##############
# Creating Total Row for "SALDOCUENTA" column
##############

# Will cast both "Días" columns as a string to avoid trailing zeroes and sum,
# also we will be able to fill with ""
df_cuentasDeudoras= df_cuentasDeudoras\
    .astype({"Días Venta Adeud": "string"})
df_cuentasDeudoras= df_cuentasDeudoras\
    .astype({"Días Desde Última Compra": "string"})


df_cuentasDeudoras.loc["colTOTAL"]= \
    pd.Series(df_cuentasDeudoras["SALDOCUENTA"].sum()
        , index= ["SALDOCUENTA"]
    )


# df_cuentasDeudoras= df_cuentasDeudoras\
#     .astype({"SALDOCUENTA": "int"})

df_cuentasDeudoras= df_cuentasDeudoras.fillna({"NOMBRE":"TOTAL"}).fillna("")

# print(df_cuentasDeudoras)


##########################################
# STYLING of the dataframe
##########################################

# The next function will format cells with the value "Excedido" to 
# have a red background

def excedidoFondoRojo(dataframe):
    return ["background-color: yellow" if valor == "Excedido" 
        else "background-color: red" if valor == "PREJUDICIAL"
        else "background-color: orange" if valor == "Moroso"
        else "background-color: default" for valor in dataframe]

df_cuentasDeudoras_Estilo = \
    df_cuentasDeudoras.style \
        .format({"SALDOCUENTA": "${0:,.0f}"}) \
        .hide_index() \
        .set_caption("DEUDORES MOROSOS Y EXCEDIDOS"
            +" "
            +tiempoInicio.strftime("%d/%m/%y")
        ) \
        .set_properties(subset=[
            "Días Venta Adeud"
            , "Cond Deuda Cliente"
            , "Días Desde Última Compra"
            ], **{"text-align": "center"}
        ) \
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
        .apply(excedidoFondoRojo,subset=["Cond Deuda Cliente"]) \
        .apply(lambda x: ["background: black" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1)


##########################################
# NOTE: to show the dataframe with the style in Jupyter Notebook you need to 
# use display() method even when it seem to not be available. If you use 
# print() it will return an error because is an styler object.
# Also display() will return an error in the Terminal window.
##########################################

try:
    display(df_cuentasDeudoras_Estilo) # type: ignore
except:
    print("")


##########################################
# PRINTING dataframe as an image
##########################################

ubicacion = str(pathlib.Path(__file__).parent)+"\\"

# This will print the df with name and time so you can have multiple
# files in the same folder

# dfi.export(df_cuentasDeudoras_Estilo,
#     ubicacion
#     +"Info_Morosos"
#     + pd.to_datetime("today").strftime("%Y-%m-%d_%H%M%S")
#     + ".png"
# )


# This will print the df with a unique name and will erase the old image 
# everytime the script is run

if os.path.exists(ubicacion+"test.png"):
    os.remove(ubicacion+"test.png")
    dfi.export(df_cuentasDeudoras_Estilo, 
        ubicacion+"test.png")
else:
    dfi.export(df_cuentasDeudoras_Estilo, 
        ubicacion+"test.png")


# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Morosos"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)