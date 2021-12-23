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
# Convert dbo.Faccli SQL table to a Dataframe
#
# At this point we will make a SQL query with the required data of the "Faccli"
# table merged to the "Vendedores" table and turn that to a dataframe
# with Pandas.
# 
# Requirements:
# -Filter unused columns
# -Show name of vendor instead of number
# -Use "ListaSaldoCC = 1" filter to avoid old or frozen accounts
# -Use a filter by total debt (SALDOPREPAGO - SALDOREMIPENDFACTU) 
#   to get only debt below -100
######### 

df_cuentasDeudoras = pd.read_sql(
    """
        SELECT 
            CAST(FacCli.[NROCLIPRO] AS VARCHAR) as 'NROCLIENTE'
            ,FacCli.[NOMBRE]
            ,FacCli.SALDOPREPAGO - FacCli.SALDOREMIPENDFACTU as 'SALDOCUENTA'
            ,CAST(FacCli.FEULTVTASQL as date) as 'FechaUltVta'
        FROM [Rumaos].[dbo].[FacCli] WITH (NOLOCK)
        where ListaSaldoCC = 1
            and (FacCli.SALDOPREPAGO - FacCli.SALDOREMIPENDFACTU) < -1000;
    """, conexMSSQL
)

df_cuentasDeudoras = df_cuentasDeudoras.convert_dtypes()


#########
# Convert dbo.FacRemDet SQL table to a Dataframe
#
# Requirements:
# -Filter unused columns
# -Cast PTOVTA, NROREMITO and NROCLIENTE as string
# -Get data not older than a year back from today
#########

fechaAñoAtras = (tiempoInicio - pd.Timedelta(90, unit="d"))\
    .strftime("'%Y%m%d'")

df_remitos = pd.read_sql(
    """
        SELECT 
            FacRemDet.[FECHASQL]
            ,cast(FacRemDet.[NROCLIENTE] AS VARCHAR) as NROCLIENTE
            ,FacCli.[NOMBRE]
            ,FacRemDet.[IMPORTE]
        FROM [Rumaos].[dbo].[FacRemDet] WITH (NOLOCK)
        Left Outer Join FacCli WITH (NOLOCK) 
            on FacRemDet.NROCLIENTE = FacCli.NROCLIPRO
        where FECHASQL >= """+fechaAñoAtras+"""
            and ListaSaldoCC = 1
            and (FacCli.SALDOPREPAGO - FacCli.SALDOREMIPENDFACTU) < -1000
    """, conexMSSQL
)

df_remitos = df_remitos.convert_dtypes()


############
# -Get the first date, last date and the sum of IMPORTE of each client in 
#   df_remitos,
# -Also get the sum of IMPORTE of the last 7 days for each client
############

df_primerRemitoPorCuenta = df_remitos[["NROCLIENTE","NOMBRE","FECHASQL"]]\
    .groupby(["NROCLIENTE","NOMBRE"]).min()

#print(df_primerRemitoPorCuenta.head())

df_ultimoRemitoPorCuenta = df_remitos[["NROCLIENTE","NOMBRE","FECHASQL"]]\
    .groupby(["NROCLIENTE","NOMBRE"]).max()

#print(df_ultimoRemitoPorCuenta.head())

df_ventaPesosPorCuenta = df_remitos[["NROCLIENTE","NOMBRE","IMPORTE"]]\
    .groupby(["NROCLIENTE","NOMBRE"]).sum()

#print(df_ventaPesosPorCuenta.head())

fechaHoy = pd.to_datetime("today").normalize() #datetime at 00:00:00

fechaSemanaAtras = fechaHoy - pd.to_timedelta(7, unit="days")

df_remitos7Dias = df_remitos[
    (df_remitos["FECHASQL"] >= fechaSemanaAtras) 
    & (df_remitos["FECHASQL"] < fechaHoy)
]

#print(df_remitos7Dias.head())

df_VentaSemanalPorCuenta = df_remitos7Dias[["NROCLIENTE","NOMBRE","IMPORTE"]]\
    .groupby(["NROCLIENTE","NOMBRE"]).sum()

#print(df_VentaSemanalPorCuenta.head())


############
# -Merge df_remitosVentasPorCliente with df_cuentasDeudoras to get 
# the "SALDOCUENTA" column
# Merging df_primerRemitoPorCuenta, df_ultimoRemitoPorCuenta, 
# df_ventaPesosPorCuenta and df_VentaSemanalPorCuenta into a new
# dataframe, df_remitosVentasPorCliente
############


df_remitosVentasPorCliente = pd.merge(
    df_cuentasDeudoras[["NROCLIENTE","NOMBRE","SALDOCUENTA"]],
    df_primerRemitoPorCuenta,
    how="left",
    on=["NROCLIENTE","NOMBRE"]
)

df_remitosVentasPorCliente = pd.merge(
    df_remitosVentasPorCliente,
    df_ultimoRemitoPorCuenta,
    how="left",
    on=["NROCLIENTE","NOMBRE"],
    suffixes=("_PrimerRemito","_UltimoRemito")
)

df_remitosVentasPorCliente = pd.merge(
    df_remitosVentasPorCliente,
    df_ventaPesosPorCuenta,
    how="left",
    on=["NROCLIENTE","NOMBRE"]
)

df_remitosVentasPorCliente = pd.merge(
    df_remitosVentasPorCliente,
    df_VentaSemanalPorCuenta,
    how="left",
    on=["NROCLIENTE","NOMBRE"],
    suffixes=("","_Semanal")
)

# Filling NaNs and NaTs found in accounts without sales in the last year
df_remitosVentasPorCliente.fillna({
    "FECHASQL_PrimerRemito": fechaHoy
    ,"FECHASQL_UltimoRemito": fechaHoy
    ,"IMPORTE": 0
    ,"IMPORTE_Semanal": 0
}, inplace=True)


#######
# Creating columns: 
#   -"Dias Entre 1er y Ultimo Remito" 
#   -"Venta $ Prom Diaria"
#   -"Venta $ Prom Ult 7 Dias" 
# in df_remitosVentasPorCliente
#######

df_remitosVentasPorCliente["Dias Entre 1er y Ultimo Remito"] = \
    df_remitosVentasPorCliente.apply(
        lambda row: 
            (row["FECHASQL_UltimoRemito"]-row["FECHASQL_PrimerRemito"])
            if (row["FECHASQL_UltimoRemito"]-row["FECHASQL_PrimerRemito"]) >
                pd.to_timedelta(0, unit="days") #Compare timedelta > 0
            else pd.to_timedelta(1, unit="days") #To avoid divide by 0
        , axis= 1 #This will apply the lambda function per row
    ).dt.days

df_remitosVentasPorCliente["Venta $ Prom Diaria"] = \
    df_remitosVentasPorCliente.apply(
        lambda row: row["IMPORTE"] / row["Dias Entre 1er y Ultimo Remito"]
        , axis= 1
    )

df_remitosVentasPorCliente["Venta $ Prom Ult 7 Dias"] = \
    df_remitosVentasPorCliente.apply(
        lambda row: row["IMPORTE_Semanal"] / 7
        , axis= 1
    )

print(df_remitosVentasPorCliente.head())


#############
# -Create column "Dias Venta Adeud" = SALDOCUENTA/"Mayor Vta $ Prom Diaria"
# -"Mayor Vta $ Prom Diaria" is the biggest of "Venta $ Prom Diaria" and
#   "Venta $ Prom Ult 7 Dias"
# -Create column "Cond Deuda Cliente" = 
#   IF("Dias Venta Adeud" < 20, "Normal",
#       IF("Dias Venta Adeud" < 30, "Excedido",
#            IF("Dias Venta Adeud" < 60, "Moroso",
#                "PREJUDICIAL")))
#############


def _diasVtaAdeud(saldocuenta, vtaPromDia, vtaProm7Dia):
    if vtaPromDia == vtaProm7Dia == 0:
        return 100
    elif vtaPromDia > vtaProm7Dia:
        return round(saldocuenta * (-1) / vtaPromDia)
    else:
        return round(saldocuenta * (-1) / vtaProm7Dia)


df_remitosVentasPorCliente["Dias Venta Adeud"] = \
    df_remitosVentasPorCliente.apply(
        lambda row: _diasVtaAdeud(
            row["SALDOCUENTA"]
            , row["Venta $ Prom Diaria"]
            , row["Venta $ Prom Ult 7 Dias"]
        )
        , axis= 1
    )

def condDeuda(diasAdeudados):
    if diasAdeudados < 20:
        return "Normal"
    elif diasAdeudados < 30:
        return "Excedido"
    elif diasAdeudados < 60:
        return "Moroso"
    else:
        return "PREJUDICIAL"

df_remitosVentasPorCliente["Cond Deuda Cliente"] = \
    df_remitosVentasPorCliente.apply(
        lambda row: condDeuda(row["Dias Venta Adeud"])
        , axis= 1
    )

df_condicionCuentas = df_remitosVentasPorCliente[
    ["NOMBRE","SALDOCUENTA","Dias Venta Adeud","Cond Deuda Cliente"]
]

df_condicionCuentasRetrasadas = \
    df_condicionCuentas[df_condicionCuentas["Cond Deuda Cliente"] != "Normal"]

df_condicionCuentasRetrasadas = \
df_condicionCuentasRetrasadas.sort_values(by=["SALDOCUENTA"])

#print(df_condicionCuentasRetrasadas)


##############
# Creating Total Row for "SALDOCUENTA" column
##############

# Will cast "Dias Venta Adeud" as a string to avoid trailing zeroes and sum
df_condicionCuentasRetrasadas= \
    df_condicionCuentasRetrasadas.astype({"Dias Venta Adeud": "string"})

df_condicionCuentasRetrasadas.loc["colTOTAL"]= \
    pd.Series(df_condicionCuentasRetrasadas["SALDOCUENTA"].sum()
        , index= ["SALDOCUENTA"]
    )

df_condicionCuentasRetrasadas= \
    df_condicionCuentasRetrasadas.fillna({"NOMBRE":"TOTAL"}).fillna("")

#print(df_condicionCuentasRetrasadas)


##############
# STYLING of the dataframe
##############

# The next function will format cells with the value "Excedido" to 
# have a red background

def excedidoFondoRojo(dataframe):
    return ["background-color: orange" if valor == "Excedido" 
        else "background-color: red" if valor == "PREJUDICIAL"
        else "background-color: yellow" if valor == "Moroso"
        else "background-color: default" for valor in dataframe]

df_conEstilo_condCtaRetrasadas = \
    df_condicionCuentasRetrasadas.style \
        .format({"SALDOCUENTA": "${0:,.0f}"}) \
        .hide_index() \
        .set_caption("DEUDORES MOROSOS Y EXCEDIDOS"
            +" "
            +tiempoInicio.strftime("%d/%m/%y")
        ) \
        .set_properties(subset=["Dias Venta Adeud", "Cond Deuda Cliente"]
            , **{"text-align": "center"}) \
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


##############
# NOTE: to show the dataframe with the style in Jupyter Notebook you need to 
# use display() method even when it seem to not be available. If you use 
# print() it will return an error because is an styler object.
# Also display() will return an error in the Terminal window.
##############

try:
    display(df_conEstilo_condCtaRetrasadas) # type: ignore
except:
    print("")

##############
# PRINTING dataframe as an image
##############

ubicacion = str(pathlib.Path(__file__).parent)+"\\"

# This will print the df with name and time so you can have multiple
# files in the same folder

# dfi.export(df_conEstilo_condCtaRetrasadas,
#     ubicacion
#     +"Info_Morosos"
#     + pd.to_datetime("today").strftime("%Y-%m-%d_%H%M%S")
#     + ".png"
# )


# This will print the df with a unique name and will erase the old image 
# everytime the script is run

if os.path.exists(ubicacion+"test.png"):
    os.remove(ubicacion+"test.png")
    dfi.export(df_conEstilo_condCtaRetrasadas, 
        ubicacion+"test.png")
else:
    dfi.export(df_conEstilo_condCtaRetrasadas, 
        ubicacion+"test.png")


# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Morosos"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)