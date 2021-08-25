import os
import pandas as pd
#########
# Formating display of dataframes with comma separator for numbers
pd.options.display.float_format = "{:20,.2f}".format 
#########

import dataframe_image as dfi

tiempoInicio = pd.to_datetime("today")

def biggestOf2(a,b):
    if a > b:
        return a
    else:
        return b

# We are connecting to a Microsoft SQL Server so we can use pyodbc library
import pyodbc

# "login" is a list that holds the connection data to the SQL Server
from DatosLogin import login

server = login[0]
database = login[1]
username = login[2] 
password = login[3]

# Try connecting to the SQL Server, will report error and stop if failed
try:
    db_conex = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};\
        SERVER="+server+";\
        DATABASE="+database+";\
        UID="+username+";\
        PWD="+ password
    )
except Exception as e:
    print("Ocurrió un error al conectar a SQL Server: ", e)
    exit()

cursor = db_conex.cursor()

############
#   Sample Query
# cursor.execute("SELECT * FROM [Rumaos].[dbo].[Mail]")
# row = cursor.fetchone()
# while row:
#     print(row[1])
#     row = cursor.fetchone()
#
# df_mail = pd.read_sql("SELECT * FROM [Rumaos].[dbo].[Mail]",db_conex)
# print(df_mail)
############

#########
# Table "Clientes condicion especial.xlsx" not needed anymore
    #
    # Convert Excel file "Clientes condicion especial.xlsx" to a Dataframe
    #
    # Requirements:
    # -Filter unused columns
    # -Drop rows with NaNs in "Condición del cliente" column
    #########

    # df_condicionCliente = pd.read_excel("C:/Users/gpedro/OneDrive - RedMercosur/"
    #     "Consultas Power BI/TABLERO SGES VS SGFIN/"
    #     "Clientes condicion especial.xlsx",
    #     usecols="B,I")

    # df_condicionCliente["NROCLIPRO"] = \
    #     df_condicionCliente["NROCLIPRO"].astype("string")

    # df_condicionCliente.dropna(subset=["Condición del cliente"], inplace=True)

    #print(df_condicionCliente)

    #########
    # Left Outer Merge of df_cuentasDeudoras with df_condicionCliente to add 
    # "Condición del cliente" column to df_cuentasDeudoras
    #########

    # df_cuentasDeudoras = df_cuentasDeudoras.merge(right=df_condicionCliente,
    #     how="left", on="NROCLIPRO")
    # df_cuentasDeudoras.info()


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

df_cuentasDeudoras = pd.read_sql("""
    SELECT 
        CAST(FacCli.[NROCLIPRO] AS VARCHAR) as NROCLIENTE
        ,FacCli.[NOMBRE]
        ,FacCli.[DOMICILIO]
        ,FacCli.[LOCALIDAD]
        ,FacCli.[PROVINCIA]
        ,FacCli.[TELEFONO]
        ,FacCli.[EMAIL]
        ,FacCli.[TIPOIVA]
        ,FacCli.[CUITDOC]
        ,CAST(FacCli.[CODFORPAGO] AS VARCHAR) as CODFORPAGO
        ,FacCli.[FEULTVTASQL]
        ,FacCli.[SALDOPREPAGO]
        ,FacCli.[SALDOREMIPENDFACTU]
        ,FacCli.SALDOPREPAGO - FacCli.SALDOREMIPENDFACTU as SALDOCUENTA
        ,FacCli.[TARJETA]
        ,FacCli.[TIPOCOMPCC]
        ,CAST(FacCli.[BLOQUEADO] AS VARCHAR) as BLOQUEADO
        ,FacCli.[LIMITECREDITO]
        ,Vendedores.[NOMBREVEND]
        ,FacCli.[ListaSaldoCC]
    FROM [Rumaos].[dbo].[FacCli]
    left outer join Vendedores On FacCli.NROVEND = Vendedores.NROVEND
    where ListaSaldoCC = 1
        and FacCli.SALDOPREPAGO - FacCli.SALDOREMIPENDFACTU < -100;
""", db_conex)

df_cuentasDeudoras = df_cuentasDeudoras.convert_dtypes()

#print(df_cuentasDeudoras.info())
#print(df_cuentasDeudoras.head(5))


#########
# Convert dbo.FacRemDet SQL table to a Dataframe
#
# Requirements:
# -Filter unused columns
# -Cast PTOVTA, NROREMITO and NROCLIENTE as string
# -Filter data Previous to 2021-01-01
######### 

df_remitos = pd.read_sql("""
    SELECT 
        FacRemDet.[UEN]
        ,FacRemDet.[FECHASQL]
        ,FacRemDet.[TURNO]
        ,cast(FacRemDet.[PTOVTA] AS VARCHAR) as PTOVTA
        ,cast(FacRemDet.[NROREMITO] AS VARCHAR) as NROREMITO
        ,cast(FacRemDet.[NROCLIENTE] AS VARCHAR) as NROCLIENTE
        ,FacCli.[NOMBRE]
        ,FacRemDet.[CODPRODUCTO]
        ,FacRemDet.[CANTIDAD]
        ,FacRemDet.[PXUNINETO]
        ,FacRemDet.[IMPINT]
        ,FacRemDet.[IMPIVA]
        ,FacRemDet.[PXUNITARIO]
        ,FacRemDet.[IMPORTE]
        ,FacCli.SALDOPREPAGO - FacCli.SALDOREMIPENDFACTU as SALDOCUENTA
        ,FacCli.[ListaSaldoCC]
    FROM [Rumaos].[dbo].[FacRemDet]
    Left Outer Join FacCli on FacRemDet.NROCLIENTE = FacCli.NROCLIPRO
    where FECHASQL >= '20210101'
        and ListaSaldoCC = 1
        and FacCli.SALDOPREPAGO - FacCli.SALDOREMIPENDFACTU < -100
""", db_conex)

#print(df_remitos.head(5))
df_remitos = df_remitos.convert_dtypes()
#df_remitos.info()


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

fechaHoy = pd.to_datetime("today").normalize()

fechaSemanaAtras = fechaHoy - pd.to_timedelta(7, unit="days")

df_remitos7Dias = df_remitos[(df_remitos["FECHASQL"] >= 
    fechaSemanaAtras) & (df_remitos["FECHASQL"] < fechaHoy)]

#print(df_remitos7Dias.head())

df_VentaSemanalPorCuenta = df_remitos7Dias[["NROCLIENTE","NOMBRE","IMPORTE"]]\
    .groupby(["NROCLIENTE","NOMBRE"]).sum()

#print(df_VentaSemanalPorCuenta.head())


############
# Merging df_primerRemitoPorCuenta, df_ultimoRemitoPorCuenta, 
# df_ventaPesosPorCuenta and df_VentaSemanalPorCuenta into a new
# dataframe, df_remitosVentasPorCliente
############

df_remitosVentasPorCliente = pd.merge(
    df_primerRemitoPorCuenta,
    df_ultimoRemitoPorCuenta,
    on=["NROCLIENTE","NOMBRE"],
    suffixes=("_PrimerRemito","_UltimoRemito")
)

df_remitosVentasPorCliente = pd.merge(
    df_remitosVentasPorCliente,
    df_ventaPesosPorCuenta,
    on=["NROCLIENTE","NOMBRE"]
)

df_remitosVentasPorCliente = pd.merge(
    df_remitosVentasPorCliente,
    df_VentaSemanalPorCuenta,
    on=["NROCLIENTE","NOMBRE"],
    suffixes=("","_Semanal")
)

#######
# Creating columns: 
#   -"Dias Entre 1er y Ultimo Remito" 
#   -"Venta $ Prom Diaria"
#   -"Venta $ Prom Ult 7 Dias" 
# in df_remitosVentasPorCliente
#######

df_remitosVentasPorCliente["Dias Entre 1er y Ultimo Remito"] = \
    df_remitosVentasPorCliente.apply(
        lambda row: (row["FECHASQL_UltimoRemito"]-row["FECHASQL_PrimerRemito"])
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

#print(df_remitosVentasPorCliente.head())


#############
# -Merge df_remitosVentasPorCliente with df_cuentasDeudoras to get 
# the "SALDOCUENTA" column
# -Create column "Dias Venta Adeud" = SALDOCUENTA/"Mayor Vta $ Prom Diaria"
# -"Mayor Vta $ Prom Diaria" is the biggest of "Venta $ Prom Diaria" and
#   "Venta $ Prom Ult 7 Dias"
# -Create column "Cond Deuda Cliente" = IF("Dias Venta Adeud" < 20, "Normal",
#   IF("Dias Venta Adeud" < 30, "Moroso","Excedido"))
#############

df_remitosVentasPorCliente = pd.merge(
    df_remitosVentasPorCliente,
    df_cuentasDeudoras[["NROCLIENTE","NOMBRE","SALDOCUENTA"]],
    on=["NROCLIENTE","NOMBRE"]
)

df_remitosVentasPorCliente["Dias Venta Adeud"] = \
    df_remitosVentasPorCliente.apply(
        lambda row: round(row["SALDOCUENTA"] * (-1) / 
            biggestOf2(
                row["Venta $ Prom Diaria"],
                row["Venta $ Prom Ult 7 Dias"]
            )
        )
        , axis= 1
    )

def condDeuda(diasAdeudados):
    if diasAdeudados < 20:
        return "Normal"
    elif diasAdeudados < 30:
        return "Moroso"
    else:
        return "Excedido"

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
    return ["background-color: red" if valor == "Excedido" 
        else "background-color: default" for valor in dataframe]

df_conEstilo_condCtaRetrasadas = \
    df_condicionCuentasRetrasadas.style \
        .format({"SALDOCUENTA": "${0:,.0f}"}) \
        .hide_index() \
        .set_caption("DEUDORES MOROSOS Y EXCEDIDOS"
            +" "
            +tiempoInicio.strftime("%d-%m-%y")
        ) \
        .set_properties(subset=["Dias Venta Adeud", "Cond Deuda Cliente"]
            , **{"text-align": "center"}) \
        .set_properties(border= "2px solid black") \
        .set_table_styles([
            {"selector": "caption", "props": [("font-size", "20px")]}
            , {"selector": "th", "props": [("text-align", "center")]}
        ]) \
        .apply(excedidoFondoRojo,subset=["Cond Deuda Cliente"])


##############
# NOTE: to show the dataframe with the style in Jupyter Notebook you need to 
# use display() method even when it seem to not be available. If you use 
# print() it will return an error because is an styler object.
# Also display() will return an error in the Terminal window.
##############

try:
    display(df_conEstilo_condCtaRetrasadas)
except:
    print("")

##############
# PRINTING dataframe as an image
##############

# This will print the df with a name and time so you can have multiple
# files in the same folder
#
# dfi.export(df_conEstilo_condCtaRetrasadas,
#     "dataframe_test_"
#     + pd.to_datetime("today").strftime("%Y-%m-%d_%H%M%S")
#     + ".png"
# )

# This will print the df with a unique name and will erase the old image 
# everytime the script is run

if os.path.exists("C:\Informes\AutoInfoIBM\Info_Morosos.png"):
    os.remove("C:\Informes\AutoInfoIBM\Info_Morosos.png")
    dfi.export(df_conEstilo_condCtaRetrasadas, "Info_Morosos.png")
else:
    dfi.export(df_conEstilo_condCtaRetrasadas, "Info_Morosos.png")

tiempoFinal = pd.to_datetime("today")
print("\nTiempo de Ejecucion Total:")
print(tiempoFinal-tiempoInicio)