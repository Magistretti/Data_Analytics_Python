import pandas as pd
import datetime as dt

# We are connecting to a Microsoft SQL Server so we can use pyodbc library

import pyodbc

from DatosLogin import login

# "login" is a list that holds the connection data to the SQL Server

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
        FacCli.[NROCLIPRO]
        ,FacCli.[NOMBRE]
        ,FacCli.[DOMICILIO]
        ,FacCli.[LOCALIDAD]
        ,FacCli.[PROVINCIA]
        ,FacCli.[TELEFONO]
        ,FacCli.[EMAIL]
        ,FacCli.[TIPOIVA]
        ,FacCli.[CUITDOC]
        ,FacCli.[CODFORPAGO]
        ,FacCli.[FEULTVTASQL]
        ,FacCli.[SALDOPREPAGO]
        ,FacCli.[SALDOREMIPENDFACTU]
        ,FacCli.SALDOPREPAGO - FacCli.SALDOREMIPENDFACTU as SALDOCUENTA
        ,FacCli.[TARJETA]
        ,FacCli.[TIPOCOMPCC]
        ,FacCli.[BLOQUEADO]
        ,FacCli.[LIMITECREDITO]
        ,Vendedores.[NOMBREVEND]
        ,FacCli.[ListaSaldoCC]
    FROM [Rumaos].[dbo].[FacCli]
    left outer join Vendedores On FacCli.NROVEND = Vendedores.NROVEND
    where ListaSaldoCC = 1
        and FacCli.SALDOPREPAGO - FacCli.SALDOREMIPENDFACTU < -100;
""", db_conex)

# Casting column [NROCLIPRO] to string

df_cuentasDeudoras["NROCLIPRO"] = \
    df_cuentasDeudoras["NROCLIPRO"].astype("int64")
df_cuentasDeudoras["NROCLIPRO"] = \
    df_cuentasDeudoras["NROCLIPRO"].astype("string")

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
    where FECHASQL >= '20210601'
        and ListaSaldoCC = 1
        and FacCli.SALDOPREPAGO - FacCli.SALDOREMIPENDFACTU < -100
""", db_conex)

#print(df_remitos.head(5))

df_remitos = df_remitos.convert_dtypes()

#df_remitos.info()

#######
# Get the first date, last date and the sum of IMPORTE of every client in 
# df_remitos
#######

df_primerRemitoPorCuenta = df_remitos[["NROCLIENTE","NOMBRE","FECHASQL"]]\
    .groupby(["NROCLIENTE","NOMBRE"]).min()

#print(df_primerRemitoPorCuenta.head(5))

df_ultimoRemitoPorCuenta = df_remitos[["NROCLIENTE","NOMBRE","FECHASQL"]]\
    .groupby(["NROCLIENTE","NOMBRE"]).max()

#print(df_ultimoRemitoPorCuenta.head(5))

# df_remitos[df_remitos["FECHASQL"]==max(df_remitos["FECHASQL"])]
# print(primerRemito)

df_ventaPesos = df_remitos[["NROCLIENTE","NOMBRE","IMPORTE"]]\
    .groupby(["NROCLIENTE","NOMBRE"]).sum()

#print(df_ventaPesos.head(5))

########
# Merging df_primerRemitoPorCuenta, df_ultimoRemitoPorCuenta and df_ventaPesos
# into a new dataframe, df_remitosVentasPorCliente
########

df_remitosVentasPorCliente = pd.merge(
    df_primerRemitoPorCuenta,
    df_ultimoRemitoPorCuenta,
    on=["NROCLIENTE","NOMBRE"],
    suffixes=("_PrimerRemito","_UltimoRemito")
)

df_remitosVentasPorCliente = pd.merge(
    df_remitosVentasPorCliente,
    df_ventaPesos,
    on=["NROCLIENTE","NOMBRE"]
)

#######
# Creating columns "Dias Entre 1er y Ultimo Remito" and "Venta $ Prom Diaria" 
# in df_remitosVentasPorCliente
#######

df_remitosVentasPorCliente["Dias Entre 1er y Ultimo Remito"] = \
    df_remitosVentasPorCliente.apply(
        lambda row: \
            (row["FECHASQL_UltimoRemito"]-row["FECHASQL_PrimerRemito"])
                if (row["FECHASQL_UltimoRemito"]-row["FECHASQL_PrimerRemito"]) >
                    pd.to_timedelta(0, unit="days") #Compare timedelta > 0
                else pd.to_timedelta(1, unit="days") #To avoid divide by 0
        , axis= 1 #This will apply the lambda function per row
    ).dt.days

df_remitosVentasPorCliente["Venta $ Prom Diaria"] = \
    df_remitosVentasPorCliente.apply(
        lambda row: row["IMPORTE"] /
        (row["Dias Entre 1er y Ultimo Remito"])
        , axis= 1
    )

print(df_remitosVentasPorCliente.head(5))
