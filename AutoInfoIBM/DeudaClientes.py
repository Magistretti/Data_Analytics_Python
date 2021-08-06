import pandas as pd

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
        FacCli.[ID] 
        ,FacCli.[NROCLIPRO]
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
        ,FacCli.SALDOPREPAGO - FacCli.SALDOREMIPENDFACTU as SALDOTOTAL
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

#df_cuentasDeudoras.head(5)

#########
# Convert dbo.FacRemDet SQL table to a Dataframe
#
# Requirements:
# -Filter unused columns
# -Show name of vendor instead of number
# -Use "ListaSaldoCC = 1" filter to avoid old or frozen accounts
# -Use a filter by total debt (SALDOPREPAGO - SALDOREMIPENDFACTU) 
#   to get only debt below -100
######### 

df_remitos = pd.read_sql("""
    SELECT 
        [UEN]
        ,[FECHASQL]
        ,[TURNO]
        ,[PTOVTA]
        ,cast([NROREMITO] AS VARCHAR) as NROREMITO
        ,[NROCLIENTE]
        ,[CODPRODUCTO]
        ,[CANTIDAD]
        ,[PXUNINETO]
        ,[IMPINT]
        ,[IMPIVA]
        ,[PXUNITARIO]
        ,[IMPORTE]
    FROM [Rumaos].[dbo].[FacRemDet]
    where FECHASQL >= '20210601'
""", db_conex)

def new_func(df,col):
    df[col] = \
        df[col].astype("int64")
    df[col] = \
        df[col].astype("string")

new_func(df_remitos,"NROCLIENTE")

#df_remitos.head(5)
df_remitos.info()
df_remitos = df_remitos.convert_dtypes()
print("////////////////")
df_remitos.info()
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
