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
    db_conex = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};\
        SERVER="+server+";DATABASE="+database+";UID="+username+";PWD="+ password)
except Exception as e:
    print("Ocurrió un error al conectar a SQL Server: ", e)
    exit()

cursor = db_conex.cursor()

############
#Sample Query
# cursor.execute("SELECT * FROM [Rumaos].[dbo].[Mail]")
# row = cursor.fetchone()
# while row:
#     print(row[1])
#     row = cursor.fetchone()
#
# df_mail = pd.read_sql("SELECT * FROM [Rumaos].[dbo].[Mail]",db_conex)
# print(df_mail)
############

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

df_cuentasDeudoras.head(5)

df_condicionCliente = pd.read_excel("C:/Users/gpedro/OneDrive - RedMercosur/"
    "Consultas Power BI/TABLERO SGES VS SGFIN/Clientes condicion especial.xlsx",
    usecols="B,C,I")

df_condicionCliente.dropna(subset=["Condición del cliente"], inplace=True)

print(df_condicionCliente)