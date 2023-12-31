﻿###################################
#
#     INFORME Primer Despacho por Activación
#             (CAMIONEROS)
#               21/09/21
###################################

import mysql.connector
from mysql.connector import Error
import pyodbc
from DatosLogin import login, loginMySQL

import pandas as pd

import os
import dataframe_image as dfi

import logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)


tiempoInicio = pd.to_datetime("today")


def conectorMySQL(datos):
    """
    This function will create a connection to a MySQL Server, should be 
    provided with a list with the following strings in this exact order:
        datos=["host","port","database","user","password"]
    """
    try:
        conexMySQL = mysql.connector.connect(
            host=datos[0]
            ,port=datos[1]
            ,database=datos[2]
            ,user=datos[3]
            ,password=datos[4]
        )
        if conexMySQL.is_connected():
            db_Info = conexMySQL.get_server_info()
            logger.info("\nConnected to MySQL Server version " + db_Info)
            cursor = conexMySQL.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            logger.info("You're connected to database: " + record[0])
            cursor.close()
            return conexMySQL
            
    except Error as e:
        logger.error("\nError while connecting to MySQL\n" + e, exc_info=1)


def conectorMSSQL(datos):
    """
    This function will create a connection to a Microsoft SQL Server, should 
    be provided with a list with the following strings in this exact order:
        datos=["server","database","username","password"]
    """
    try:
        conexMSSQL = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};\
            SERVER="+datos[0]+";\
            DATABASE="+datos[1]+";\
            UID="+datos[2]+";\
            PWD="+ datos[3]
        )
        logger.info(
            "\nConnected to server: "
            + datos[0]
            + "\nDatabase: " 
            + datos[1]
        )
        
        return conexMSSQL

    except Exception as e:
        listaErrores = e.args[1].split(".")
        logger.error("\nOcurrió un error al conectar a SQL Server: ")
        for i in listaErrores:
            logger.error(i)
        

# Create database connections
conexMySQL = conectorMySQL(loginMySQL)
conexMSSQL = conectorMSSQL(login)


# Extracting all the cards numbers that had been activated in the last 92 days 
# and belong to truckers
df_tarj_camion = pd.read_sql("""
    SELECT 
        users.tarjeta as 'TARJETA'      
    FROM red_mas_prod.users
    where users.tarjeta like 'ig%'
        #and users.category = '17'
        and register_app_at >= cast(date_add(now(), INTERVAL -92 DAY) as date)
""", conexMySQL)

df_tarj_camion = df_tarj_camion.convert_dtypes()
df_tarj_camion["TARJETA"] = df_tarj_camion["TARJETA"].str.strip()


# Extracting all the cards numbers that had been used for the first time 
# yesterday and belong to certain UENs
df_1era_carga = pd.read_sql("""
    SELECT
        D.TARJETA
        , MAX(d.UEN) as UEN
        , MIN(FECHADESPSQL) as 'F.DESPACHO'
    FROM [Rumaos].[dbo].[VIEW_DESPAPRO_FILTRADA] as D
        join GDM_SRV.dbo.Cliente as GDM_C on D.TARJETA = GDM_C.TARJETA
    where D.TARJETA like 'ig%'
        and D.UEN IN (
            'MERC GUAYMALLEN'
            ,'MERCADO 2'
            ,'MITRE'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
        )
        and GDM_C.Categoria = '17'
        and GDM_C.FechaAlta >= DATEADD(day, -92, CAST(GETDATE() AS date))
    GROUP BY D.TARJETA
    HAVING (CAST(MIN(FECHADESPSQL) AS date) = DATEADD(day, -1, CAST(GETDATE() AS date)))
""", conexMSSQL)

df_1era_carga = df_1era_carga.convert_dtypes()
df_1era_carga["UEN"] = df_1era_carga["UEN"].str.strip()
df_1era_carga["TARJETA"] = df_1era_carga["TARJETA"].str.strip()


# Inner merge of "df_1era_carga" and "df_tarj_camion" to get the truckers cards 
# activated in the last 92 days and used for the first time yesterday
df_1era_carga_camion = pd.merge(
    df_1era_carga,
    df_tarj_camion,
    on="TARJETA"
)

# Using a pivot table to count TARJETA per UEN
pivot_1era_carga_camion = pd.pivot_table(df_1era_carga_camion
    , values="TARJETA"
    , index="UEN"
    , aggfunc="count"
)

pivot_1era_carga_camion = pivot_1era_carga_camion.reset_index()
pivot_1era_carga_camion.rename(columns={"TARJETA":"ACTIVACIONES"},inplace=True)
pivot_1era_carga_camion = pivot_1era_carga_camion.convert_dtypes()

# Counting all the truckers purchases, of yesterday, that dont have an account
df_despachos = pd.read_sql("""
     SELECT a.UEN, count(distinct a.DESPACHOS) as 'DESPACHOS' FROM (
	SELECT 
	
       D.UEN, D.ID as 'DESPACHOS'
    FROM [Rumaos].[dbo].[VIEW_DESPAPRO_FILTRADA] as D
        inner join Rumaos.dbo.Fac001 as F on 
            D.TIPOCOMP = F.TIPOCOMP
            and D.LETRACOMP = F.LETRACOMP
            and D.PTOVTA = F.PTOVTA
            and D.NROCOMP = F.NROCOMP
    WHERE 
		
		D.CODPRODUCTO not like 'GNC'
        and D.UEN IN (
            'MERC GUAYMALLEN'
            ,'MERCADO 2'
            ,'MITRE'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
        )
        and D.VOLUMEN >= '99'
        and D.FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
    
	UNION
	SELECT    
			D.UEN,
         D.ID as 'DESPACHOS'
    FROM [Rumaos].[dbo].[VIEW_DESPAPRO_FILTRADA] as D
        inner join Rumaos.dbo.FacRemDet as F on 
            D.PTOVTA = F.PTOVTA
            and D.NROCOMP = F.NROREMITO
    WHERE 
		
		D.CODPRODUCTO not like 'GNC'
        and D.UEN IN (
            'MERC GUAYMALLEN'
            ,'MERCADO 2'
            ,'MITRE'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
        )
        and D.VOLUMEN >= '99'
        and D.FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date)))as a
		group by a.UEN
""", conexMSSQL)

df_despachos["UEN"] = df_despachos["UEN"].str.strip()


# Counting all the truckers purchases, of yesterday, that dont have an account
# but use "RedMas" Card
df_despachosRedmas = pd.read_sql("""
    SELECT a.UEN, count(distinct a.DESPACHOS) as 'DESPACHOS RED MAS' FROM (
	SELECT 
	
       D.UEN, D.ID as 'DESPACHOS'
    FROM [Rumaos].[dbo].[VIEW_DESPAPRO_FILTRADA] as D
        inner join Rumaos.dbo.Fac001 as F on 
            D.TIPOCOMP = F.TIPOCOMP
            and D.LETRACOMP = F.LETRACOMP
            and D.PTOVTA = F.PTOVTA
            and D.NROCOMP = F.NROCOMP
    WHERE 
		D.TARJETA like 'i%' 
		and D.TARJETA not like 'ig99999%'
		and D.CODPRODUCTO not like 'GNC'
        and D.UEN IN (
            'MERC GUAYMALLEN'
            ,'MERCADO 2'
            ,'MITRE'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
        )
        and D.VOLUMEN >= '99'
        and D.FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
    
	UNION
	SELECT    
			D.UEN,
         D.ID as 'DESPACHOS'
    FROM [Rumaos].[dbo].[VIEW_DESPAPRO_FILTRADA] as D
        inner join Rumaos.dbo.FacRemDet as F on 
            D.PTOVTA = F.PTOVTA
            and D.NROCOMP = F.NROREMITO
    WHERE 
		D.TARJETA like 'i%' 
		and D.TARJETA not like 'ig99999%'
		and D.CODPRODUCTO not like 'GNC'
        and D.UEN IN (
            'MERC GUAYMALLEN'
            ,'MERCADO 2'
            ,'MITRE'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
        )
        and D.VOLUMEN >= '99'
        and D.FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date)) )as a
		group by a.UEN
""", conexMSSQL)

df_despachosRedmas["UEN"] = df_despachosRedmas["UEN"].str.strip()


# Merge "df_despachosRedmas" and "df_despachos"
df_despachosCompleto = pd.merge(
    df_despachosRedmas,
    df_despachos,
    on="UEN",
    how="outer"
)

# Create column "PENETRACIÓN RED MÁS" and insert it in position 1
df_despachosCompleto.insert(
    1
    ,"PENETRACIÓN RED MÁS"
    ,df_despachosCompleto.apply(
        lambda row: row["DESPACHOS RED MAS"]/row["DESPACHOS"]
            , axis= 1
    )
)

# Sort by "PENETRACIÓN RED MÁS" in descending order
df_despachosCompleto = df_despachosCompleto.sort_values(
    by=["PENETRACIÓN RED MÁS"]
    ,ascending=False
)

# Merge pivot_1era_carga_camion and df_despachosCompleto
df_despachos_Y_Activ = pd.merge(
    df_despachosCompleto,
    pivot_1era_carga_camion,
    on="UEN",
    how="outer"
)

# Fill NaNs of merges
df_despachos_Y_Activ.fillna(value=0, inplace=True)

# Create row of column totals
df_despachos_Y_Activ.loc["colTOTAL"]= pd.Series(
    df_despachos_Y_Activ.sum()
    , index=["DESPACHOS RED MAS","DESPACHOS","ACTIVACIONES"]
)
# Fill NaN in UEN column at total row
df_despachos_Y_Activ.fillna({"UEN":"TOTAL"}, inplace=True)

# Fill NaN in "PENETRACIÓN RED MÁS" column at total row with the total quantity 
# of "DESPACHOS RED MAS" over "DESPACHOS"
tasa = (df_despachos_Y_Activ.loc["colTOTAL","DESPACHOS RED MAS"] /
    df_despachos_Y_Activ.loc["colTOTAL","DESPACHOS"])

df_despachos_Y_Activ.fillna({"PENETRACIÓN RED MÁS":tasa}, inplace=True)

# Creating column "ACTIVACIONES" with value zero in case it doesn't exist
if "ACTIVACIONES" not in df_despachos_Y_Activ:
    df_despachos_Y_Activ["ACTIVACIONES"] = 0



##########################################
# STYLING of the dataframe
##########################################

def estiladorVtaTitulo(df,listaColNumericas,titulo):
    """
This function will return a styled dataframe that must be assign to a variable.
ARGS:
    df: Dataframe that will be styled.
    listaColNumericas: List of numeric columns that will be formatted with
    zero decimals and thousand separator.
    titulo: String for the table caption.
    """
    resultado = df.style \
        .format("{0:,.0f}", subset=listaColNumericas) \
        .hide_index() \
        .set_caption(titulo
            +"\n"
            +((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
        ) \
        .set_properties(subset=listaColNumericas
            , **{"text-align": "center", "width": "100px"}) \
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

df_despachos_Y_Activ_Estilo = estiladorVtaTitulo(
    df_despachos_Y_Activ
    ,["DESPACHOS", "DESPACHOS RED MAS", "ACTIVACIONES"]
    , "Despachos a Camioneros: Efectividad Red Más"
)

# Apply percentage format and center text to "PENETRACIÓN RED MÁS"
df_despachos_Y_Activ_Estilo.format("{:,.2%}", subset="PENETRACIÓN RED MÁS")
df_despachos_Y_Activ_Estilo.set_properties(subset="PENETRACIÓN RED MÁS"
    , **{"text-align": "center", "width": "100px"})

#Gradient color for "PENETRACIÓN RED MÁS" without affecting TOTAL Row
evitarTotales = df_despachos_Y_Activ.index.get_level_values(0)
df_despachos_Y_Activ_Estilo.background_gradient(
    cmap="summer_r"
    ,vmin=0
    ,vmax=1
    ,subset=pd.IndexSlice[evitarTotales[:-1],"PENETRACIÓN RED MÁS"]
)

try:
    display(df_despachos_Y_Activ_Estilo) # type: ignore
except:
    pass


##########################################
# PRINTING dataframe as an image
##########################################

# This will print the df with a unique name and will erase the old image 
# everytime the script is run

ubicacion = "C:\Informes\DespachosCamionerosRedmas\\"
nombreDespachos = "Info_Despachos_Camioneros.png"

def df_to_image(df, ubicacion, nombre):
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


df_to_image(df_despachos_Y_Activ_Estilo, ubicacion, nombreDespachos)


# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo 1er Despachos Camioneros"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)


# if __name__ == "__main__":
#     print("Nada por aquí")
#     # Timer
#     tiempoFinal = pd.to_datetime("today")
#     print("\nInfo 1er Despachos Camionero"+"\nTiempo de Ejecucion Total:")
#     print(tiempoFinal-tiempoInicio)  