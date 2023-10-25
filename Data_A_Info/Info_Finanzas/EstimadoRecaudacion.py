import os
import math
import numpy as np
#from Conectores import conectorMSSQL
from PIL import Image
import pandas as pd
from pandas.api.types import CategoricalDtype
import dataframe_image as dfi
import pyodbc #Library to connect to Microsoft SQL Server
from DatosLogin import login #Holds connection data to the SQL Server
import sys
import pathlib
from datetime import datetime
from datetime import timedelta
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))
import logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)

#################################

tiempoInicio = pd.to_datetime("today")
#########
# Formating display of dataframes with comma separator for numbers
pd.options.display.float_format = "{:20,.0f}".format 
#########

try:
    db_conex = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};\
        SERVER="+login[0]+";\
        DATABASE="+login[1]+";\
        UID="+login[2]+";\
        PWD="+ login[3]
    )
except Exception as e:
    listaErrores = e.args[1].split(".")
    logger.error("\nOcurrió un error al conectar a SQL Server: ")
    for i in listaErrores:
        logger.error(i)
    exit()

server2 = "192.168.200.33\SGESFIN"
database2 = "Sgfin"
username2 = "gpedro"
password2 = "pedrito1234"

login2= [server2,database2,username2,password2]

try:
    db_conex2 = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};\
        SERVER="+login2[0]+";\
        DATABASE="+login2[1]+";\
        UID="+login2[2]+";\
        PWD="+ login2[3]
    )
except Exception as e:
    listaErrores = e.args[1].split(".")
    logger.error("\nOcurrió un error al conectar a SQL Server: ")
    for i in listaErrores:
        logger.error(i)
    exit()

########################################
################ ESTIMADO DE RECAUDACION #####
########################################

#####

fondoInicial= pd.read_sql('''
SELECT  FondoInicial as 'SALDO CAJA',Nombre AS 'UEN'
FROM (
  SELECT 
a.FondoInicial ,b.Nombre,
    ROW_NUMBER() OVER (PARTITION BY b.Nombre ORDER BY a.Fecha DESC) AS rn
from SGFIN_Arqueo as a left join SGFIN_Box as b 
on a.IdBox=b.Id
) ranked
WHERE rn = 1;
''',db_conex2
)
fondoInicial=fondoInicial.convert_dtypes()


dolaresEnCaja= pd.read_sql('''
select a.Fecha,b.Nombre as 'UEN',-d.cantidadbilletes as 'SALDO DOLARES' from SGFIN_Arqueo as A left join [Sgfin].[dbo].[SGFIN_DetalleArqueo] as D
on A.Id= D.idArqueo
left join SGFIN_Box as b 
on a.IdBox=b.Id
left join SGFIN_Billete as l
on d.idbillete=l.id

where l.Descripcion like 'dolar pesifi%'
AND d.cantidadbilletes > 0
order by a.Fecha desc
''',db_conex2
)
dolaresEnCaja=dolaresEnCaja.convert_dtypes()

# Ordenar el DataFrame por 'Fecha' en orden descendente
dolaresEnCaja = dolaresEnCaja.sort_values(by='Fecha', ascending=False)

# Usar groupby para encontrar la fecha más reciente para cada estación
dolaresEnCaja = dolaresEnCaja.groupby('UEN').first().reset_index()
dolaresEnCaja = dolaresEnCaja.reindex(columns=['UEN','SALDO DOLARES'])

fondoInicial = fondoInicial.merge(dolaresEnCaja,on=['UEN'],how='outer') 
fondoInicial = fondoInicial.fillna(0)
fondoInicial['SALDO CAJA'] = fondoInicial['SALDO CAJA'] + fondoInicial['SALDO DOLARES']
fondoInicial = fondoInicial.reindex(columns=['UEN','SALDO CAJA'])
##########################  HASTA EL TURNO 3 DE AYER

hastaT3 = pd.read_sql('''

    select G.UEN,SUM(G.IMPORTE) AS IMPORTE1 from EPGen as G join VtaTurno as T 
    on G.UEN = T.UEN
    AND G.FECHASQL = T.FECHASQL
    AND G.TURNO = T.TURNO
    WHERE (MEDIOPAGO = 1)
    and G.FECHASQL >= DATEADD(day, -31, CAST(GETDATE() AS date))
    and G.FECHASQL <= DATEADD(day, -1, CAST(GETDATE() AS date))
    AND (G.DESCDEPTO = 'PLAYA' or G.DESCDEPTO = 'PANADERIA           ' OR G.DESCDEPTO = 'SERVICOMPRAS        ')
    and T.NRORECA = 0
    GROUP BY G.UEN

    ''' ,db_conex)
hastaT3 = hastaT3.convert_dtypes()

##########################  POR RECAUDAR HASTA EL TURNO 1 HOY



turno1HOY = pd.read_sql('''

    select G.UEN,SUM(G.IMPORTE) AS IMPORTE2 from EPGen as G join VtaTurno as T 
    on G.UEN = T.UEN
    AND G.FECHASQL = T.FECHASQL
    AND G.TURNO = T.TURNO
    WHERE (MEDIOPAGO = 1)
    and G.FECHASQL = DATEADD(day, 0, CAST(GETDATE() AS date))
    AND (G.DESCDEPTO = 'PLAYA' or G.DESCDEPTO = 'PANADERIA           ' OR G.DESCDEPTO = 'SERVICOMPRAS        ')
    and T.NRORECA = 0
    and G.TURNO = 1
    GROUP BY G.UEN

    ''' ,db_conex)
turno1HOY = turno1HOY.convert_dtypes()



##########################  RECAUDADO HASTA EL MOMENTO

recaudadoHoy = pd.read_sql('''

       select UEN, SUM(IMPORTE) AS 'Recaudado Hasta el Momento' from MovFondos
	where FECHASQL = DATEADD(day, 0, CAST(GETDATE() AS date))
	group by UEN

    ''' ,db_conex)
recaudadoHoy = recaudadoHoy.convert_dtypes()

estimadoRecaud = hastaT3.merge(turno1HOY,on='UEN',how='outer')
estimadoRecaud = estimadoRecaud.merge(recaudadoHoy,on='UEN',how='outer')
estimadoRecaud = estimadoRecaud.fillna(0)

estimadoRecaud['Por Recaudar Hasta T1 de Hoy']= estimadoRecaud["IMPORTE1"]+estimadoRecaud["IMPORTE2"]



estimadoRecaud=estimadoRecaud.reindex(columns=['UEN','Por Recaudar Hasta T1 de Hoy' ,'Recaudado Hasta el Momento'])
estimadoRecaud = estimadoRecaud.fillna(0)

estimadoRecaud['UEN']=estimadoRecaud['UEN'].str.strip()
estimadoRecaud = estimadoRecaud.merge(fondoInicial, on='UEN', how='outer')
estimadoRecaud = estimadoRecaud.loc[estimadoRecaud["Por Recaudar Hasta T1 de Hoy"] != None ,:]
estimadoRecaud = estimadoRecaud.fillna(0)
estimadoRecaud.loc["colTOTAL"]= pd.Series(
    estimadoRecaud.sum(numeric_only=True)
    , index=["Por Recaudar Hasta T1 de Hoy","Recaudado Hasta el Momento","SALDO CAJA"]
)
estimadoRecaud.fillna({"UEN":"TOTAL"}, inplace=True)




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
        .format("{0:,.2f}", subset=list_Col_Num) \
        .format("{:,.2%}", subset=list_Col_Perc) \
        .hide(axis=0) \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio).strftime("%d/%m/%y"))
            + "<br>") \
        .set_properties(subset= list_Col_Perc + list_Col_Num
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

#### Defino columnas para cada Dataframe (Numericas)
numCols = ["Recaudado Hasta el Momento"
           ,"SALDO CAJA"
            ,'Por Recaudar Hasta T1 de Hoy'
        
         ]
### COLUMNAS PARA INFORME PENETRACION
percColsPen = [
]

#### COLUMNAS INFORME EJECUCION PANADERIA PRESUPUESTADO DIARIO
percColsDiaria = [

]
###### Aplico el formato elegido a la imagen


estimadoRecaud = _estiladorVtaTitulo(estimadoRecaud,numCols,percColsPen, "Estimado Recaudacion")
###### DEFINO NOMBRE Y UBICACION DE DONDE SE VA A GUARDAR LA IMAGEN

ubicacion= str(pathlib.Path(__file__).parent)+"\\"
nombrepng = "Estimado_Recaudacion.png"

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

df_to_image(estimadoRecaud, ubicacion, nombrepng)


#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)








#%%
