import os
import math
import numpy as np
from DatosLogin import login
from Conectores import conectorMSSQL
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


##########################################
# TRABAJO CON TABLA DE EXCEL    ################
##########################################

ubicacion = str(pathlib.Path(__file__).parent)+"\\"
aux_semanal = "Presupuesto_Mix.xlsx"
presuMix =pd.read_excel(ubicacion+aux_semanal, sheet_name= 'Hoja1')
presuMix = presuMix.convert_dtypes()


##########################################
################ MENSUAL ACUMULADO  ######
##########################################

##########################   DESPACHOS ACUMULADOS MENSUAL

df_despachosM = pd.read_sql('''
	
    DECLARE @inicioMesActual DATETIME
	SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, CURRENT_TIMESTAMP), 0)

	DECLARE @inicioMesAnterior DATETIME
	SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

	--Divide por la cant de días del mes anterior y multiplica por la cant de días del
	--mes actual
	
	DECLARE @hoy DATETIME
	SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

	--Divide por la cantidad de días cursados del mes actual y multiplica por la cant
	--de días del mes actual

  	select   UEN, SUM(VTATOTVOL) as 'DESPACHOS', CODPRODUCTO from EmpVenta 
	WHERE 
	FECHASQL >= @inicioMesActual
	and	FECHASQL < @hoy
	and CODPRODUCTO not like 'GNC'
        and UEN IN (
            'SAN JOSE'
            ,'LAMADRID'
            ,'AZCUENAGA'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE')
	group by UEN, CODPRODUCTO
	ORDER BY UEN
    ''' ,db_conex)

######### Creo columnas de Naftas y Gasoleos con sus respectivos valores
df_despachosM = df_despachosM.loc[:,df_despachosM.columns!="ID"]
turno1 = df_despachosM['CODPRODUCTO'] == "EU   "
turno1 = df_despachosM[turno1]
df_despachosM["EU"]= 1 * turno1["DESPACHOS"]

turno2 = df_despachosM['CODPRODUCTO'] == "GO   "
turno2 = df_despachosM[turno2]
df_despachosM["GO"]= 1 * turno2["DESPACHOS"]

turno3 = df_despachosM['CODPRODUCTO'] == "NS   "
turno3 = df_despachosM[turno3]
df_despachosM["NS"]= 1* turno3["DESPACHOS"]

turno4 = df_despachosM['CODPRODUCTO'] == "NU   "
turno4 = df_despachosM[turno4]
df_despachosM["NU"]= 1* turno4["DESPACHOS"]
#### Elimino columnas Codproducto Y CANTIDAD Dado que la informacion de estas columnas ya las tengo en TURNO 1, 2 Y 3

df_despachosM = df_despachosM.loc[:,df_despachosM.columns!="CODPRODUCTO"]
df_despachosM = df_despachosM.loc[:,df_despachosM.columns!="DESPACHOS"]

df_despachosM = df_despachosM.reset_index()

##### Agrupo por UEN
df_despachosM = df_despachosM.groupby(
        ["UEN"]
        , as_index=False
    ).sum()

###### Elimino columna Turno 1 Ya que Panaderia en Turno 1 no tiene Ventas
df_despachosM = df_despachosM.loc[:,df_despachosM.columns!="index"]


#############################################   
######################### DIARIO
##############################################

############## DESPACHOS DIARIOS


df_despachos = pd.read_sql('''
	
  	select   UEN, SUM(VTATOTVOL) as 'DESPACHOS', CODPRODUCTO from EmpVenta 
	WHERE 
	FECHASQL = DATEADD(day, -1, CAST(GETDATE() AS date))
	and CODPRODUCTO not like 'GNC'
        and UEN IN (
            'SAN JOSE'
            ,'LAMADRID'
            ,'AZCUENAGA'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE')
	group by UEN, CODPRODUCTO
	ORDER BY UEN
     ''',db_conex)

#### Filtro y creo columnas de Naftas Y gasoleos

df_despachos = df_despachos.convert_dtypes()
df_despachos = df_despachos.loc[:,df_despachos.columns!="ID"]
turno1 = df_despachos['CODPRODUCTO'] == "EU   "
turno1 = df_despachos[turno1]
df_despachos["EU"]= 1 * turno1["DESPACHOS"]

turno2 = df_despachos['CODPRODUCTO'] == "GO   "
turno2 = df_despachos[turno2]
df_despachos["GO"]= 1 * turno2["DESPACHOS"]

turno3 = df_despachos['CODPRODUCTO'] == "NS   "
turno3 = df_despachos[turno3]
df_despachos["NS"]= 1* turno3["DESPACHOS"]

turno4 = df_despachos['CODPRODUCTO'] == "NU   "
turno4 = df_despachos[turno4]
df_despachos["NU"]= 1* turno4["DESPACHOS"]


df_despachos = df_despachos.groupby(
        ["UEN"]
        , as_index=False
    ).sum()

#### Elimino columnas CODPRODUCTO Y CANTIDAD Dado que la informacion de estas columnas ya las tengo en TURNO 1, 2 Y 3

df_despachos = df_despachos.loc[:,df_despachos.columns!="CODPRODUCTO"]
df_despachos = df_despachos.loc[:,df_despachos.columns!="DESPACHOS"]
df_despachos = df_despachos.loc[:,df_despachos.columns!="index"]
df_despachos = df_despachos.reset_index()

#####CONCATENO TABLAS

despachosT = df_despachos.merge(df_despachosM, on='UEN', how='outer')
despachosT = despachosT.merge(presuMix, on= 'UEN', how= 'outer')
#### Agrupo por UEN
despachosT = despachosT.groupby(
        ["UEN"]
        , as_index=False
    ).sum()
despachosT = despachosT.loc[:,despachosT.columns!="index"]

##### CREO COLUMNAS DE MIX Y DESVIOS 
despachosT = despachosT.assign(MixDG=despachosT["EU_x"]/(despachosT["EU_x"]+despachosT["GO_x"]))
despachosT = despachosT.assign(MixDN=despachosT["NU_x"]/(despachosT["NU_x"]+despachosT["NS_x"]))
despachosT = despachosT.assign(MixMG=despachosT["EU_y"]/(despachosT["EU_y"]+despachosT["GO_y"]))
despachosT = despachosT.assign(MixMN=despachosT["NU_y"]/(despachosT["NU_y"]+despachosT["NS_y"]))
despachosT = despachosT.assign(DesvioDG=(despachosT["MixDG"] / despachosT["Presupuesto Gas Oil"])-1)
despachosT = despachosT.assign(DesvioDN=(despachosT["MixDN"]/ despachosT["Presupuesto Nafta"])-1)
despachosT = despachosT.assign(DesvioMG=(despachosT["MixMG"]/ despachosT["Presupuesto Gas Oil"])-1)
despachosT = despachosT.assign(DesvioMN=(despachosT["MixMN"]/ despachosT["Presupuesto Nafta"])-1)
###########CREO TOTALES
### Creo columna (fila) TOTALES
despachosT.loc["colTOTAL"]= pd.Series(
    despachosT.sum()
    , index=["EU_x","GO_x","NS_x","NU_x","EU_y","GO_y","NS_y","NU_y"]
)
despachosT.fillna({"UEN":"TOTAL"}, inplace=True)

#Creo totales de MIX##########
tasa = (despachosT.loc["colTOTAL","EU_x"] / (despachosT.loc["colTOTAL","EU_x"]+despachosT.loc["colTOTAL","GO_x"]))
despachosT.fillna({"MixDG":tasa}, inplace=True)

tasa1 = (despachosT.loc["colTOTAL", "NU_x"]/(despachosT.loc["colTOTAL","NU_x"]+despachosT.loc["colTOTAL","NS_x"]))
despachosT.fillna({"MixDN":tasa1}, inplace=True)

tasa2 = (despachosT.loc["colTOTAL", "EU_y"]/(despachosT.loc["colTOTAL","EU_y"]+despachosT.loc["colTOTAL","GO_y"]))
despachosT.fillna({"MixMG":tasa2}, inplace=True)

tasa3 = (despachosT.loc["colTOTAL", "NU_y"]/(despachosT.loc["colTOTAL","NU_y"]+despachosT.loc["colTOTAL","NS_y"]))
despachosT.fillna({"MixMN":tasa3}, inplace=True)
#Creo totales de PRESUPUESTO############
tasa8 = 0.3333
despachosT.fillna({"Presupuesto Nafta":tasa8}, inplace=True)

tasa9 = 0.4916667
despachosT.fillna({"Presupuesto Gas Oil":tasa9}, inplace=True)
#Creo totales de DESVIOS##############
tasa4 = (despachosT.loc["colTOTAL", "MixDG"]/ despachosT.loc["colTOTAL","Presupuesto Gas Oil"]) -1
despachosT.fillna({"DesvioDG":tasa4}, inplace=True)

tasa5 = (despachosT.loc["colTOTAL", "MixDN"]/ despachosT.loc["colTOTAL","Presupuesto Nafta"]) -1
despachosT.fillna({"DesvioDN":tasa5}, inplace=True)

tasa6 = (despachosT.loc["colTOTAL", "MixMG"]/ despachosT.loc["colTOTAL","Presupuesto Gas Oil"]) -1
despachosT.fillna({"DesvioMG":tasa6}, inplace=True)

tasa7 = (despachosT.loc["colTOTAL", "MixMN"]/ despachosT.loc["colTOTAL","Presupuesto Nafta"]) -1
despachosT.fillna({"DesvioMN":tasa7}, inplace=True)

#### RENOMBRO COLUMNAS
despachosT = despachosT.rename({'UEN': 'UEN', "MixDG": "Mix Gas Oil Diario", "MixDN" :"Mix Nafta Diario",
"MixMG" : "Mix Gas Oil Acumulado", "MixMN" : "Mix Nafta Acumulado", "DesvioDG": "Desvio Gas Oil Diario",
"DesvioDN":"Desvio Nafta Diario", "DesvioMG": "Desvio Gas Oil Acumulado", "DesvioMN":"Desvio Nafta Acumulado" }, axis=1)
##### ELIMINO COLUMNAS QUE NO VAN A ENTRAR EN EL INFORME
despachosT = despachosT.loc[:,despachosT.columns!="EU_x"]
despachosT = despachosT.loc[:,despachosT.columns!="GO_x"]
despachosT = despachosT.loc[:,despachosT.columns!="NU_x"]
despachosT = despachosT.loc[:,despachosT.columns!="NS_x"]
despachosT = despachosT.loc[:,despachosT.columns!="EU_y"]
despachosT = despachosT.loc[:,despachosT.columns!="GO_y"]
despachosT = despachosT.loc[:,despachosT.columns!="NU_y"]
despachosT = despachosT.loc[:,despachosT.columns!="NS_y"]

##### ORDENO COLUMNAS 
despachosT = despachosT.reindex(columns= ["UEN","Mix Gas Oil Acumulado","Presupuesto Gas Oil"
,"Desvio Gas Oil Acumulado"
,"Mix Nafta Acumulado","Presupuesto Nafta","Desvio Nafta Acumulado"])

####### MODIFICO DATAFRAME

def _estiladorVtaTituloD(df, list_Col_Num, list_Col_Perc, titulo):
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
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
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
    evitarTotales = df.index.get_level_values(0)
    resultado = resultado.background_gradient(
        cmap="RdYlGn" # Red->Yellow->Green
        ,vmin=-0.1
        ,vmax=0.1
        ,subset=pd.IndexSlice["Desvio Gas Oil Acumulado"]
    )
    resultado = resultado.background_gradient(
        cmap="RdYlGn" # Red->Yellow->Green
        ,vmin=-0.1
        ,vmax=0.1
        ,subset=pd.IndexSlice["Desvio Nafta Acumulado"]
    )
   
    return resultado

#  columnas sin decimales
numCols = [ 
         ]
# Columnas Porcentaje
percColsPen = [
        "Mix Gas Oil Acumulado"
        ,"Mix Nafta Acumulado"
        ,"Desvio Nafta Acumulado"
        ,"Desvio Gas Oil Acumulado"
        ,"Presupuesto Nafta"
        ,"Presupuesto Gas Oil"
]



### Aplico la modificacion al Dataframe
despachosT = _estiladorVtaTituloD(despachosT,numCols,percColsPen, "MIX G3/G2 Ejecucion Presupuestaria")

ubicacion = "C:/Informes/PRESUPUESTO/"
nombrePen = "Info_MIX.png"
nombrePenDiario = "Info_Penetracion_Lubri.png"
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

df_to_image(despachosT, ubicacion, nombrePen)



#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)









