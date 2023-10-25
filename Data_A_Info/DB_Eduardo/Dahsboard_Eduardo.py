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
## Importo Datos ##
from DB_Eduardo.DB_Eduardo_Comercial import mixGasoleos,cueta_cte_yer,varIntermensual,volIntermensual
from DB_Eduardo.DB_Eduardo_Deuda_MesActual import total as deuda,adelantado as adelanto
from DB_Eduardo.DB_Eduardo_Deuda_MesAnterior import total as deudaMA,adelantado as adelantoMA
from DB_Eduardo.DB_Eduardo_Economico_Mes_Actual import mbcGasoleosGO,mbcGasoleosEU,precioCartelypf_GO,precioCartelypf_EU,precioCartelDapsa_GO,precioCartelDapsa_EU
from DB_Eduardo.DB_Eduardo_Economico_Mes_Anterior import mbcGasoleosGO as mbcGasoleosGO_MA,mbcGasoleosEU as mbcGasoleosEU_MA
from DB_Eduardo.DB_Eduardo_Rent_ctecte import vtaCtaCteGO,vtaCtaCteEU,deudaCtaCte
import sys
import pathlib
import datetime


tasaInteres=0.09
# Establecer el locale en español

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

########################################

# Obtener la fecha actual
fecha_actual = datetime.date.today()
# Obtener el primer día del mes actual
primer_dia_mes_actual = fecha_actual.replace(day=1)
# Obtener el último día del mes anterior
ultimo_dia_mes_anterior = primer_dia_mes_actual - datetime.timedelta(days=1)
# Obtener el primer día del mes anterior
primer_dia_mes_anterior = ultimo_dia_mes_anterior.replace(day=1)
# Obtener la fecha actual
fecha_actual = datetime.date.today()
# Obtener el mes actual
mes_actual = fecha_actual.month
# Obtener el año actual
anio_actual = fecha_actual.year
# Calcular el mes hace dos meses atrás
mes_dos_atras = mes_actual - 2
# Calcular el año dos meses atrás
if mes_dos_atras <= 0:
    mes_dos_atras += 12
    anio_dos_atras = anio_actual - 1
else:
    anio_dos_atras = anio_actual
# Obtener el primer día del mes dos meses atrás
primer_dia_dos_meses_atras = datetime.date(anio_dos_atras, mes_dos_atras, 1)


primer_dia_dos_meses_atras = primer_dia_dos_meses_atras
ultimo_dia_mes_anterior = ultimo_dia_mes_anterior


######### PRECIO DOLAR ################

import requests
from bs4 import BeautifulSoup

url = "https://www.dolarhoy.com/cotizaciondolarblue"
response = requests.get(url)
html = response.content
soup = BeautifulSoup(html, "html.parser")

dolar_blue_HOY = soup.find("div", {"class": "value"})

dolar_blue = dolar_blue_HOY.text

dolar_blue = dolar_blue.replace("$", "").strip()

dolar_blue=float(dolar_blue)

fecha_mes_Actual = datetime.datetime.strptime(primer_dia_mes_anterior.strftime("%Y-%m-%d"), "%Y-%m-%d")
nombre_mes_Actual = fecha_mes_Actual.strftime("%B").capitalize()

fecha_mes_Anterior = datetime.datetime.strptime(primer_dia_dos_meses_atras.strftime("%Y-%m-%d"), "%Y-%m-%d")
nombre_mes_Anterior = fecha_mes_Anterior.strftime("%B").capitalize()



# BANDERA
def _bandera(uen):
    if uen in [
        "AZCUENAGA           "
        , "LAMADRID            "
        , "PERDRIEL            "
        , "PERDRIEL2           "
        , "PUENTE OLIVE        "
        , "SAN JOSE            "
        , 'YPF'
    ]:
        return "YPF"
    else:
        return "DAPSA"

### Creo una columna con el nombre de la bandera
mbcGasoleosGO["BANDERA"] = mbcGasoleosGO.apply(
    lambda row: _bandera(row["UEN"])
        , axis= 1
)
mbcGasoleosGO_MA["BANDERA"] = mbcGasoleosGO_MA.apply(
    lambda row: _bandera(row["UEN"])
        , axis= 1
)
mbcGasoleosEU_MA["BANDERA"] = mbcGasoleosEU_MA.apply(
    lambda row: _bandera(row["UEN"])
        , axis= 1
)
mbcGasoleosEU["BANDERA"] = mbcGasoleosEU.apply(
    lambda row: _bandera(row["UEN"])
        , axis= 1
)
### Separo por Bandera en diferentes variables
mbcGasoleosGO_Dapsa = mbcGasoleosGO.loc[(mbcGasoleosGO["BANDERA"] == 'DAPSA'),:]
mbcGasoleosGO_YPF = mbcGasoleosGO.loc[(mbcGasoleosGO["BANDERA"] == 'YPF'),:]

mbcGasoleosGO_MA_Dapsa = mbcGasoleosGO_MA.loc[(mbcGasoleosGO_MA["BANDERA"] == 'DAPSA'),:]
mbcGasoleosGO_MA_YPF = mbcGasoleosGO_MA.loc[(mbcGasoleosGO_MA["BANDERA"] == 'YPF'),:]

mbcGasoleosEU_Dapsa = mbcGasoleosEU.loc[(mbcGasoleosEU["BANDERA"] == 'DAPSA'),:]
mbcGasoleosEU_YPF = mbcGasoleosEU.loc[(mbcGasoleosEU["BANDERA"] == 'YPF'),:]

mbcGasoleosEU_MA_Dapsa = mbcGasoleosEU_MA.loc[(mbcGasoleosEU_MA["BANDERA"] == 'DAPSA'),:]
mbcGasoleosEU_MA_YPF = mbcGasoleosEU_MA.loc[(mbcGasoleosEU_MA["BANDERA"] == 'YPF'),:]

## Creo variables de MBC Totales por Bandera
mbcGasoleos_MA_Dapsa= (mbcGasoleosGO_MA_Dapsa["MBC Acumulado $"].sum() + mbcGasoleosEU_MA_Dapsa["MBC Acumulado $"].sum())

mbcGasoleos_MA_YPF= (mbcGasoleosEU_MA_YPF["MBC Acumulado $"].sum()+ mbcGasoleosGO_MA_YPF["MBC Acumulado $"].sum())

mbcGasoleos_Dapsa= (mbcGasoleosGO_Dapsa["MBC Acumulado $"].sum() + mbcGasoleosEU_Dapsa["MBC Acumulado $"].sum())

mbcGasoleos_YPF= (mbcGasoleosEU_YPF["MBC Acumulado $"].sum()+ mbcGasoleosGO_YPF["MBC Acumulado $"].sum())




### Creo DF Renta Mensual Gasoleos
#### INDICADORES ECONOMICOS
# RENTA MENSUAL GASOLEOS
contr_M = pd.DataFrame()
contr_M.loc[1,'BANDERA']= 'DAPSA'
contr_M.loc[1,f'MBC {nombre_mes_Anterior}']= mbcGasoleos_MA_Dapsa
contr_M.loc[1,f'Proporcion {nombre_mes_Anterior}']= mbcGasoleos_MA_Dapsa/(mbcGasoleos_MA_Dapsa+mbcGasoleos_MA_YPF)
contr_M.loc[1,f'MBC {nombre_mes_Actual}']= mbcGasoleos_Dapsa
contr_M.loc[1,f'Proporcion {nombre_mes_Actual}']= mbcGasoleos_Dapsa/(mbcGasoleos_Dapsa+mbcGasoleos_YPF)
contr_M.loc[1,f'Incremental']= mbcGasoleos_Dapsa-mbcGasoleos_MA_Dapsa

contr_M.loc[0,'BANDERA']= 'YPF'
contr_M.loc[0,f'MBC {nombre_mes_Anterior}']= mbcGasoleos_MA_YPF
contr_M.loc[0,f'Proporcion {nombre_mes_Anterior}']=mbcGasoleos_MA_YPF/(mbcGasoleos_MA_Dapsa+mbcGasoleos_MA_YPF)
contr_M.loc[0,f'MBC {nombre_mes_Actual}']= mbcGasoleos_YPF
contr_M.loc[0,f'Proporcion {nombre_mes_Actual}']= mbcGasoleos_YPF/(mbcGasoleos_Dapsa+mbcGasoleos_YPF)
contr_M.loc[0,f'Incremental']= mbcGasoleos_YPF-mbcGasoleos_MA_YPF

## Precios Dolar YPF------

precioDolar = pd.DataFrame()
precioDolar.loc[1,'BANDERA']='DAPSA'
precioDolar.loc[1,'G2']= precioCartelDapsa_GO/dolar_blue
precioDolar.loc[1,'G3']= precioCartelDapsa_EU/dolar_blue

precioDolar.loc[0,'BANDERA']='YPF'
precioDolar.loc[0,'G2']= precioCartelypf_GO/dolar_blue
precioDolar.loc[0,'G3']= precioCartelypf_EU/dolar_blue

## Margen Bruto Contributivo

mbc = pd.DataFrame()
mbc.loc[1,'BANDERA']='DAPSA'
mbc.loc[1,'G2 %']= mbcGasoleosGO_Dapsa["MBC Acumulado $"].sum()/mbcGasoleosGO_Dapsa["Ventas Acumuladas $"].sum()
mbc.loc[1,'G3 %']= mbcGasoleosEU_Dapsa["MBC Acumulado $"].sum()/mbcGasoleosEU_Dapsa["Ventas Acumuladas $"].sum()
mbc.loc[1,'G2 $']= (mbcGasoleosGO_Dapsa["MBC Acumulado $"].sum()/mbcGasoleosGO_Dapsa["Ventas Acumuladas $"].sum())*precioCartelDapsa_GO
mbc.loc[1,'G3 $']= (mbcGasoleosEU_Dapsa["MBC Acumulado $"].sum()/mbcGasoleosEU_Dapsa["Ventas Acumuladas $"].sum())*precioCartelDapsa_EU

mbc.loc[0,'BANDERA']='YPF'
mbc.loc[0,'G2 %']= mbcGasoleosGO_YPF["MBC Acumulado $"].sum()/mbcGasoleosGO_YPF["Ventas Acumuladas $"].sum()
mbc.loc[0,'G3 %']= mbcGasoleosEU_YPF["MBC Acumulado $"].sum()/mbcGasoleosEU_YPF["Ventas Acumuladas $"].sum()
mbc.loc[0,'G2 $']= (mbcGasoleosGO_YPF["MBC Acumulado $"].sum()/mbcGasoleosGO_YPF["Ventas Acumuladas $"].sum())*precioCartelypf_GO
mbc.loc[0,'G3 $']= (mbcGasoleosEU_YPF["MBC Acumulado $"].sum()/mbcGasoleosEU_YPF["Ventas Acumuladas $"].sum())*precioCartelypf_EU


####### INDICADORES FINANCIEROS
deudaNominal=pd.DataFrame()
deudaNominal.loc[0,f'{nombre_mes_Anterior}'] = deudaMA['SALDOCUENTA'].sum()
deudaNominal.loc[0,f'{nombre_mes_Actual}'] = deuda['SALDOCUENTA'].sum()
deudaNominal.loc[0,'INCREMENTAL'] = (deuda['SALDOCUENTA'].sum() - deudaMA['SALDOCUENTA'].sum())

balanceSaldos=pd.DataFrame()
balanceSaldos.loc[0,f'{nombre_mes_Anterior}'] = deudaMA['SALDOCUENTA'].sum()+adelantoMA
balanceSaldos.loc[0,f'{nombre_mes_Actual}'] = deuda['SALDOCUENTA'].sum()+adelanto
balanceSaldos.loc[0,'INCREMENTAL'] = -((deuda['SALDOCUENTA'].sum()+adelanto) -(deudaMA['SALDOCUENTA'].sum()+adelantoMA))


#Rentabilidad Cta Ctes
rentaGO=vtaCtaCteGO*mbc.loc[0,'G2 %']
rentaEU=vtaCtaCteEU*mbc.loc[0,'G3 %']
deudaCtaCte=deudaCtaCte.loc[(deudaCtaCte['Días Desde Última Compra'] < 60),:]
deudaCF= deuda['SALDOCUENTA'].sum() *((1+tasaInteres)**(deudaCtaCte['Dias de Venta Adeudada'].mean()/30)-1)

rentabilidad_Cta_cte=pd.DataFrame()
rentabilidad_Cta_cte.loc[0,'Renta Ctas Negativas']= rentaGO+rentaEU
rentabilidad_Cta_cte.loc[0,'Costo Financiero']= deudaCF
rentabilidad_Cta_cte.loc[0,'Resultado Neto']= (rentabilidad_Cta_cte.loc[0,'Renta Ctas Negativas'] + rentabilidad_Cta_cte.loc[0,'Costo Financiero'])


presupuesto=pd.DataFrame()
presupuesto.loc[0,'Presupuesto']= 210222700
presupuesto.loc[0,'Ejecucion']=  164530771
presupuesto.loc[0,'Resultado']= presupuesto.loc[0,'Ejecucion']-presupuesto.loc[0,'Presupuesto']
presupuesto.loc[0,'%']= (presupuesto.loc[0,'Ejecucion']-presupuesto.loc[0,'Presupuesto'])/presupuesto.loc[0,'Ejecucion']






######### LE DOY FORMATO AL DATAFRAME
def _estiladorVtaTituloDTotal(df,list_Col_Numpes,listaporcentaje,numCols,numDecimal,numPesos, titulo):
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
        .format("{:,.0f}", subset=numCols) \
        .format("{:,.2%}", subset=listaporcentaje) \
        .format("{:,.2f}", subset=numDecimal) \
        .format("$ {:,.0f}", subset=numPesos) \
        .hide(axis=0) \
        .set_caption(
            "   -    "
            +"<br>"
            +titulo
            + "<br>") \
        .set_properties(subset= list_Col_Numpes+listaporcentaje+numCols+numDecimal+numPesos
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
        ]) 
    return resultado

#  columnas Contribucion
letrasCols=['BANDERA']
porcCols = [f'Proporcion {nombre_mes_Anterior}',f'Proporcion {nombre_mes_Actual}']
numCols = []
numDecimal = []
numPesos = [f'MBC {nombre_mes_Anterior}',f'MBC {nombre_mes_Actual}','Incremental']

#  columnas Precio Dolar YPF
letrasCols1=['BANDERA']
porcCols1 = []
numCols1 = []
numDecimal1 = ['G2','G3']
numPesos1 = []

#  columnas Margen Bruto Contributivo
letrasCols2=['BANDERA']
porcCols2 = ['G2 %','G3 %']
numCols2 = []
numDecimal2 = []
numPesos2 = ['G2 $','G3 $']
#  columnas Deuda Nominal
letrasCols3=[]
porcCols3 = []
numCols3 = [f'{nombre_mes_Anterior}',f'{nombre_mes_Actual}','INCREMENTAL']
numDecimal3 = []
numPesos3 = []

#  columnas Balance Saldos
letrasCols4=[]
porcCols4 = []
numCols4 = [f'{nombre_mes_Anterior}',f'{nombre_mes_Actual}','INCREMENTAL']
numDecimal4 = []
numPesos4 = []

#  columnas Rentabilidad Cta Cte
letrasCols5=[]
porcCols5 = []
numCols5 = ['Renta Ctas Negativas','Costo Financiero','Resultado Neto']
numDecimal5 = []
numPesos5 = []

#  columnas Presupuesto
letrasCols6=[]
porcCols6 = ['%']
numCols6 = ['Presupuesto','Ejecucion','Resultado']
numDecimal6 = []
numPesos6 = []


### Aplico estilador

contr_M = _estiladorVtaTituloDTotal(contr_M,letrasCols,porcCols,numCols,numDecimal,numPesos, "RENTA MENSUAL GASOLEOS")
precioDolar = _estiladorVtaTituloDTotal(precioDolar,letrasCols1,porcCols1,numCols1,numDecimal1,numPesos1, "PRECIOS DOLAR YPF/DAPSA")
mbc = _estiladorVtaTituloDTotal(mbc,letrasCols2,porcCols2,numCols2,numDecimal2,numPesos2, "MARGEN BRUTO CONTRIBUTIVO")
deudaNominal = _estiladorVtaTituloDTotal(deudaNominal,letrasCols3,porcCols3,numCols3,numDecimal3,numPesos3, "DEUDA NOMINAL")
balanceSaldos = _estiladorVtaTituloDTotal(balanceSaldos,letrasCols4,porcCols4,numCols4,numDecimal4,numPesos4, "BALANCE SALDOS")
rentabilidad_Cta_cte = _estiladorVtaTituloDTotal(rentabilidad_Cta_cte,letrasCols5,porcCols5,numCols5,numDecimal5,numPesos5, "RENTABILIDAD NETA CTAS CTES (RENTA - CF)")
presupuesto = _estiladorVtaTituloDTotal(presupuesto,letrasCols6,porcCols6,numCols6,numDecimal6,numPesos6, "Resultados Renta Presupuestada")
###### DEFINO NOMBRE Y UBICACION DE DONDE SE VA A GUARDAR LA IMAGEN

ubicacion = "C:/Informes/DB_Eduardo/"
nombrepng = "Renta_Mensual_Gasoleos.png"
nombrepng1 = "Mix_Gasoleos.png"
nombrepng2 = "ctdo_cte_yer.png"
nombrepng3 = "var_Intermensual.png"
nombrepng4 = "vol_Intermensual.png"
nombrepng5 = "Precio_Dolar_YPF.png"
nombrepng6 = "MBC_Dasboard_Eduardo.png"
nombrepng7 = "Deuda_Nominal.png"
nombrepng8 = "Balance_Saldos.png"
nombrepng9 = 'Rentabilidad_CtaCte.png'
nombrepng10 = 'Rdo_Renta_Presupuestada.png'
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

df_to_image(contr_M, ubicacion, nombrepng)
df_to_image(mixGasoleos, ubicacion, nombrepng1)
df_to_image(cueta_cte_yer, ubicacion, nombrepng2)
df_to_image(varIntermensual, ubicacion, nombrepng3)
df_to_image(volIntermensual, ubicacion, nombrepng4)
df_to_image(precioDolar, ubicacion, nombrepng5)
df_to_image(mbc, ubicacion, nombrepng6)
df_to_image(deudaNominal, ubicacion, nombrepng7)
df_to_image(balanceSaldos, ubicacion, nombrepng8)
df_to_image(rentabilidad_Cta_cte, ubicacion, nombrepng9)
df_to_image(presupuesto, ubicacion, nombrepng10)

def _append_images(listOfImages, direction='vertical',
                  bg_color=(255,255,255), alignment='center'):
    """
    Appends images in horizontal/vertical direction.

    Args:
        listOfImages: List of images with complete path
        direction: direction of concatenation, 'horizontal' or 'vertical'
        bg_color: Background color (default: white)
        alignment: alignment mode if images need padding;
           'left', 'right', 'top', 'bottom', or 'center'

    Returns:
        Concatenated image as a new PIL image object.
    """
    images = [Image.open(x) for x in listOfImages]
    widths, heights = zip(*(i.size for i in images))

    if direction=='horizontal':
        new_width = sum(widths)
        new_height = max(heights)
    else:
        new_width = max(widths)
        new_height = sum(heights)

    new_im = Image.new('RGB', (new_width, new_height), color=bg_color)

    offset = 0
    for im in images:
        if direction=='horizontal':
            y = 0
            if alignment == 'center':
                y = int((new_height - im.size[1])/2)
            elif alignment == 'bottom':
                y = new_height - im.size[1]
            new_im.paste(im, (offset, y))
            offset += im.size[0]
        else:
            x = 0
            if alignment == 'center':
                x = int((new_width - im.size[0])/2)
            elif alignment == 'right':
                x = new_width - im.size[0]
            new_im.paste(im, (x, offset))
            offset += im.size[1]

    return new_im


### Indicadores Comerciales

listaImgComerciales = [ubicacion + nombrepng3, ubicacion + nombrepng4,ubicacion+nombrepng2]
# Merge DFs images vertically and save it as a .png
fusionImgComercial1 = _append_images(listaImgComerciales, direction="vertical")
fusionImgComercial1.save(ubicacion + "DE_Comercial_1.png")

listaImgComerciales2 = [ubicacion + nombrepng1, ubicacion + 'DE_Comercial_1.png']
# Merge DFs images vertically and save it as a .png
fusionImgComercial2 = _append_images(listaImgComerciales2, direction="horizontal")
fusionImgComercial2.save(ubicacion + "DE_Comercial_Final.png")

### Indicadores Economicos

listaImgEconomicos = [ubicacion + nombrepng5, ubicacion + nombrepng6]
# Merge DFs images vertically and save it as a .png
listaImgEconomicos = _append_images(listaImgEconomicos, direction="vertical")
listaImgEconomicos.save(ubicacion + "DE_Economico_1.png")

listaImgEconomicos2 = [ubicacion + nombrepng, ubicacion + 'DE_Economico_1.png']
# Merge DFs images vertically and save it as a .png
listaImgEconomicos2 = _append_images(listaImgEconomicos2, direction="horizontal")
listaImgEconomicos2.save(ubicacion + "DE_Economico_Final.png")

### Indicadores Financieros

listaImgFinancieros = [ubicacion + nombrepng7, ubicacion + nombrepng8]
# Merge DFs images vertically and save it as a .png
listaImgFinancieros = _append_images(listaImgFinancieros, direction="vertical")
listaImgFinancieros.save(ubicacion + "DE_Financieros_1.png")



listaImgEconomicos2 = [ubicacion + nombrepng9, ubicacion + 'DE_Financieros_1.png']
# Merge DFs images vertically and save it as a .png
listaImgEconomicos2 = _append_images(listaImgEconomicos2, direction="horizontal")
listaImgEconomicos2.save(ubicacion + "DE_Financieros_Final.png")




#########
# Timer #
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)


