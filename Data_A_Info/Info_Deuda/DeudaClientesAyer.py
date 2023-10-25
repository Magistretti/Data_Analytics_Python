﻿import os
import math
import re
import numpy as np
from DatosLogin import login
from PIL import Image

import pandas as pd
import dataframe_image as dfi
import pyodbc #Library to connect to Microsoft SQL Server
from DatosLogin import login #Holds connection data to the SQL Server
import sys
import pathlib
from calendar import monthrange
from datetime import datetime
from datetime import timedelta
import datetime
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))
import logging
import matplotlib.pyplot as plt
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

###########################################################################
##############################   Deuda Clientes ######################################
###########################################################################
deuda = pd.read_sql(
 ''' 
   select CAST(E.[NROCLIENTE] as VARCHAR) as 'NROCLIENTE'
            ,RTRIM(E.NOMBRE) as 'NOMBRE'
            ,E.[saldo Cuenta],E.LIMITECREDITO as 'Acuerdo de Descubierto $' FROM 
(SELECT vta.NROCLIENTE,sum(vta.importe) as 'saldo Cuenta',CLI.NOMBRE,CLI.LIMITECREDITO
  FROM [Rumaos].[dbo].[VTCtaCte] as vta
  INNER JOIN dbo.FacCli as Cli with (NOLOCK)
  ON vta.NROCLIENTE = Cli.NROCLIPRO
  where vta.NROCLIENTE > '100000'
   and Cli.ListaSaldoCC = 1
   and vta.FECHASQL <= DATEADD(day, -1, CAST(GETDATE() AS date))
   group by vta.NROCLIENTE,CLI.NOMBRE,CLI.LIMITECREDITO) AS E 
   ORDER BY E.NOMBRE
 ''' 
  ,db_conex)
deuda = deuda.convert_dtypes()
deuda = deuda.fillna(0)
####################  saldo remitos pendiente de facturacion HASTA EL MES PASADO #############################3
deudaPendFactu = pd.read_sql( '''
        select CAST(E.[NROCLIENTE] as VARCHAR) as 'NROCLIENTE'
        ,sum(e.IMPTOTAL) as 'REMPENDFACTURACION' from
		(select vta.NROCLIENTE,vta.FECHASQL as FECHASQLFAC,rem.FECHASQL AS FECHASQLREM,rem.IMPTOTAL, rem.NROREMITO  
		FROM [Rumaos].[dbo].FacRemGen as rem
		left outer join [VTCtaCte] as vta on
		rem.NROCLIPRO=vta.NROCLIENTE
		and  rem.NROCOMP = vta.NROCOMP
		and rem.PTOVTAFAC=vta.PTOVTA
		AND rem.LETRACOMP = vta.LETRACOMP
		and rem.TIPO = vta.TIPOCOMP

		where vta.NROCLIENTE > '100000'
		and rem.FECHASQL <= DATEADD(DAY,-1,CAST(GETDATE() AS date)) ) as e
		where e.FECHASQLFAC  > DATEADD(DAY,-1,CAST(GETDATE() AS date))
		GROUP BY E.NROCLIENTE
'''
,db_conex)
deudaPendFactu = deudaPendFactu.convert_dtypes()
####################  saldo remitos pendiente de facturacion HASTA HOY #############################3
deudaPendFactum = pd.read_sql( '''
        select CAST([NROCLIPRO] as VARCHAR) as 'NROCLIENTE'
        ,SUM(IMPTOTAL) as 'REMPENDFACTURACIONHOY'  FROM [Rumaos].[dbo].FacRemGen where 
         PTOVTAFAC = 0
        AND NROCOMP = 0
        AND IMPTOTAL > 0
        AND FECHASQL <= DATEADD(DAY,-1,CAST(GETDATE() AS date))
		and NROCLIPRO > '100000'
		group by NROCLIPRO
'''
,db_conex)
deudaPendFactum = deudaPendFactum.convert_dtypes()

####################  VENTAS PROMEDIO 60 DIAS #############################3
ventasprom = pd.read_sql( '''
SELECT
            CAST(FRD.[NROCLIENTE] as VARCHAR) as 'NROCLIENTE'
            ,RTRIM(Cli.NOMBRE) as 'NOMBRE'
			, sum(FRD.importe)/60 as 'Venta Prom. 60 Dias $'
            FROM [Rumaos].[dbo].[FacRemDet] as FRD with (NOLOCK)
            INNER JOIN dbo.FacCli as Cli with (NOLOCK)
                ON FRD.NROCLIENTE = Cli.NROCLIPRO
            LEFT OUTER JOIN dbo.Vendedores as Vend with (NOLOCK)
	            ON Cli.NROVEND = Vend.NROVEND

            where FRD.NROCLIENTE > '100000'
                and Cli.ListaSaldoCC = 1
                and FECHASQL >= DATEADD(day, -61, CAST(GETDATE() AS date))
				and FECHASQL <= DATEADD(day, -1, CAST(GETDATE() AS date))

            group by FRD.NROCLIENTE, Cli.NOMBRE
'''
,db_conex)
ventasprom = ventasprom.convert_dtypes()
####################  dias desde la ultima compra #############################3

ultimacompra = pd.read_sql(
 ''' 
   SELECT
            CAST(FRD.[NROCLIENTE] as VARCHAR) as 'NROCLIENTE'

            ,MIN(CAST(FRD.[FECHASQL] as date)) as 'FECHA_1erRemito'
            ,MAX(CAST(FRD.[FECHASQL] as date)) as 'FECHA_UltRemito'

            ----Días Entre Remitos
            ,IIF(MIN(CAST(FRD.[FECHASQL] as date)) = MAX(CAST(FRD.[FECHASQL] as date))
            	, -1
            	, DATEDIFF(DAY,MAX(CAST(FRD.[FECHASQL] as date)),MIN(CAST(FRD.[FECHASQL] as date)))
            ) as 'Días Entre Remitos'

            --,sum(FRD.[IMPORTE]) as 'ConsumoHistorico'

            ----Consumo Diario
            --,sum(FRD.[IMPORTE])/IIF(MIN(CAST(FRD.[FECHASQL] as date)) = MAX(CAST(FRD.[FECHASQL] as date))
            --	, -1
            --	, DATEDIFF(DAY,MAX(CAST(FRD.[FECHASQL] as date)),MIN(CAST(FRD.[FECHASQL] as date)))
            --) as 'Consumo Diario'


            , DATEDIFF(DAY, MAX(CAST(FRD.[FECHASQL] as date)), DATEADD(DAY,-1,CAST(GETDATE() AS date))) as 'Días Desde Última Compra'
            FROM [Rumaos].[dbo].[FacRemDet] as FRD with (NOLOCK)

            where FRD.NROCLIENTE > '100000'
                and FECHASQL >= '20140101' and FECHASQL <= DATEADD(DAY,-1,CAST(GETDATE() AS date))
            group by FRD.NROCLIENTE
            order by FRD.NROCLIENTE
 ''' 
  ,db_conex)
ultimacompra = ultimacompra.convert_dtypes()
ultimacompra = ultimacompra.fillna(0)
################# PRECIO DE GO A LA FECHA ##############################

precioGO = pd.read_sql( '''
    select uen,PRECARTEL from EmpVenta where FECHASQL = DATEADD(DAY,-1,CAST(GETDATE() AS date))
    and CODPRODUCTO = 'GO'
    and UEN = 'PERDRIEL'
    group by PRECARTEL,UEN
'''
,db_conex)
precioGO = precioGO.convert_dtypes()
precioGO = precioGO['PRECARTEL'].mean()

###########################################3

deuda=deuda.merge(deudaPendFactu,on=['NROCLIENTE'],how='outer')
deuda=deuda.merge(deudaPendFactum,on=['NROCLIENTE'],how='outer')
deuda=deuda.merge(ultimacompra,on=['NROCLIENTE'],how='outer')
deuda=deuda.merge(ventasprom,on=['NROCLIENTE','NOMBRE'],how='outer')
deuda=deuda.loc[(deuda['NOMBRE'] != '<NA>') ,:]  
deuda['Venta Prom. 60 Dias $']=deuda['Venta Prom. 60 Dias $'].fillna(1)


deuda=deuda.fillna(0)


deuda['SALDOCUENTA']=(deuda['saldo Cuenta']-deuda['REMPENDFACTURACION']-deuda['REMPENDFACTURACIONHOY'])
adelantado=deuda
deuda=deuda.loc[(deuda['SALDOCUENTA'] < -1000),:]
adelantado=adelantado.loc[(adelantado['SALDOCUENTA'] > 1000),:]
adelantado = adelantado['SALDOCUENTA'].sum()



## Creo columnas dias de venta adeudada y el cupo disponible
deuda['Dias de Venta Adeudada']=(deuda['SALDOCUENTA']/deuda['Venta Prom. 60 Dias $'])*-1
deuda['Cupo Disponible']= deuda['SALDOCUENTA']+deuda['Acuerdo de Descubierto $']
deuda = deuda.fillna(0)
# Funcion para categorizar a los clientes 
def categorizar_deuda(dias_adeuda,dias_ultima):
    if dias_adeuda < 20:
        if dias_ultima < 20:
            return '1-Normal'
        elif dias_ultima < 40:
            return '2-Moroso'
        elif dias_ultima < 60:
            return '3-Moroso Grave'
        else:
            return '4-Gestion Judicial'
    elif dias_adeuda < 40:
        if dias_ultima < 20:
            return '2-Moroso'
        elif dias_ultima < 60:
            return '3-Moroso Grave'
        else:
            return '4-Gestion Judicial'
    elif dias_adeuda < 60:
        return '3-Moroso Grave'
    else:
        return '4-Gestion Judicial'

deuda['Tipo de deudor'] = deuda.apply(lambda x: categorizar_deuda(x['Dias de Venta Adeudada'], x['Días Desde Última Compra']), axis=1)

deuda['Acuerdo'] = deuda['Cupo Disponible'].apply(lambda x: 'Deudor Excedido' if x < 0 else 'Deudor No Excedido')

## Funcion para calcular el interes a cobrarle a cada cliente

def calcular_interes(row):
    if row['Dias de Venta Adeudada'] > 20:
        interes_diario = 1.0 / 365  # Tasa de interés diaria (100% anual)
        dias_adeudados = row['Dias de Venta Adeudada'] - 20  # Días de venta adeudados que exceden los 20 días
        saldo_adeudado = -row['SALDOCUENTA']  # Saldo adeudado (en valor absoluto)
        interes_cobrado = saldo_adeudado * interes_diario * dias_adeudados  # Interés cobrado
        return interes_cobrado
    else:
        return 0

for i in deuda.index:
    if deuda.loc[i,'Dias de Venta Adeudada'] > 120:
        deuda.loc[i,'Dias de Venta Adeudada']=120
        
        
# Aplicar la función a cada fila del DataFrame para crear la nueva columna
deuda['Interes por Mora'] = deuda.apply(calcular_interes, axis=1)

        
deuda = deuda.reindex(columns=['NOMBRE','SALDOCUENTA','Días Desde Última Compra','Acuerdo de Descubierto $','Dias de Venta Adeudada','Cupo Disponible','Interes por Mora','Tipo de deudor','Acuerdo'])
deuda = deuda.fillna(0)
## Agrupo por tipo de deudor
normal= deuda.loc[deuda["Tipo de deudor"] == "1-Normal",:]
moroso= deuda.loc[deuda["Tipo de deudor"] == "2-Moroso",:]
morosoG= deuda.loc[deuda["Tipo de deudor"] == "3-Moroso Grave",:]
gestionJ= deuda.loc[deuda["Tipo de deudor"] == "4-Gestion Judicial",:]
normal=normal.sort_values(['NOMBRE'])
### Creo colores para categorizar clientes
color_map = {
    '1-Normal': 'green',
    '2-Moroso': 'orange',
    '3-Moroso Grave': 'red',
    '4-Gestion Judicial': 'maroon',
    'TOTAL':'black'
}
color_map2 = {
    'Deudor No Excedido': 'green',
    'Deudor Excedido': 'red',

}


## Creo la tabla de Totales dependiendo del tipo de deudor

total = deuda.reindex(columns=['SALDOCUENTA','Tipo de deudor'])
total = total.groupby(
        ["Tipo de deudor"]
        , as_index=False
    ).sum()

total['Participacion']=total['SALDOCUENTA']/total['SALDOCUENTA'].sum()

total.loc["colTOTAL"]= pd.Series(
    total.sum()
    , index=["SALDOCUENTA"]
)
total.fillna({"Tipo de deudor":"TOTAL"}, inplace=True)
total.fillna({"Participacion": total['SALDOCUENTA'].sum()/total['SALDOCUENTA'].sum()}, inplace=True)




######### LE DOY FORMATO AL DATAFRAME
def _estiladorVtaTituloD(df,list_Col_Perc,list_Col_Numpes, list_Col_Num,listaporcentaje, titulo):
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
        .format("$ {0:,.0f}", subset=list_Col_Numpes) \
        .format("{0:,.0f}", subset=list_Col_Num) \
        .format("{:,.2%}", subset=listaporcentaje) \
        .hide_index() \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>") \
        .set_properties(subset= list_Col_Perc + list_Col_Num +list_Col_Numpes+listaporcentaje
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
            , axis=1)\
        .applymap(lambda x: f'background-color: {color_map[x]}', subset=['Tipo de deudor'])\
        .applymap(lambda x: f'background-color: {color_map2[x]}', subset=['Acuerdo'])
     
    return resultado


######### LE DOY FORMATO AL DATAFRAME
def _estiladorVtaTituloDTotal(df,list_Col_Numpes,listaporcentaje, titulo):
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
        .format("$ {0:,.0f}", subset=list_Col_Numpes) \
        .format("{:,.2%}", subset=listaporcentaje) \
        .hide_index() \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>") \
        .set_properties(subset= list_Col_Numpes+listaporcentaje
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
        .applymap(lambda x: f'background-color: {color_map[x]}', subset=['Tipo de deudor'])\
        .apply(lambda x: ["background: black" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1)
     
    return resultado

#  columnas sin decimales
numCols1=[]
numCols = [ 'Días Desde Última Compra','Dias de Venta Adeudada']
colpesos=[ 'SALDOCUENTA','Acuerdo de Descubierto $','Cupo Disponible','Interes por Mora']
coldeuda=['SALDOCUENTA']
# Columnas Porcentaje
percColsstr = ['Tipo de deudor','Acuerdo']
percColsstr1 = ['Tipo de deudor']
percCol = ['Participacion']

deudaS = _estiladorVtaTituloD(deuda,percColsstr,colpesos,numCols,numCols1, "Deudores Comerciales")

morosoS = _estiladorVtaTituloD(moroso,percColsstr,colpesos,numCols,numCols1, "Deudores Comerciales Morosos")

morosoGS = _estiladorVtaTituloD(morosoG,percColsstr,colpesos,numCols,numCols1, "Deudores Comerciales Morosos Graves")

gestionJS = _estiladorVtaTituloD(gestionJ,percColsstr,colpesos,numCols,numCols1, "Deudores Comerciales en Gestion Judicial")


totalS = _estiladorVtaTituloDTotal(total,coldeuda,percCol, "Clientes Deudores")

'''
### APLICO EL FORMATO A LA TABLA
ubicacion = "C:/Informes/InfoDeuda/"
nombreN = "Deudores_Comerciales.png"
nombreM= "Deudores_Morosos.png"
nombreMG = "Deudores_MorososG.png"
nombreGJ = "Deudores_GestionJ.png"
nombreT='Deuda Comercial Ayer.png'

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
        dfi.export(df, ubicacion+nombre,max_rows=-1)
    else:
        dfi.export(df, ubicacion+nombre,max_rows=-1)

#df_to_image(deudaS, ubicacion, nombreN)
#df_to_image(morosoS, ubicacion, nombreM)
#df_to_image(morosoGS, ubicacion, nombreMG)
#df_to_image(gestionJS, ubicacion, nombreGJ)
df_to_image(totalS, ubicacion, nombreT)
'''
#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)

