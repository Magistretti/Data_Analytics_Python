import os
import math
import re
import numpy as np
from DatosLogin import login
from Conectores import conectorMSSQL
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
##############################   GNC ######################################
###########################################################################
tarjetas = pd.read_sql(
 ''' 
   
 SELECT
    DISTINCT d.TARJETA,d.FECHASQL
    FROM Rumaos.dbo.Despapro as d left join [GDM_SRV].[dbo].[CLIENTE] AS cli 
	on d.TARJETA = cli.tarjeta
    WHERE 
        d.TARJETA like 'i%'
		and d.FECHASQL >= '2022-02-01'
		and d.FECHASQL < DATEADD(DAY,0,CAST(GETDATE() AS date))
        AND d.VOLUMEN > '0'
        AND d.CODPRODUCTO = 'GNC'
        and d.TARJETA NOT LIKE 'cc%'
        and d.TARJETA NOT LIKE 'ig99999%'
		and cli.Categoria = 6
 ''' 
  ,db_conex)
tarjetas = tarjetas.convert_dtypes()

hoy = datetime.date.today()  # Obtén la fecha de hoy
ayer = hoy - datetime.timedelta(days=1)

fechas_mes_actual = []  # Crea una lista vacía para guardar las fechas
from datetime import datetime, timedelta

# Obtener la fecha actual
fecha_actual = datetime.today()
# Crear una lista con las últimas 30 fechas en formato '%Y-%m-%d'
ultimos_30_dias = [(ayer - timedelta(days=d)).strftime('%Y-%m-%d') for d in reversed(range(30))]
# Imprimir la lista



def clasificador(df):
    dfTOTAL = pd.DataFrame({'Clientes Activos': [5] ,
                            'Clientes Pasivos': [5],
                            'Nuevos Clientes':[1],
                            'Clientes Recaptados':[2],
                   'FECHA':['2021-01-01']})
    for i in range(len(ultimos_30_dias)):
        activos = 0
        pasivos = 0
        nuevoscli = 0
        df1=df
        df1 = df1.loc[df1["FECHASQL"] <= ultimos_30_dias[i],:]
        dfmin= df1.groupby('TARJETA')['FECHASQL'].min().reset_index() ### Agrupo por la primera vez que cargo cada tarjeta
        dfagrupado = df1.groupby('TARJETA')['FECHASQL'].max().reset_index() ### Agrupo por la ultima vez que cargo cada tarjeta
        dfagrupado = dfagrupado.convert_dtypes()
        ### Determino los Activos y Pasivos
        for d in dfagrupado.index:
            from datetime import datetime, timedelta
            fecha_datetime = datetime.strptime(ultimos_30_dias[i], '%Y-%m-%d').date()
            fecha_21_dias_atras = fecha_datetime - timedelta(days=13)
            hace_21_dias=fecha_21_dias_atras.strftime('%Y-%m-%d')
            if dfagrupado.loc[d,'FECHASQL'].strftime('%Y-%m-%d') >= hace_21_dias:
                activos += 1
            else:
                pasivos += 1

        fecha_datetimeayer = datetime.strptime(ultimos_30_dias[i-1], '%Y-%m-%d').date()
        fecha_22_dias_atras = fecha_datetimeayer - timedelta(days=13)
        fecha_22_dias_atras = fecha_22_dias_atras.strftime('%Y-%m-%d')
        df2=df
        df3=df
        df2 = df2.loc[df2["FECHASQL"] <= ultimos_30_dias[i-1],:]
        df3 = df3.loc[df3["FECHASQL"] <= ultimos_30_dias[i],:]
        df2=df2.groupby('TARJETA')['FECHASQL'].max().reset_index()
        df3=df3.groupby('TARJETA')['FECHASQL'].max().reset_index()
        df2 = df2.loc[df2["FECHASQL"] >= fecha_22_dias_atras,:]
        df3 = df3.loc[df3["FECHASQL"] >= hace_21_dias,:]
        df2lista = df2['TARJETA'].tolist()
        df3lista = df3['TARJETA'].tolist()
        recaptadas=0
        for elem in df3lista:
            if elem not in df2lista:
                    recaptadas += 1

        for e in dfmin.index:
            if dfmin.loc[e,'FECHASQL'].strftime('%Y-%m-%d') == ultimos_30_dias[i]:
                nuevoscli += 1

        dfTOTAL.loc[i,'Clientes Activos']= activos-recaptadas-nuevoscli
        dfTOTAL.loc[i,'Clientes Pasivos']= pasivos
        dfTOTAL.loc[i,'Nuevos Clientes']= nuevoscli
        dfTOTAL.loc[i,'Clientes Recaptados']= recaptadas-nuevoscli
        dfTOTAL.loc[i,'Fecha']= ultimos_30_dias[i]

    return dfTOTAL


clientespordia=clasificador(tarjetas)

clientespordia=clientespordia.reindex(columns={'Fecha','Clientes Activos','Clientes Pasivos','Nuevos Clientes','Clientes Recaptados'})
clientespordia=clientespordia.drop(0, axis=0)
ratios=clientespordia

ratios=ratios.reset_index()
ratios['Activos TOTALES']= (ratios['Clientes Activos']+ratios['Clientes Recaptados']+ratios['Nuevos Clientes'])
promActivos=ratios['Activos TOTALES'].mean()
promrecap=ratios['Clientes Recaptados'].mean()
promnuevos=ratios['Nuevos Clientes'].mean()

recaptados= (promrecap/promActivos)*100
nuevos= (promnuevos/promActivos)*100
recaptados="{:.2f}%".format(recaptados)
nuevos="{:.2f}%".format(nuevos)
colors1 = ['green','blue','orange']

clientespordia.set_index("Fecha", inplace=True)
df = clientespordia[['Clientes Pasivos','Nuevos Clientes','Clientes Recaptados']]
df.plot.bar(stacked=True,color=colors1)

plt.text(0.6, 380, f'Nuevos Clientes Prom. Diario: {nuevos} \nRecaptados Prom. Diario: {recaptados}', bbox=dict(facecolor='white', alpha=0.5))
plt.ylim(200, 400)
plt.title('CRM GNC Fletes')


# Defino ubicacion de donde se almacenara la imagen
ubicacion = "C:/Informes/InfoLubri_y_RedMas/"
nombre = "graficoRemis.png"

# Creo una imagen en funcion al dataframe 
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

#df_to_image(clientespordia, ubicacion, nombre)
plt.savefig("C:/Informes/InfoLubri_y_RedMas/graficoFlete.png")


#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)
