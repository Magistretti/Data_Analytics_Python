import os
import math
import numpy as np
from DatosLogin import login
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

hoy = datetime.now()
ayer = hoy - timedelta(days=1)
primer_dia_mes = hoy.replace(day=1)

###########################################################################
##############################   Deuda Clientes ###########################
###########################################################################
deuda = pd.read_sql(
    ''' 
   SELECT CAST(vta.NROCLIENTE as INT) AS 'NROCLIENTE',sum(vta.importe) as 'saldo Cuenta',CLI.NOMBRE,vta.Fechasql
     FROM [Rumaos].[dbo].[VTCtaCte] as vta
     INNER JOIN dbo.FacCli as Cli with (NOLOCK)
     ON vta.NROCLIENTE = Cli.NROCLIPRO
     where vta.NROCLIENTE > '100000'
      and Cli.ListaSaldoCC = 1 
      group by vta.NROCLIENTE,CLI.NOMBRE,vta.Fechasql
      order by fechasql asc
    '''
    ,db_conex)
deuda = deuda.convert_dtypes()
deuda = deuda.fillna(0)


# Agrupa por 'Nro de Cliente' y calcula la suma acumulativa para cada cliente
deuda['Saldo a la Fecha'] = deuda.groupby(deuda['NOMBRE'])['saldo Cuenta'].cumsum()
deuda=deuda.reindex(columns=['NOMBRE','NROCLIENTE','Saldo a la Fecha','Fechasql'])

deuda=deuda.groupby(['NOMBRE','NROCLIENTE','Fechasql'])['Saldo a la Fecha'].sum().reset_index()
####################  saldo remitos pendiente de facturacion HASTA EL MES PASADO ####################

deudaPendFactu = pd.read_sql( '''
        select * from
		(select CAST(vta.NROCLIENTE AS INT) AS 'NROCLIENTE',vta.FECHASQL as FECHASQLFAC,rem.FECHASQL AS FECHASQLREM,rem.IMPTOTAL, rem.NROREMITO  
		FROM [Rumaos].[dbo].FacRemGen as rem
		left outer join [VTCtaCte] as vta on
		rem.NROCLIPRO=vta.NROCLIENTE
		and  rem.NROCOMP = vta.NROCOMP
		and rem.PTOVTAFAC=vta.PTOVTA
		AND rem.LETRACOMP = vta.LETRACOMP
		and rem.TIPO = vta.TIPOCOMP

		where vta.NROCLIENTE > '100000'
        and vta.NROCLIENTE != '101206' ) as e
    order by FECHASQLREM asc

    ''',db_conex)

deudaPendFactu = deudaPendFactu.convert_dtypes()

remitosPendientesFac = pd.DataFrame()

i=1

for i in deudaPendFactu.index:
    df = pd.DataFrame()
    if i != 0:
        if deudaPendFactu.loc[i,'FECHASQLREM'] != deudaPendFactu.loc[i-1,'FECHASQLREM']:

            fecha = deudaPendFactu.loc[i,'FECHASQLREM']
            df=deudaPendFactu.loc[deudaPendFactu['FECHASQLREM'] <= fecha ,:]
            df = df.loc[df['FECHASQLFAC'] > fecha ,:]
            df = df.reindex(columns=['NROCLIENTE','IMPTOTAL'])
            df=df.groupby('NROCLIENTE')['IMPTOTAL'].sum().reset_index()
            df = df.reindex(columns=['NROCLIENTE','IMPTOTAL'])
            df['Fechasql'] = fecha

            remitosPendientesFac = pd.concat([remitosPendientesFac, df])



####################  saldo remitos pendiente de facturacion HASTA HOY #############################3

deudaPendFactumHOY = pd.read_sql(''' 
        select CAST([NROCLIPRO] as INT) as 'NROCLIENTE',Fechasql
        ,sum(IMPTOTAL) as 'REMPENDFACTURACIONHOY'  FROM [Rumaos].[dbo].FacRemGen where 
         PTOVTAFAC = 0
        AND NROCOMP = 0
        AND IMPTOTAL > 0
        and NROCLIPRO > '100000'
        group by nroclipro,fechasql
'''
                                 ,db_conex)

deudaPendFactumHOY = deudaPendFactumHOY.convert_dtypes()


#remitosPendientesFac = remitosPendientesFac.drop(0)
#remitosPendientesFac['Fechasql'] = pd.to_datetime(remitosPendientesFac['Fechasql'])

deudaSheet = deuda.merge(remitosPendientesFac,on=['NROCLIENTE','Fechasql'],how='outer')

deudaSheet = deudaSheet.merge(deudaPendFactumHOY,on=['NROCLIENTE','Fechasql'],how='outer')


deudaSheet = deudaSheet.sort_values(['NROCLIENTE','Fechasql'])
deudaSheet2 = deudaSheet
# rellena los valores nulos en la columna de precio con el valor anterior en la misma columna
deudaSheet2['Saldo a la Fecha'] = deudaSheet2['Saldo a la Fecha'].fillna(method='ffill')
deudaSheet2['IMPTOTAL'] = deudaSheet2['IMPTOTAL'].fillna(method='ffill')
deudaSheet2['REMPENDFACTURACIONHOY'] = deudaSheet2['REMPENDFACTURACIONHOY'].fillna(0)


#deudaSheet['NOMBRE']= deudaSheet['NOMBRE'].fillna('')
#deudaSheet= deudaSheet.fillna(0)
deudaSheet2 = deudaSheet2.loc[deudaSheet2['Fechasql']>='2022-01-01']
deudaSheet2
deudaSheet2['NOMBRE']= deudaSheet2['NOMBRE'].fillna('')

from oauth2client.service_account import ServiceAccountCredentials
import gspread

scope= ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
rutaJson='https://api.bluelytics.com.ar/v2/evolution.json'
ruta2="curious-athlete-393417-45edffc04e53.json"

def cargar(df):
    credenciales = ServiceAccountCredentials.from_json_keyfile_name(ruta2, scope)
    cliente = gspread.authorize(credenciales)
    sheet= cliente.open("Deuda").get_worksheet_by_id(0)
    sheet.clear()
    df['Fechasql'] = pd.to_datetime(df['Fechasql'])
    df['Fechasql'] = df['Fechasql'].dt.strftime('%Y-%m-%d')

    #sheet.insert_rows([df.columns.values.tolist()]+ df.values.tolist())

    sheet.append_rows([df.columns.values.tolist()]+ df.values.tolist())
    print('Carga completa')

bandera = True

if bandera == True:
    cargar(deudaSheet2)

