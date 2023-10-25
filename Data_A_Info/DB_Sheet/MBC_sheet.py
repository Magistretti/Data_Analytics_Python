import os
import math
import numpy as np
from DatosLogin import login
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

today = datetime.today()  # obtiene la fecha actual
start_of_month = datetime(today.year, today.month, 1)  # obtiene la fecha de inicio del mes actual
start_of_month=start_of_month.strftime('%Y-%m-%d')
today=today.strftime('%Y-%m-%d')

now = datetime.now()  # Obtiene la fecha y hora actual
primer_dia_mes_actual = datetime(now.year, now.month, 1)  # Obtiene el primer día del mes actual
ultimo_dia_mes_anterior = primer_dia_mes_actual - timedelta(days=1)  # Resta un día para obtener el último día del mes anterior
primer_dia_mes_anterior = datetime(ultimo_dia_mes_anterior.year, ultimo_dia_mes_anterior.month, 1)  # Obtiene el primer día del mes anterior
fecha_inicio_mes_anterior = primer_dia_mes_anterior.strftime('%Y-%m-%d')

    
hoy = datetime.now()
ayer = hoy - timedelta(days=1)
primer_dia_mes = hoy.replace(day=1)

fechaInicio='2022-05-01'
fechaInicio = datetime.strptime(fechaInicio, "%Y-%d-%m").date()

#################################

tiempoInicio = pd.to_datetime("today")
#########
# Formating display of dataframes with comma separator for numbers
pd.options.display.float_format = "{:20,.0f}".format

### TRAMOS Y COSTOS 
sheet_id_TyC='1yJlZkGWDcYa5hdlXZxY5_xbi4s3Y0AqoS-L2QgDdFTQ'

sheet_name_DapsaNS= 'CostoDapsaNS'
sheet_name_DapsaNU= 'CostoDapsaNU'
sheet_name_DapsaGO= 'CostoDapsaGO'
sheet_name_DapsaEU= 'CostoDapsaEU'

sheet_name_GNCM3 = 'CostoGNCM3'

sheet_name_CyT_NS= 'ComisionyTramoNS'
sheet_name_CyT_NU= 'ComisionyTramoNU'
sheet_name_CyT_GO='ComisionyTramoGO'
sheet_name_CyT_EU='ComisionyTramoEU'


def get_sheet(sheet_name, sheet_id):
    gsheet_url_costo = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id, sheet_name)
    costo=pd.read_csv(gsheet_url_costo)
    costo= costo.convert_dtypes()
    return costo


######### NAFTA
######### COSTOS DAPSA EU
costoDapsaNS = get_sheet(sheet_name_DapsaNS, sheet_id_TyC)
costoDapsaNS['FECHASQL'] = pd.to_datetime(costoDapsaNS['FECHASQL'], format='%d/%m/%Y')

######### OSTOS DAPSA EU
costoDapsaNU=get_sheet(sheet_name_DapsaNU, sheet_id_TyC)
costoDapsaNU['FECHASQL'] = pd.to_datetime(costoDapsaNU['FECHASQL'], format='%d/%m/%Y')

######### COMISIONES Y TRAMOS NS
comisionNS= get_sheet(sheet_name_CyT_NS, sheet_id_TyC)
comisionNS['Fecha'] = pd.to_datetime(comisionNS['Fecha'], format='%d/%m/%Y')

######### COMISIONES Y TRAMOS EU
comisionNU = get_sheet(sheet_name_CyT_NU, sheet_id_TyC)
comisionNU['Fecha'] = pd.to_datetime(comisionNU['Fecha'], format='%d/%m/%Y')

######### GASOLEOS
###COSTO
costoDapsaGO= get_sheet(sheet_name_DapsaGO, sheet_id_TyC)
costoDapsaGO['FECHASQL'] = pd.to_datetime(costoDapsaGO['FECHASQL'], format='%d/%m/%Y')

###COSTO
costoDapsaEU=get_sheet(sheet_name_DapsaEU, sheet_id_TyC)
costoDapsaEU['FECHASQL'] = pd.to_datetime(costoDapsaEU['FECHASQL'], format='%d/%m/%Y')

###COMISION
comisionGO= get_sheet(sheet_name_CyT_GO, sheet_id_TyC)
comisionGO['Fecha'] = pd.to_datetime(comisionGO['Fecha'], format='%d/%m/%Y')

comisionEU= get_sheet(sheet_name_CyT_EU, sheet_id_TyC)
comisionEU['Fecha'] = pd.to_datetime(comisionEU['Fecha'], format='%d/%m/%Y')

######### LECTURA DE EXCEL DE COSTOS GNC
fechaInicio='2022-01-01'
fechaInicio = datetime.strptime(fechaInicio, "%Y-%m-%d").date()

costoGNC= get_sheet(sheet_name_GNCM3, sheet_id_TyC)
costoGNC['FECHASQL'] = pd.to_datetime(costoGNC['FECHASQL'], format='%d/%m/%Y')
costoGNC= costoGNC.loc[costoGNC["FECHASQL"] >= fechaInicio.strftime('%Y-%m-%d'),:]



costoDapsaGO= costoDapsaGO.loc[(costoDapsaGO["FECHASQL"] <= ayer.strftime('%Y-%m-%d')),:]
costoDapsaEU= costoDapsaEU.loc[(costoDapsaEU["FECHASQL"] <= ayer.strftime('%Y-%m-%d')),:]


## UNCION PARA APLICAR COMISION NS
def aplicar_comision_acumuladaDAPSA(df):
    df=df.sort_values(['UEN','FECHASQL'])
    df = df.merge(costoDapsaNS,on=['FECHASQL','CODPRODUCTO'],how='outer')
    df['Ventas Acumuladas'] = df.groupby(df['FECHASQL'].dt.to_period('M'))['Volumen Total Vendido'].cumsum()

    # convertir la columna 'Precio Cartel' a un tipo de datos numérico
    df['Precio Cartel'] = pd.to_numeric(df['Precio Cartel'], errors='coerce')

    # reemplazar los valores 0 en la columna 'Precio Cartel' con valores nulos (NaN)
    df['Precio Cartel'] = df['Precio Cartel'].replace(0, np.nan)
    # llenar los valores nulos utilizando la función fillna de Pandas con el argumento method='ffill'
    df['Precio Cartel'] = df['Precio Cartel'].fillna(method='ffill')
    df['Precio Cartel'] = df['Precio Cartel'].fillna(method='bfill')   
    
    df['COMISION']= df['Precio Cartel']-df['COSTO NS DAP']
    df['Comision $']= df['COMISION']*df['Volumen Total Vendido']
    df['Ventas Acumuladas $'] = df['Volumen Total Vendido']*df['Precio Cartel']
    df = df.reindex(columns=['UEN',  'CODPRODUCTO','FECHASQL','Ventas Acumuladas $','Ventas Acumuladas', 'Volumen Total Vendido', 'Comision $', 'COMISION', 'Precio Cartel'])
    return df

def aplicar_comision_acumuladaNU(df,estacion):
    df=df.sort_values(['UEN','FECHASQL'])
    comi = comisionNU.loc[(comisionNU["UEN"] == estacion),:]
    comi = comi.reset_index()
    t1=comi.loc[0,'COMISION TRAMO I NU']
    df['Ventas Acumuladas'] = df.groupby(df['FECHASQL'].dt.to_period('M'))['Volumen Total Vendido'].cumsum()
    for i in df.index:
        if df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO I NU']:
            df.loc[i,'Marcador'] = 1
        elif df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO II NU']:
            df.loc[i,'Marcador'] = 2
        elif df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO III NU']:
                df.loc[i,'Marcador'] = 3
        else:
            if df.loc[i,'Ventas Acumuladas'] > comi.loc[0,'TRAMO III NU']:
                df.loc[i,'Marcador'] = 4     
                
    for i in df.index:
        if df.loc[i,'Marcador'] == 1:
            df.loc[i,'COMISION']=comi.loc[0,'COMISION TRAMO I NU']
        elif df.loc[i,'Marcador'] == 2:
            if df.loc[i,'Marcador'] != df.loc[i-1,'Marcador']:
                tsig = (df.loc[i,'Ventas Acumuladas'] - comi.loc[0,'TRAMO I NU'])
                tant = (df.loc[i,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO I NU']*tant)+(comi.loc[0,'COMISION TRAMO II NU']*tsig)
                df.loc[i,'COMISION']= comis/df.loc[i,'Volumen Total Vendido']
            else:
                df.loc[i,'COMISION']=comi.loc[0,'COMISION TRAMO II NU']
        elif df.loc[i,'Marcador'] == 3:
            if df.loc[i,'Marcador'] != df.loc[i-1,'Marcador']:
                tsig = (df.loc[i,'Ventas Acumuladas'] - comi.loc[0,'TRAMO II NU'])
                tant = (df.loc[i,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO II NU']*tant)+(comi.loc[0,'COMISION TRAMO III NU']*tsig)
                df.loc[i,'COMISION']= comis/df.loc[i,'Volumen Total Vendido']
            else:
                df.loc[i,'COMISION']=comi.loc[0,'COMISION TRAMO III NU']
        elif df.loc[i,'Marcador'] == 4:
            if df.loc[i,'Marcador'] != df.loc[i-1,'Marcador']:
                tsig = (df.loc[i,'Ventas Acumuladas'] - comi.loc[0,'TRAMO III NU'])
                tant = (df.loc[i,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO III NU']*tant)+(comi.loc[0,'COMISION TRAMO IV NU']*tsig)
                df.loc[i,'COMISION']= comis/df.loc[i,'Volumen Total Vendido']
            else:
                df.loc[i,'COMISION']=comi.loc[0,'COMISION TRAMO IV NU']
    
    df['Comision $']= (df['COMISION']*df['Total Vendido $'])*1.21/0.97
    df['Comision Yer $'] = (df['Total Vendido YER $']*t1)
    df['Ventas Acumuladas $'] = df['Total Vendido $']
    df['Ventas Acumuladas $ YER'] = df['Total Vendido YER $']
    df = df.reindex(columns=['UEN',  'CODPRODUCTO','FECHASQL','Ventas Acumuladas $','Ventas Acumuladas', 'Volumen Total Vendido', 'Comision $', 'COMISION', 'Precio Cartel','Ventas Acumuladas $ YER','Comision Yer $','Volumen Total Vendido YER','Volumen Total Vendido sin YER'])
    return df

# Definir una función para aplicar la comisión acumulada según el tramo de ventas
def aplicar_comision_acumuladaNS(df,estacion):
    df=df.sort_values(['UEN','FECHASQL'])
    comi = comisionNS.loc[(comisionNS["UEN"] == estacion),:]
    comi = comi.reset_index()
    t1=comi.loc[0,'COMISION TRAMO I NS']

    df['Ventas Acumuladas'] = df.groupby(df['FECHASQL'].dt.to_period('M'))['Volumen Total Vendido'].cumsum()
    for i in df.index:
        if df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO I NS']:
            df.loc[i,'Marcador'] = 1
        elif df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO II NS']:
            df.loc[i,'Marcador'] = 2
        elif df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO III NS']:
                df.loc[i,'Marcador'] = 3
        else:
            if df.loc[i,'Ventas Acumuladas'] > comi.loc[0,'TRAMO III NS']:
                df.loc[i,'Marcador'] = 4          
    
    for i in df.index:
        if df.loc[i,'Marcador'] == 1:
            df.loc[i,'COMISION']=comi.loc[0,'COMISION TRAMO I NS']
        elif df.loc[i,'Marcador'] == 2:
            if df.loc[i,'Marcador'] != df.loc[i-1,'Marcador']:
                tsig = (df.loc[i,'Ventas Acumuladas'] - comi.loc[0,'TRAMO I NS'])
                tant = (df.loc[i,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO I NS']*tant)+(comi.loc[0,'COMISION TRAMO II NS']*tsig)
                df.loc[i,'COMISION']= comis/df.loc[i,'Volumen Total Vendido']
            else:
                df.loc[i,'COMISION']=comi.loc[0,'COMISION TRAMO II NS']
        elif df.loc[i,'Marcador'] == 3:
            if df.loc[i,'Marcador'] != df.loc[i-1,'Marcador']:
                tsig = (df.loc[i,'Ventas Acumuladas'] - comi.loc[0,'TRAMO II NS'])
                tant = (df.loc[i,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO II NS']*tant)+(comi.loc[0,'COMISION TRAMO III NS']*tsig)
                df.loc[i,'COMISION']= comis/df.loc[i,'Volumen Total Vendido']
            else:
                df.loc[i,'COMISION']=comi.loc[0,'COMISION TRAMO III NS']
        elif df.loc[i,'Marcador'] == 4:
            if df.loc[i,'Marcador'] != df.loc[i-1,'Marcador']:
                tsig = (df.loc[i,'Ventas Acumuladas'] - comi.loc[0,'TRAMO III NS'])
                tant = (df.loc[i,'Volumen Total Vendido']-tsig)
                comis=(comi.loc[0,'COMISION TRAMO III NS']*tant)+(comi.loc[0,'COMISION TRAMO IV NS']*tsig)
                df.loc[i,'COMISION']= comis/df.loc[i,'Volumen Total Vendido']
            else:
                df.loc[i,'COMISION']=comi.loc[0,'COMISION TRAMO IV NS']
            
    df['Comision $']= (df['COMISION']*df['Total Vendido $'])*1.21/0.97
    df['Comision Yer $'] = (df['Total Vendido YER $']*t1)
    df['Ventas Acumuladas $'] = df['Total Vendido $']
    df['Ventas Acumuladas $ YER'] = df['Total Vendido YER $']
    df = df.reindex(columns=['UEN',  'CODPRODUCTO','FECHASQL','Ventas Acumuladas $','Ventas Acumuladas', 'Volumen Total Vendido', 'Comision $', 'COMISION', 'Precio Cartel','Ventas Acumuladas $ YER','Comision Yer $',     'Volumen Total Vendido YER','Volumen Total Vendido sin YER'])
    return df

def combustible_nafta():
    
    ################################################
    ############# Volumen diario GASOLEOS YPF 
    ################################################

    df_naftasTotal = pd.read_sql('''
            DECLARE @ayer DATETIME
        SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))
        DECLARE @inicioMesActual DATETIME
        SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

            DECLARE @inicioMesAnterior DATETIME
            SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

            --Divide por la cant de días del mes anterior y multiplica por la cant de días del
            --mes actual
            
            DECLARE @hoy DATETIME
            SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

            --Divide por la cantidad de días cursados del mes actual y multiplica por la cant
            --de días del mes actual
            
        SELECT UEN  
            ,[CODPRODUCTO]
            ,SUM([VTATOTVOL]) AS 'VENTA TOTAL VOLUMEN'
            ,FECHASQL
            ,sum(VTAPRECARTELVOL) AS 'Ventas Efectivo'
            ,MAX(PRECARTEL) as 'Precio Cartel'
            ,sum(VTACTACTEVOL + VTAADELVOL) AS 'Venta Cta Cte'
            ,MAX(PREVTAADEL) AS 'Precio Cta Cte'
            ,sum(VTAPROMOSVOL) AS 'ventas Promos'
        FROM [Rumaos].[dbo].[EmpVenta]
        WHERE FECHASQL >= '2022-01-01'
            AND FECHASQL < @hoy
            AND VTATOTVOL > '0'
            and (CODPRODUCTO = 'NS' OR CODPRODUCTO = 'NU')
            and UEN IN (
                'LAMADRID'
                ,'AZCUENAGA'
                ,'PERDRIEL'
                ,'PERDRIEL2'
                ,'PUENTE OLIVE'
                ,'SAN JOSE'
            )
            group BY FECHASQL,CODPRODUCTO,UEN
            order by CODPRODUCTO,UEN

    ''',db_conex)
    df_naftasTotal = df_naftasTotal.convert_dtypes()

    ############################################
    #####Volumen Diario GASOLEOS YPF PLAYA######
    ############################################

    df_naftasPlaya = pd.read_sql('''
        DECLARE @ayer DATETIME
        SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))
        DECLARE @inicioMesActual DATETIME
        SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

        DECLARE @inicioMesAnterior DATETIME
        SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

        --Divide por la cant de días del mes anterior y multiplica por la cant de días del
        --mes actual
        
        DECLARE @hoy DATETIME
        SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)
        SELECT emp.UEN,emp.FECHASQL,sum(emp.VOLUMEN) as 'Volumen YER',emp.CODPRODUCTO
        FROM [Rumaos].[dbo].[EmpPromo] AS EmP
            INNER JOIN Promocio AS P 
                ON EmP.UEN = P.UEN 
                AND EmP.CODPROMO = P.CODPROMO
        WHERE FECHASQL >= '2022-01-01'
            AND FECHASQL < @hoy
            and emp.UEN IN (
                'LAMADRID'
                ,'AZCUENAGA'
                ,'PERDRIEL'
                ,'PERDRIEL2'
                ,'PUENTE OLIVE'
                ,'SAN JOSE'
            )
            and (emp.CODPRODUCTO = 'NS' OR emp.CODPRODUCTO = 'NU')
            AND  P.[DESCRIPCION] like '%ruta%'
            group by emp.FECHASQL,emp.CODPRODUCTO,emp.UEN
            order by CODPRODUCTO,UEN
    '''      ,db_conex)
    df_naftasPlaya = df_naftasPlaya.convert_dtypes()


    ############################################
    #####Volumen Diario GASOLEOS REDMAS APPYPF######
    ############################################

    df_naftasREDMAS = pd.read_sql('''
        DECLARE @ayer DATETIME
        SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))
        DECLARE @inicioMesActual DATETIME
        SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

        DECLARE @inicioMesAnterior DATETIME
        SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

        --Divide por la cant de días del mes anterior y multiplica por la cant de días del
        --mes actual
        
        DECLARE @hoy DATETIME
        SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)
        SELECT emp.UEN,emp.FECHASQL,sum(emp.VOLUMEN) as 'Volumen REDMAS',emp.CODPRODUCTO,MAX(emp.PRECIO) AS 'Precio Promos'
        FROM [Rumaos].[dbo].[EmpPromo] AS EmP
            INNER JOIN Promocio AS P 
                ON EmP.UEN = P.UEN 
                AND EmP.CODPROMO = P.CODPROMO
        WHERE FECHASQL >= '2022-01-01'
            AND FECHASQL < @hoy
            and emp.UEN IN (
                'LAMADRID'
                ,'AZCUENAGA'
                ,'PERDRIEL'
                ,'PERDRIEL2'
                ,'PUENTE OLIVE'
                ,'SAN JOSE'
            )
            and (emp.CODPRODUCTO = 'NS' OR emp.CODPRODUCTO = 'NU')
            AND  (P.[DESCRIPCION] like '%PROMO%' OR P.DESCRIPCION LIKE '%MERCO%' OR P.DESCRIPCION LIKE '%MAS%')
            group by emp.FECHASQL,emp.CODPRODUCTO,emp.UEN
            order by CODPRODUCTO,UEN
    '''      ,db_conex)
    df_naftasREDMAS = df_naftasREDMAS.convert_dtypes()

    df_naftasTotal=df_naftasTotal.merge(df_naftasREDMAS,on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')
    df_naftasTotal=df_naftasTotal.merge(df_naftasPlaya,on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')
    ########################################################################################################
    df_naftasTotal = df_naftasTotal.fillna(0)
    df_naftasTotal['Total Vendido $'] = ((df_naftasTotal['Ventas Efectivo']*df_naftasTotal['Precio Cartel'])
                                        +(df_naftasTotal['Venta Cta Cte']*df_naftasTotal['Precio Cta Cte'])
                                        +(df_naftasTotal['Volumen REDMAS']*df_naftasTotal['Precio Cartel']))
    
    df_naftasTotal['Volumen Total Vendido']=(df_naftasTotal['Ventas Efectivo']+df_naftasTotal['Venta Cta Cte']
                                            +df_naftasTotal['Volumen REDMAS']+df_naftasTotal['Volumen YER'])
    
    df_naftasTotal['Volumen Total Vendido YER']=df_naftasTotal['Volumen YER']

    
    df_naftasTotal['Volumen Total Vendido sin YER'] = (df_naftasTotal['Ventas Efectivo']+df_naftasTotal['Venta Cta Cte']
                                            +df_naftasTotal['Volumen REDMAS'])

    df_naftasTotal['Total Vendido YER $']=df_naftasTotal['Volumen YER']*df_naftasTotal['Precio Cartel']

    df_naftasTotal['UEN']=df_naftasTotal['UEN'].str.strip()
    df_naftasTotal['CODPRODUCTO']=df_naftasTotal['CODPRODUCTO'].str.strip()

    # convertir la columna 'Precio Cartel' a un tipo de datos numérico
    df_naftasTotal['Precio Cartel'] = pd.to_numeric(df_naftasTotal['Precio Cartel'], errors='coerce')

    # reemplazar los valores 0 en la columna 'Precio Cartel' con valores nulos (NaN)
    df_naftasTotal['Precio Cartel'] = df_naftasTotal['Precio Cartel'].replace(0, np.nan)
    # llenar los valores nulos utilizando la función fillna de Pandas con el argumento method='ffill'
    df_naftasTotal['Precio Cartel'] = df_naftasTotal['Precio Cartel'].fillna(method='ffill')
    df_naftasTotal['Precio Cartel'] = df_naftasTotal['Precio Cartel'].fillna(method='bfill')  


    df_naftasTotal = df_naftasTotal.reindex(columns=['UEN','FECHASQL','CODPRODUCTO','Volumen Total Vendido','Total Vendido $','Total Vendido YER $','Precio Cartel', 'Volumen Total Vendido YER','Volumen Total Vendido sin YER'])
    df_naftasTotal = df_naftasTotal.fillna(0)
    df_naftasTotal= df_naftasTotal.loc[df_naftasTotal["FECHASQL"] <= ayer.strftime('%Y-%m-%d'),:]

    df_naftasTotal=df_naftasTotal.sort_values(['UEN','CODPRODUCTO','FECHASQL'])
    costoNaftasYPF=df_naftasTotal


    ### Aplico la comision a cada estacion para el producto NS
    perdriel1= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'PERDRIEL') & (df_naftasTotal["CODPRODUCTO"] == 'NS'),:]
    perdriel1 = aplicar_comision_acumuladaNS(perdriel1,'PERDRIEL')
    perdriel2= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'PERDRIEL2') & (df_naftasTotal["CODPRODUCTO"] == 'NS'),:]
    perdriel2 = aplicar_comision_acumuladaNS(perdriel2,'PERDRIEL2')
    azcuenaga= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'AZCUENAGA') & (df_naftasTotal["CODPRODUCTO"] == 'NS'),:]
    azcuenaga = aplicar_comision_acumuladaNS(azcuenaga,'AZCUENAGA')
    san_Jose= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'SAN JOSE') & (df_naftasTotal["CODPRODUCTO"] == 'NS'),:]
    san_Jose = aplicar_comision_acumuladaNS(san_Jose,'SAN JOSE')
    puente_Olive= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'PUENTE OLIVE') & (df_naftasTotal["CODPRODUCTO"] == 'NS'),:]
    puente_Olive = aplicar_comision_acumuladaNS(puente_Olive,'PUENTE OLIVE')
    lamadrid= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'LAMADRID') & (df_naftasTotal["CODPRODUCTO"] == 'NS'),:]
    lamadrid = aplicar_comision_acumuladaNS(lamadrid,'LAMADRID')

    ### Creo tabla de YPF NS
    naftaNSYPF= pd.concat([perdriel1, perdriel2, azcuenaga, san_Jose, puente_Olive, lamadrid], ignore_index=True)
    print(naftaNSYPF)
    
    ### Aplico la comision a cada estacion para el producto EU
    perdriel1= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'PERDRIEL') & (df_naftasTotal["CODPRODUCTO"] == 'NU'),:]
    perdriel1 = aplicar_comision_acumuladaNU(perdriel1,'PERDRIEL')
    perdriel2= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'PERDRIEL2') & (df_naftasTotal["CODPRODUCTO"] == 'NU'),:]
    perdriel2 = aplicar_comision_acumuladaNU(perdriel2,'PERDRIEL2')
    azcuenaga= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'AZCUENAGA') & (df_naftasTotal["CODPRODUCTO"] == 'NU'),:]
    azcuenaga = aplicar_comision_acumuladaNU(azcuenaga,'AZCUENAGA')
    san_Jose= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'SAN JOSE') & (df_naftasTotal["CODPRODUCTO"] == 'NU'),:]
    san_Jose = aplicar_comision_acumuladaNU(san_Jose,'SAN JOSE')
    puente_Olive= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'PUENTE OLIVE') & (df_naftasTotal["CODPRODUCTO"] == 'NU'),:]
    puente_Olive = aplicar_comision_acumuladaNU(puente_Olive,'PUENTE OLIVE')
    lamadrid= df_naftasTotal.loc[(df_naftasTotal["UEN"] == 'LAMADRID') & (df_naftasTotal["CODPRODUCTO"] == 'NU'),:]
    lamadrid = aplicar_comision_acumuladaNU(lamadrid,'LAMADRID')


    ### Concateno Tabla De YPF NS con YPF EU
    naftaNUYPF = pd.concat([perdriel1, perdriel2, azcuenaga, san_Jose, puente_Olive, lamadrid], ignore_index=True)
    naftaYPF= pd.concat([naftaNUYPF,naftaNSYPF], ignore_index=True)


    ##################################################
    ####### VENTAS DAPSA #############################
    ##################################################

    # Obtener la fecha actual
    fecha_actual = datetime.now()
    # Crear una nueva fecha con el primer día del mes actual
    primer_dia_mes = datetime(fecha_actual.year, fecha_actual.month, 1)




    ####  Volumen diario GASOLEOS YPF 
    df_naftasdapsa = pd.read_sql(f''' 
            DECLARE @ayer DATETIME
        SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))
        DECLARE @inicioMesActual DATETIME
        SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

            DECLARE @inicioMesAnterior DATETIME
            SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

            --Divide por la cant de días del mes anterior y multiplica por la cant de días del
            --mes actual
            
            DECLARE @hoy DATETIME
            SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

            --Divide por la cantidad de días cursados del mes actual y multiplica por la cant
            --de días del mes actual
            
        SELECT UEN  
            ,[CODPRODUCTO]
            ,SUM([VTATOTVOL]) AS 'VENTA TOTAL VOLUMEN'
            ,FECHASQL
            ,sum(VTAPRECARTELVOL) AS 'Ventas Efectivo'
            ,MAX(PRECARTEL) as 'Precio Cartel'
            ,sum(VTACTACTEVOL + VTAADELVOL) AS 'Venta Cta Cte'
            ,MAX(PREVTAADEL) AS 'Precio Cta Cte'
            ,sum(VTAPROMOSVOL) AS 'ventas Promos'
        FROM [Rumaos].[dbo].[EmpVenta]
        WHERE FECHASQL >= '2022-01-01'
            AND FECHASQL < @hoy
            AND VTATOTVOL > '0'
            and (CODPRODUCTO = 'NS' OR CODPRODUCTO = 'NU')
            and UEN IN (
                'SARMIENTO'
                ,'URQUIZA'
                ,'MITRE'
                ,'MERC GUAYMALLEN'
                ,'ADOLFO CALLE'
                ,'VILLANUEVA'
                ,'MERCADO 2'
                ,'LAS HERAS'
            )
            group BY FECHASQL,CODPRODUCTO,UEN
            order by CODPRODUCTO,UEN

    '''  ,db_conex)
    df_naftasdapsa = df_naftasdapsa.convert_dtypes()
    df_naftasdapsa =df_naftasdapsa.fillna(0)
    ### Descuentos
    df_naftadapsaDesc = pd.read_sql(f'''

                DECLARE @ayer DATETIME
        SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))
        DECLARE @inicioMesActual DATETIME
        SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

        DECLARE @inicioMesAnterior DATETIME
        SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

        --Divide por la cant de días del mes anterior y multiplica por la cant de días del
        --mes actual
        
        DECLARE @hoy DATETIME
        SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 1, CURRENT_TIMESTAMP), 0)
        SELECT emp.UEN,EmP.FECHASQL
            ,EmP.[CODPRODUCTO]
            ,SUM(-EmP.[VOLUMEN]) as 'Descuentos'
        FROM [Rumaos].[dbo].[EmpPromo] AS EmP
            INNER JOIN Promocio AS P 
                ON EmP.UEN = P.UEN 
                AND EmP.CODPROMO = P.CODPROMO
        WHERE FECHASQL >= '2022-01-01'
            AND FECHASQL < @hoy
            and emp.UEN IN (
                'SARMIENTO'
                ,'URQUIZA'
                ,'MITRE'
                ,'MERC GUAYMALLEN'
                ,'ADOLFO CALLE'
                ,'VILLANUEVA'
                ,'MERCADO 2'
                ,'LAS HERAS'
            )
            and (emp.CODPRODUCTO = 'NS' OR emp.CODPRODUCTO = 'NU')
            AND (EmP.[CODPROMO] = '30'
                OR P.[DESCRIPCION] like '%PRUEBA%')
            GROUP BY emp.UEN,EMP.FECHASQL,EMP.CODPRODUCTO

    '''      ,db_conex)
    df_naftadapsaDesc = df_naftadapsaDesc.convert_dtypes()
    ### CONCATENO TABLA DE VOLUMEN TOTAL VENDIDO CON LA TABLA DE LOS DESCUENTOS
    df_naftasdapsa = df_naftasdapsa.merge(df_naftadapsaDesc, on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')
    df_naftasdapsa =df_naftasdapsa.fillna(0)

        

    df_naftasdapsa['Total Vendido $'] = ((df_naftasdapsa['Ventas Efectivo']*df_naftasdapsa['Precio Cartel'])
                                        +(df_naftasdapsa['Venta Cta Cte']*df_naftasdapsa['Precio Cta Cte'])
                                        +((df_naftasdapsa['ventas Promos']+df_naftasdapsa['Descuentos'])*df_naftasdapsa['Precio Cartel']))

    df_naftasdapsa['Volumen Total Vendido']=(df_naftasdapsa['Ventas Efectivo']+df_naftasdapsa['Venta Cta Cte']
                                            +(df_naftasdapsa['ventas Promos']+df_naftasdapsa['Descuentos']))

    df_naftasdapsa['UEN']=df_naftasdapsa['UEN'].str.strip()
    df_naftasdapsa['CODPRODUCTO']=df_naftasdapsa['CODPRODUCTO'].str.strip()


    # convertir la columna 'Precio Cartel' a un tipo de datos numérico
    df_naftasdapsa['Precio Cartel'] = pd.to_numeric(df_naftasdapsa['Precio Cartel'], errors='coerce')

    # reemplazar los valores 0 en la columna 'Precio Cartel' con valores nulos (NaN)
    df_naftasdapsa['Precio Cartel'] = df_naftasdapsa['Precio Cartel'].replace(0, np.nan)
    # llenar los valores nulos utilizando la función fillna de Pandas con el argumento method='ffill'
    df_naftasdapsa['Precio Cartel'] = df_naftasdapsa['Precio Cartel'].fillna(method='ffill')
    df_naftasdapsa['Precio Cartel'] = df_naftasdapsa['Precio Cartel'].fillna(method='bfill')

    df_naftasdapsa = df_naftasdapsa.reindex(columns=['UEN','CODPRODUCTO' ,'FECHASQL','Volumen Total Vendido','Total Vendido $','Total Vendido YER $','Precio Cartel'])
    df_naftasdapsa = df_naftasdapsa.fillna(0)
    df_naftasdapsa= df_naftasdapsa.loc[df_naftasdapsa["FECHASQL"] <= ayer.strftime('%Y-%m-%d'),:]

    df_naftasdapsa=df_naftasdapsa.sort_values(['UEN','CODPRODUCTO','FECHASQL'])
    
    '''
    costoDapsaNS= costoDapsaNS.loc[(costoDapsaNS["FECHASQL"] <= ayer.strftime('%Y-%m-%d')),:]
    costoDapsaNU= costoDapsaNU.loc[(costoDapsaNU["FECHASQL"] <= ayer.strftime('%Y-%m-%d')),:]
    '''


    ### Aplico la comision a cada estacion para el producto NS
    las_heras= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'LAS HERAS') & (df_naftasdapsa["CODPRODUCTO"] == 'NS'),:]
    las_heras = aplicar_comision_acumuladaDAPSA(las_heras)
    mercado1= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'MERC GUAYMALLEN') & (df_naftasdapsa["CODPRODUCTO"] == 'NS'),:]
    mercado1 = aplicar_comision_acumuladaDAPSA(mercado1)
    mercado2= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'MERCADO 2') & (df_naftasdapsa["CODPRODUCTO"] == 'NS'),:]
    mercado2 = aplicar_comision_acumuladaDAPSA(mercado2)
    sarmiento= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'SARMIENTO') & (df_naftasdapsa["CODPRODUCTO"] == 'NS'),:]
    sarmiento = aplicar_comision_acumuladaDAPSA(sarmiento)
    villa_nueva= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'VILLANUEVA') & (df_naftasdapsa["CODPRODUCTO"] == 'NS'),:]
    villa_nueva = aplicar_comision_acumuladaDAPSA(villa_nueva)
    adolfo_Calle= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'ADOLFO CALLE') & (df_naftasdapsa["CODPRODUCTO"] == 'NS'),:]
    adolfo_Calle = aplicar_comision_acumuladaDAPSA(adolfo_Calle)
    mitre= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'MITRE') & (df_naftasdapsa["CODPRODUCTO"] == 'NS'),:]
    mitre = aplicar_comision_acumuladaDAPSA(mitre)
    urquiza= df_naftasdapsa.loc[(df_naftasdapsa["UEN"] == 'URQUIZA') & (df_naftasdapsa["CODPRODUCTO"] == 'NS'),:]
    urquiza = aplicar_comision_acumuladaDAPSA(urquiza)

    ### Creo tabla de YPF NS
    mbcNSDAPSA= pd.concat([las_heras, mercado1, mercado2, sarmiento, villa_nueva, adolfo_Calle, mitre, urquiza], ignore_index=True)

    '''
    mbcNSDAPSA['Comision $']= mbcNSDAPSA['Comision $'].fillna(0)
    mbcNSDAPSA['COMISION']= mbcNSDAPSA['COMISION'].fillna(0)
    '''

    df_nafta = pd.concat([naftaYPF,mbcNSDAPSA],ignore_index=True)
    df_nafta = df_nafta.dropna(subset=['UEN'])
    
    return df_nafta

def combustible_gasoleos():
        
    ################################################
    ############# Volumen diario GASOLEOS YPF 
    ################################################

    df_gasoleosTotal = pd.read_sql('''
            DECLARE @ayer DATETIME
        SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))
        DECLARE @inicioMesActual DATETIME
        SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

            DECLARE @inicioMesAnterior DATETIME
            SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

            --Divide por la cant de días del mes anterior y multiplica por la cant de días del
            --mes actual
            
            DECLARE @hoy DATETIME
            SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

            --Divide por la cantidad de días cursados del mes actual y multiplica por la cant
            --de días del mes actual
            
        SELECT UEN  
            ,[CODPRODUCTO]
            ,SUM([VTATOTVOL]) AS 'VENTA TOTAL VOLUMEN'
            ,FECHASQL
            ,sum(VTAPRECARTELVOL) AS 'Ventas Efectivo'
            ,MAX(PRECARTEL) as 'Precio Cartel'
            ,sum(VTACTACTEVOL + VTAADELVOL) AS 'Venta Cta Cte'
            ,MAX(PREVTAADEL) AS 'Precio Cta Cte'
            ,sum(VTAPROMOSVOL) AS 'ventas Promos'
        FROM [Rumaos].[dbo].[EmpVenta]
        WHERE FECHASQL >= '2022-01-01'
            AND FECHASQL < @hoy
            AND VTATOTVOL > '0'
            and (CODPRODUCTO = 'GO' OR CODPRODUCTO = 'EU')
            and UEN IN (
                'LAMADRID'
                ,'AZCUENAGA'
                ,'PERDRIEL'
                ,'PERDRIEL2'
                ,'PUENTE OLIVE'
                ,'SAN JOSE'
            )
            group BY FECHASQL,CODPRODUCTO,UEN
            order by CODPRODUCTO,UEN

    '''      ,db_conex)
    df_gasoleosTotal = df_gasoleosTotal.convert_dtypes()

    ############################################
    #####Volumen Diario GASOLEOS YPF PLAYA######
    ############################################

    df_gasoleosPlaya = pd.read_sql('''
        DECLARE @ayer DATETIME
        SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))
        DECLARE @inicioMesActual DATETIME
        SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

        DECLARE @inicioMesAnterior DATETIME
        SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

        --Divide por la cant de días del mes anterior y multiplica por la cant de días del
        --mes actual
        
        DECLARE @hoy DATETIME
        SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)
        SELECT emp.UEN,emp.FECHASQL,sum(emp.VOLUMEN) as 'Volumen YER',emp.CODPRODUCTO
        FROM [Rumaos].[dbo].[EmpPromo] AS EmP
            INNER JOIN Promocio AS P 
                ON EmP.UEN = P.UEN 
                AND EmP.CODPROMO = P.CODPROMO
        WHERE FECHASQL >= '2022-01-01'
            AND FECHASQL < @hoy
            and emp.UEN IN (
                'LAMADRID'
                ,'AZCUENAGA'
                ,'PERDRIEL'
                ,'PERDRIEL2'
                ,'PUENTE OLIVE'
                ,'SAN JOSE'
            )
            and (emp.CODPRODUCTO = 'GO' OR emp.CODPRODUCTO = 'EU')
            AND  P.[DESCRIPCION] like '%ruta%'
            group by emp.FECHASQL,emp.CODPRODUCTO,emp.UEN
            order by CODPRODUCTO,UEN
    '''      ,db_conex)
    df_gasoleosPlaya = df_gasoleosPlaya.convert_dtypes()


    ############################################
    #####Volumen Diario GASOLEOS REDMAS APPYPF######
    ############################################

    df_gasoleosREDMAS = pd.read_sql('''
        DECLARE @ayer DATETIME
        SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))
        DECLARE @inicioMesActual DATETIME
        SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

        DECLARE @inicioMesAnterior DATETIME
        SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

        --Divide por la cant de días del mes anterior y multiplica por la cant de días del
        --mes actual
        
        DECLARE @hoy DATETIME
        SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)
        SELECT emp.UEN,emp.FECHASQL,sum(emp.VOLUMEN) as 'Volumen REDMAS',emp.CODPRODUCTO,MAX(emp.PRECIO) AS 'Precio Promos'
        FROM [Rumaos].[dbo].[EmpPromo] AS EmP
            INNER JOIN Promocio AS P 
                ON EmP.UEN = P.UEN 
                AND EmP.CODPROMO = P.CODPROMO
        WHERE FECHASQL >= '2022-01-01'
            AND FECHASQL < @hoy
            and emp.UEN IN (
                'LAMADRID'
                ,'AZCUENAGA'
                ,'PERDRIEL'
                ,'PERDRIEL2'
                ,'PUENTE OLIVE'
                ,'SAN JOSE'
            )
            and (emp.CODPRODUCTO = 'GO' OR emp.CODPRODUCTO = 'EU')
            AND  (P.[DESCRIPCION] like '%PROMO%' OR P.DESCRIPCION LIKE '%MERCO%' OR P.DESCRIPCION LIKE '%MAS%')
            group by emp.FECHASQL,emp.CODPRODUCTO,emp.UEN
            order by CODPRODUCTO,UEN
    '''      ,db_conex)
    df_gasoleosREDMAS = df_gasoleosREDMAS.convert_dtypes()



    ################################################
    ############# Volumen diario GASOLEOS YPF 
    ################################################

    df_gasoleosTotal = pd.read_sql('''
            DECLARE @ayer DATETIME
        SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))
        DECLARE @inicioMesActual DATETIME
        SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

            DECLARE @inicioMesAnterior DATETIME
            SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

            --Divide por la cant de días del mes anterior y multiplica por la cant de días del
            --mes actual
            
            DECLARE @hoy DATETIME
            SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

            --Divide por la cantidad de días cursados del mes actual y multiplica por la cant
            --de días del mes actual
            
        SELECT UEN  
            ,[CODPRODUCTO]
            ,SUM([VTATOTVOL]) AS 'VENTA TOTAL VOLUMEN'
            ,FECHASQL
            ,sum(VTAPRECARTELVOL) AS 'Ventas Efectivo'
            ,MAX(PRECARTEL) as 'Precio Cartel'
            ,sum(VTACTACTEVOL + VTAADELVOL) AS 'Venta Cta Cte'
            ,MAX(PREVTAADEL) AS 'Precio Cta Cte'
            ,sum(VTAPROMOSVOL) AS 'ventas Promos'
        FROM [Rumaos].[dbo].[EmpVenta]
        WHERE FECHASQL >= '2022-01-01'
            AND FECHASQL < @hoy
            AND VTATOTVOL > '0'
            and (CODPRODUCTO = 'GO' OR CODPRODUCTO = 'EU')
            and UEN IN (
                'LAMADRID'
                ,'AZCUENAGA'
                ,'PERDRIEL'
                ,'PERDRIEL2'
                ,'PUENTE OLIVE'
                ,'SAN JOSE'
            )
            group BY FECHASQL,CODPRODUCTO,UEN
            order by CODPRODUCTO,UEN

    '''      ,db_conex)
    df_gasoleosTotal = df_gasoleosTotal.convert_dtypes()

    ############################################
    #####Volumen Diario GASOLEOS YPF PLAYA######
    ############################################

    df_gasoleosPlaya = pd.read_sql('''
        DECLARE @ayer DATETIME
        SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))
        DECLARE @inicioMesActual DATETIME
        SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

        DECLARE @inicioMesAnterior DATETIME
        SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

        --Divide por la cant de días del mes anterior y multiplica por la cant de días del
        --mes actual
        
        DECLARE @hoy DATETIME
        SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)
        SELECT emp.UEN,emp.FECHASQL,sum(emp.VOLUMEN) as 'Volumen YER',emp.CODPRODUCTO
        FROM [Rumaos].[dbo].[EmpPromo] AS EmP
            INNER JOIN Promocio AS P 
                ON EmP.UEN = P.UEN 
                AND EmP.CODPROMO = P.CODPROMO
        WHERE FECHASQL >= '2022-01-01'
            AND FECHASQL < @hoy
            and emp.UEN IN (
                'LAMADRID'
                ,'AZCUENAGA'
                ,'PERDRIEL'
                ,'PERDRIEL2'
                ,'PUENTE OLIVE'
                ,'SAN JOSE'
            )
            and (emp.CODPRODUCTO = 'GO' OR emp.CODPRODUCTO = 'EU')
            AND  P.[DESCRIPCION] like '%ruta%'
            group by emp.FECHASQL,emp.CODPRODUCTO,emp.UEN
            order by CODPRODUCTO,UEN
    '''      ,db_conex)
    df_gasoleosPlaya = df_gasoleosPlaya.convert_dtypes()


    ############################################
    #####Volumen Diario GASOLEOS REDMAS APPYPF######
    ############################################

    df_gasoleosREDMAS = pd.read_sql('''
        DECLARE @ayer DATETIME
        SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))
        DECLARE @inicioMesActual DATETIME
        SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

        DECLARE @inicioMesAnterior DATETIME
        SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

        --Divide por la cant de días del mes anterior y multiplica por la cant de días del
        --mes actual
        
        DECLARE @hoy DATETIME
        SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)
        SELECT emp.UEN,emp.FECHASQL,sum(emp.VOLUMEN) as 'Volumen REDMAS',emp.CODPRODUCTO,MAX(emp.PRECIO) AS 'Precio Promos'
        FROM [Rumaos].[dbo].[EmpPromo] AS EmP
            INNER JOIN Promocio AS P 
                ON EmP.UEN = P.UEN 
                AND EmP.CODPROMO = P.CODPROMO
        WHERE FECHASQL >= '2022-01-01'
            AND FECHASQL < @hoy
            and emp.UEN IN (
                'LAMADRID'
                ,'AZCUENAGA'
                ,'PERDRIEL'
                ,'PERDRIEL2'
                ,'PUENTE OLIVE'
                ,'SAN JOSE'
            )
            and (emp.CODPRODUCTO = 'GO' OR emp.CODPRODUCTO = 'EU')
            AND  (P.[DESCRIPCION] like '%PROMO%' OR P.DESCRIPCION LIKE '%MERCO%' OR P.DESCRIPCION LIKE '%MAS%')
            group by emp.FECHASQL,emp.CODPRODUCTO,emp.UEN
            order by CODPRODUCTO,UEN
    '''      ,db_conex)
    df_gasoleosREDMAS = df_gasoleosREDMAS.convert_dtypes()


    df_gasoleosTotal=df_gasoleosTotal.merge(df_gasoleosREDMAS,on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')
    df_gasoleosTotal=df_gasoleosTotal.merge(df_gasoleosPlaya,on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')
    df_gasoleosTotal = df_gasoleosTotal.fillna(0)
    df_gasoleosTotal['Total Vendido $'] = ((df_gasoleosTotal['Ventas Efectivo']*df_gasoleosTotal['Precio Cartel'])
                                        +(df_gasoleosTotal['Venta Cta Cte']*df_gasoleosTotal['Precio Cta Cte'])
                                        +(df_gasoleosTotal['Volumen REDMAS']*df_gasoleosTotal['Precio Cartel']))

    df_gasoleosTotal['Volumen Total Vendido']=(df_gasoleosTotal['Ventas Efectivo']+df_gasoleosTotal['Venta Cta Cte']
                                            +df_gasoleosTotal['Volumen REDMAS']+df_gasoleosTotal['Volumen YER'])
    
    df_gasoleosTotal['Volumen Total Vendido YER']=(df_gasoleosTotal['Volumen YER'])
    
    df_gasoleosTotal['Volumen Total Vendido sin YER']=(df_gasoleosTotal['Ventas Efectivo']+df_gasoleosTotal['Venta Cta Cte']
                                            +df_gasoleosTotal['Volumen REDMAS'])
    

    df_gasoleosTotal['Total Vendido YER $']=df_gasoleosTotal['Volumen YER']*df_gasoleosTotal['Precio Cartel']

    df_gasoleosTotal['UEN']= df_gasoleosTotal['UEN'].str.strip()
    df_gasoleosTotal['CODPRODUCTO']= df_gasoleosTotal['CODPRODUCTO'].str.strip()

    # reemplazar los valores 0 en la columna 'Precio Cartel' con valores nulos (NaN)
    df_gasoleosTotal['Precio Cartel'] = df_gasoleosTotal['Precio Cartel'].replace(0, np.nan)
    # llenar los valores nulos utilizando la función fillna de Pandas con el argumento method='ffill'
    df_gasoleosTotal['Precio Cartel'] = df_gasoleosTotal['Precio Cartel'].fillna(method='ffill')
    df_gasoleosTotal['Precio Cartel'] = df_gasoleosTotal['Precio Cartel'].fillna(method='bfill')

    df_gasoleosTotal = df_gasoleosTotal.reindex(columns=['UEN','FECHASQL','CODPRODUCTO','Volumen Total Vendido','Total Vendido $','Total Vendido YER $','Precio Cartel', 'Volumen Total Vendido YER','Volumen Total Vendido sin YER'])
    df_gasoleosTotal = df_gasoleosTotal.fillna(0)
    df_gasoleosTotal= df_gasoleosTotal.loc[df_gasoleosTotal["FECHASQL"] <= ayer.strftime('%Y-%m-%d'),:]

    df_gasoleosTotal=df_gasoleosTotal.sort_values(['UEN','CODPRODUCTO','FECHASQL'])
    costogasoleosYPF=df_gasoleosTotal

    # Definir una función para aplicar la comisión acumulada según el tramo de ventas
    # Definir una función para aplicar la comisión acumulada según el tramo de ventas
    def aplicar_comision_acumulada(df,estacion):
        df=df.sort_values(['UEN','FECHASQL'])
        comi = comisionGO.loc[(comisionGO["UEN"] == estacion),:]
        comi = comi.reset_index()
        df['Ventas Acumuladas'] = df.groupby(df['FECHASQL'].dt.to_period('M'))['Volumen Total Vendido'].cumsum()    
        for i in df.index:
            if df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO I GO']:
                df.loc[i,'Marcador'] = 1
            elif df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO II GO']:                
                df.loc[i,'Marcador'] = 2
            elif df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO III GO']:
                df.loc[i,'Marcador'] = 3
            else:
                df.loc[i,'Marcador'] = 4
                
    
        for r in df.index:
            if df.loc[r,'Marcador'] == 1:
                df.loc[r,'COMISION']=comi.loc[0,'COMISION TRAMO I GO']
            elif df.loc[r,'Marcador'] == 2:
                if df.loc[r,'Marcador'] != df.loc[r-1,'Marcador']:
                    tsig = (df.loc[r,'Ventas Acumuladas'] - comi.loc[0,'TRAMO I GO'])
                    tant = (df.loc[r,'Volumen Total Vendido']-tsig)
                    comis=(comi.loc[0,'COMISION TRAMO I GO']*tant)+(comi.loc[0,'COMISION TRAMO II GO']*tsig)
                    df.loc[r,'COMISION']= comis/df.loc[r,'Volumen Total Vendido']
                else:
                    df.loc[r,'COMISION']=comi.loc[0,'COMISION TRAMO II GO']
            elif df.loc[r,'Marcador'] == 3:
                if df.loc[r,'Marcador'] != df.loc[r-1,'Marcador']:
                    tsig = (df.loc[r,'Ventas Acumuladas'] - comi.loc[0,'TRAMO II GO'])
                    tant = (df.loc[r,'Volumen Total Vendido']-tsig)
                    comis=(comi.loc[0,'COMISION TRAMO II GO']*tant)+(comi.loc[0,'COMISION TRAMO III GO']*tsig)
                    df.loc[r,'COMISION']= comis/df.loc[r,'Volumen Total Vendido']
                else:
                    df.loc[r,'COMISION']=comi.loc[0,'COMISION TRAMO III GO']
            elif df.loc[r,'Marcador'] == 3:
                if df.loc[r,'Marcador'] != df.loc[r-1,'Marcador']:
                    tsig = (df.loc[r,'Ventas Acumuladas'] - comi.loc[0,'TRAMO III GO'])
                    tant = (df.loc[r,'Volumen Total Vendido']-tsig)
                    comis=(comi.loc[0,'COMISION TRAMO III GO']*tant)+(comi.loc[0,'COMISION TRAMO IV GO']*tsig)
                    df.loc[r,'COMISION']= comis/df.loc[r,'Volumen Total Vendido']
                else:
                    df.loc[r,'COMISION']=comi.loc[0,'COMISION TRAMO IV GO']
                
        df['Comision $']= (df['COMISION']*df['Total Vendido $'])*1.21/0.97
        df['Comision Yer $'] = (df['Total Vendido YER $']*comi.loc[0,'COMISION TRAMO I GO'])
        df['Ventas Acumuladas $'] = df['Total Vendido $']
        df['Ventas Acumuladas $ YER'] = df['Total Vendido YER $']
        df = df.reindex(columns=['UEN',  'CODPRODUCTO','FECHASQL','Ventas Acumuladas $','Ventas Acumuladas', 'Volumen Total Vendido', 'Comision $', 'COMISION', 'Precio Cartel','Ventas Acumuladas $ YER','Comision Yer $', 'Volumen Total Vendido YER','Volumen Total Vendido sin YER'])
    
        return df


    ### Aplico la comision a cada estacion para el producto GO
    perdriel1= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'PERDRIEL') & (df_gasoleosTotal["CODPRODUCTO"] == 'GO'),:]
    perdriel1 = aplicar_comision_acumulada(perdriel1,'PERDRIEL')
    perdriel2= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'PERDRIEL2') & (df_gasoleosTotal["CODPRODUCTO"] == 'GO'),:]
    perdriel2 = aplicar_comision_acumulada(perdriel2,'PERDRIEL2')
    azcuenaga= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'AZCUENAGA') & (df_gasoleosTotal["CODPRODUCTO"] == 'GO'),:]
    azcuenaga = aplicar_comision_acumulada(azcuenaga,'AZCUENAGA')
    san_Jose= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'SAN JOSE') & (df_gasoleosTotal["CODPRODUCTO"] == 'GO'),:]
    san_Jose = aplicar_comision_acumulada(san_Jose,'SAN JOSE')
    puente_Olive= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'PUENTE OLIVE') & (df_gasoleosTotal["CODPRODUCTO"] == 'GO'),:]
    puente_Olive = aplicar_comision_acumulada(puente_Olive,'PUENTE OLIVE')
    lamadrid= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'LAMADRID') & (df_gasoleosTotal["CODPRODUCTO"] == 'GO'),:]
    lamadrid = aplicar_comision_acumulada(lamadrid,'LAMADRID')

    ### Creo tabla de YPF GO

    ypfGO=pd.concat([perdriel1, perdriel2, azcuenaga, san_Jose, puente_Olive, lamadrid])

    def aplicar_comision_acumuladaEU(df,estacion):
        df=df.sort_values(['UEN','FECHASQL'])
        comi = comisionEU.loc[(comisionEU["UEN"] == estacion),:]
        comi = comi.reset_index()
        df['Ventas Acumuladas'] = df.groupby(df['FECHASQL'].dt.to_period('M'))['Volumen Total Vendido'].cumsum()    
        for i in df.index:
            if df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO I EU']:
                df.loc[i,'Marcador'] = 1
            else:
                if df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO II EU']:
                    df.loc[i,'Marcador'] = 2
                elif df.loc[i,'Ventas Acumuladas'] <= comi.loc[0,'TRAMO III EU']:
                    df.loc[i,'Marcador'] = 3
                else:
                    df.loc[i,'Marcador'] = 4

        for r in df.index:
            if df.loc[r,'Marcador'] == 1:
                df.loc[r,'COMISION']=comi.loc[0,'COMISION TRAMO I EU']
            elif df.loc[r,'Marcador'] == 2:
                if df.loc[r,'Marcador'] != df.loc[r-1,'Marcador']:
                    tsig = (df.loc[r,'Ventas Acumuladas'] - comi.loc[0,'TRAMO I EU'])
                    tant = (df.loc[r,'Volumen Total Vendido']-tsig)
                    comis=(comi.loc[0,'COMISION TRAMO I EU']*tant)+(comi.loc[0,'COMISION TRAMO II EU']*tsig)
                    df.loc[r,'COMISION']= comis/df.loc[r,'Volumen Total Vendido']
                else:
                    df.loc[r,'COMISION']=comi.loc[0,'COMISION TRAMO II EU']
            elif df.loc[r,'Marcador'] == 3:
                if df.loc[r,'Marcador'] != df.loc[r-1,'Marcador']:
                    tsig = (df.loc[r,'Ventas Acumuladas'] - comi.loc[0,'TRAMO II EU'])
                    tant = (df.loc[r,'Volumen Total Vendido']-tsig)
                    comis=(comi.loc[0,'COMISION TRAMO II EU']*tant)+(comi.loc[0,'COMISION TRAMO III EU']*tsig)
                    df.loc[r,'COMISION']= comis/df.loc[r,'Volumen Total Vendido']
                else:
                    df.loc[r,'COMISION']=comi.loc[0,'COMISION TRAMO III EU']
            elif df.loc[r,'Marcador'] == 4:
                if df.loc[r,'Marcador'] != df.loc[r-1,'Marcador']:
                    tsig = (df.loc[r,'Ventas Acumuladas'] - comi.loc[0,'TRAMO III EU'])
                    tant = (df.loc[r,'Volumen Total Vendido']-tsig)
                    comis=(comi.loc[0,'COMISION TRAMO III EU']*tant)+(comi.loc[0,'COMISION TRAMO IV EU']*tsig)
                    df.loc[r,'COMISION']= comis/df.loc[r,'Volumen Total Vendido']
                else:
                    df.loc[r,'COMISION']=comi.loc[0,'COMISION TRAMO IV EU']
                    
                    
        df['Comision $']= (df['COMISION']*df['Total Vendido $'])*1.21/0.97
        df['Comision Yer $'] = (df['Total Vendido YER $']*comi.loc[0,'COMISION TRAMO I EU'])
        df['Ventas Acumuladas $'] = df['Total Vendido $']
        df['Ventas Acumuladas $ YER'] = df['Total Vendido YER $']
        df = df.reindex(columns=['UEN',  'CODPRODUCTO','FECHASQL','Ventas Acumuladas $','Ventas Acumuladas', 'Volumen Total Vendido', 'Comision $', 'COMISION', 'Precio Cartel','Ventas Acumuladas $ YER','Comision Yer $',     'Volumen Total Vendido YER','Volumen Total Vendido sin YER'])
    
        return df



    ### Aplico la comision a cada estacion para el producto EU
    perdriel1= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'PERDRIEL') & (df_gasoleosTotal["CODPRODUCTO"] == 'EU'),:]
    perdriel1 = aplicar_comision_acumuladaEU(perdriel1,'PERDRIEL')
    perdriel2= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'PERDRIEL2') & (df_gasoleosTotal["CODPRODUCTO"] == 'EU'),:]
    perdriel2 = aplicar_comision_acumuladaEU(perdriel2,'PERDRIEL2')
    azcuenaga= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'AZCUENAGA') & (df_gasoleosTotal["CODPRODUCTO"] == 'EU'),:]
    azcuenaga = aplicar_comision_acumuladaEU(azcuenaga,'AZCUENAGA')
    san_Jose= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'SAN JOSE') & (df_gasoleosTotal["CODPRODUCTO"] == 'EU'),:]
    san_Jose = aplicar_comision_acumuladaEU(san_Jose,'SAN JOSE')
    puente_Olive= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'PUENTE OLIVE') & (df_gasoleosTotal["CODPRODUCTO"] == 'EU'),:]
    puente_Olive = aplicar_comision_acumuladaEU(puente_Olive,'PUENTE OLIVE')
    lamadrid= df_gasoleosTotal.loc[(df_gasoleosTotal["UEN"] == 'LAMADRID') & (df_gasoleosTotal["CODPRODUCTO"] == 'EU'),:]
    lamadrid = aplicar_comision_acumuladaEU(lamadrid,'LAMADRID')

    ### Creo tabla de YPF EU
    ypfEU= pd.concat([perdriel1, perdriel2, azcuenaga, san_Jose, puente_Olive, lamadrid], ignore_index=True)

    ### Concateno Tabla De YPF GO con YPF EU
    mbcYPF=pd.concat([ypfGO, ypfEU], ignore_index=True)


    ##################################################
    ####### VENTAS DAPSA #############################
    ##################################################

    # Obtener la fecha actual
    fecha_actual = datetime.now()
    # Crear una EUeva fecha con el primer día del mes actual
    primer_dia_mes = datetime(fecha_actual.year, fecha_actual.month, 1)

    ####  Volumen diario GASOLEOS YPF 
    df_gasoleosdapsa = pd.read_sql(''' 
            DECLARE @ayer DATETIME
        SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))
        DECLARE @inicioMesActual DATETIME
        SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

            DECLARE @inicioMesAnterior DATETIME
            SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

            --Divide por la cant de días del mes anterior y multiplica por la cant de días del
            --mes actual
            
            DECLARE @hoy DATETIME
            SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

            --Divide por la cantidad de días cursados del mes actual y multiplica por la cant
            --de días del mes actual
            
        SELECT UEN  
            ,[CODPRODUCTO]
            ,SUM([VTATOTVOL]) AS 'VENTA TOTAL VOLUMEN'
            ,FECHASQL
            ,sum(VTAPRECARTELVOL) AS 'Ventas Efectivo'
            ,MAX(PRECARTEL) as 'Precio Cartel'
            ,sum(VTACTACTEVOL + VTAADELVOL) AS 'Venta Cta Cte'
            ,MAX(PREVTAADEL) AS 'Precio Cta Cte'
            ,sum(VTAPROMOSVOL) AS 'ventas Promos'
        FROM [Rumaos].[dbo].[EmpVenta]
        WHERE FECHASQL >= '2022-01-01'
            AND FECHASQL < @hoy
            AND VTATOTVOL > '0'
            and (CODPRODUCTO = 'GO' OR CODPRODUCTO = 'EU')
            and UEN IN (
                'SARMIENTO'
                ,'URQUIZA'
                ,'MITRE'
                ,'MERC GUAYMALLEN'
                ,'ADOLFO CALLE'
                ,'VILLANUEVA'
                ,'MERCADO 2'
                ,'LAS HERAS'
            )
            group BY FECHASQL,CODPRODUCTO,UEN
            order by CODPRODUCTO,UEN

    '''  ,db_conex)
    df_gasoleosdapsa = df_gasoleosdapsa.convert_dtypes()
    df_gasoleosdapsa =df_gasoleosdapsa.fillna(0)
    ### Descuentos
    df_naftadapsaDesc = pd.read_sql('''

                DECLARE @ayer DATETIME
        SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))
        DECLARE @inicioMesActual DATETIME
        SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

        DECLARE @inicioMesAnterior DATETIME
        SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

        --Divide por la cant de días del mes anterior y multiplica por la cant de días del
        --mes actual
        
        DECLARE @hoy DATETIME
        SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 1, CURRENT_TIMESTAMP), 0)
        SELECT emp.UEN,EmP.FECHASQL
            ,EmP.[CODPRODUCTO]
            ,SUM(-EmP.[VOLUMEN]) as 'Descuentos'
        FROM [Rumaos].[dbo].[EmpPromo] AS EmP
            INNER JOIN Promocio AS P 
                ON EmP.UEN = P.UEN 
                AND EmP.CODPROMO = P.CODPROMO
        WHERE FECHASQL >= '2022-01-01'
            AND FECHASQL < @hoy
            and emp.UEN IN (
                'SARMIENTO'
                ,'URQUIZA'
                ,'MITRE'
                ,'MERC GUAYMALLEN'
                ,'ADOLFO CALLE'
                ,'VILLANUEVA'
                ,'MERCADO 2'
                ,'LAS HERAS'
            )
            and (emp.CODPRODUCTO = 'GO' OR emp.CODPRODUCTO = 'EU')
            AND (EmP.[CODPROMO] = '30'
                OR P.[DESCRIPCION] like '%PRUEBA%')
            GROUP BY emp.UEN,EMP.FECHASQL,EMP.CODPRODUCTO

    '''      ,db_conex)
    df_naftadapsaDesc = df_naftadapsaDesc.convert_dtypes()
    ### CONCATENO TABLA DE VOLUMEN TOTAL VENDIDO CON LA TABLA DE LOS DESCUENTOS
    df_gasoleosdapsa = df_gasoleosdapsa.merge(df_naftadapsaDesc, on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')
    df_gasoleosdapsa =df_gasoleosdapsa.fillna(0)


    df_gasoleosdapsa['Total Vendido $'] = ((df_gasoleosdapsa['Ventas Efectivo']*df_gasoleosdapsa['Precio Cartel'])
                                        +(df_gasoleosdapsa['Venta Cta Cte']*df_gasoleosdapsa['Precio Cta Cte'])
                                        +((df_gasoleosdapsa['ventas Promos']+df_gasoleosdapsa['Descuentos'])*df_gasoleosdapsa['Precio Cartel']))

    df_gasoleosdapsa['Volumen Total Vendido']=(df_gasoleosdapsa['Ventas Efectivo']+df_gasoleosdapsa['Venta Cta Cte']
                                            +(df_gasoleosdapsa['ventas Promos']+df_gasoleosdapsa['Descuentos']))


    df_gasoleosdapsa['UEN']= df_gasoleosdapsa['UEN'].str.strip()
    df_gasoleosdapsa['CODPRODUCTO']= df_gasoleosdapsa['CODPRODUCTO'].str.strip()

    # reemplazar los valores 0 en la columna 'Precio Cartel' con valores nulos (NaN)
    df_gasoleosdapsa['Precio Cartel'] = df_gasoleosdapsa['Precio Cartel'].replace(0, np.nan)
    # llenar los valores nulos utilizando la función fillna de Pandas con el argumento method='ffill'
    df_gasoleosdapsa['Precio Cartel'] = df_gasoleosdapsa['Precio Cartel'].fillna(method='ffill')
    df_gasoleosdapsa['Precio Cartel'] = df_gasoleosdapsa['Precio Cartel'].fillna(method='bfill')

    df_gasoleosdapsa = df_gasoleosdapsa.reindex(columns=['UEN','FECHASQL','CODPRODUCTO','Volumen Total Vendido','Total Vendido $','Total Vendido YER $', 'Precio Cartel'])
    df_gasoleosdapsa = df_gasoleosdapsa.fillna(0)
    df_gasoleosdapsa= df_gasoleosdapsa.loc[df_gasoleosdapsa["FECHASQL"] <= ayer.strftime('%Y-%m-%d'),:]

    df_gasoleosdapsa=df_gasoleosdapsa.sort_values(['UEN','CODPRODUCTO','FECHASQL'])

    ## FUNCION PARA APLICAR COMISION GO
    def aplicar_comision_acumuladaDAPSA(df):
        df=df.sort_values(['UEN','FECHASQL'])
        df = df.merge(costoDapsaGO,on=['FECHASQL','CODPRODUCTO'],how='outer')
        
        df['Ventas Acumuladas'] = df.groupby(df['FECHASQL'].dt.to_period('M'))['Volumen Total Vendido'].cumsum()    

        # convertir la columna 'Precio Cartel' a un tipo de datos EUmérico
        df['Precio Cartel'] = pd.to_numeric(df['Precio Cartel'], errors='coerce')

        # reemplazar los valores 0 en la columna 'Precio Cartel' con valores EUlos (NaN)
        df['Precio Cartel'] = df['Precio Cartel'].replace(0, np.nan)
        
        # llenar los valores EUlos utilizando la función fillna de Pandas con el argumento method='ffill'
        df['Precio Cartel'] = df['Precio Cartel'].fillna(method='bfill')
        
        df['COMISION']= df['Precio Cartel']-df['COSTO GO DAP']
        df['Comision $']= df['COMISION']*df['Volumen Total Vendido']
        df['Ventas Acumuladas $'] = df['Volumen Total Vendido']*df['Precio Cartel']
        df = df.reindex(columns=['UEN',  'CODPRODUCTO','FECHASQL','Ventas Acumuladas $','Ventas Acumuladas', 'Volumen Total Vendido', 'Comision $', 'COMISION', 'Precio Cartel'])

        return df

    ### Aplico la comision a cada estacion para el producto GO
    las_heras= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'LAS HERAS') & (df_gasoleosdapsa["CODPRODUCTO"] == 'GO'),:]
    las_heras = aplicar_comision_acumuladaDAPSA(las_heras)
    mercado1= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'MERC GUAYMALLEN') & (df_gasoleosdapsa["CODPRODUCTO"] == 'GO'),:]
    mercado1 = aplicar_comision_acumuladaDAPSA(mercado1)
    mercado2= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'MERCADO 2') & (df_gasoleosdapsa["CODPRODUCTO"] == 'GO'),:]
    mercado2 = aplicar_comision_acumuladaDAPSA(mercado2)
    sarmiento= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'SARMIENTO') & (df_gasoleosdapsa["CODPRODUCTO"] == 'GO'),:]
    sarmiento = aplicar_comision_acumuladaDAPSA(sarmiento)
    villa_Nueva= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'VILLANUEVA') & (df_gasoleosdapsa["CODPRODUCTO"] == 'GO'),:]
    villa_Nueva = aplicar_comision_acumuladaDAPSA(villa_Nueva)
    adolfo_Calle= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'ADOLFO CALLE') & (df_gasoleosdapsa["CODPRODUCTO"] == 'GO'),:]
    adolfo_Calle = aplicar_comision_acumuladaDAPSA(adolfo_Calle)
    mitre= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'MITRE') & (df_gasoleosdapsa["CODPRODUCTO"] == 'GO'),:]
    mitre = aplicar_comision_acumuladaDAPSA(mitre)
    urquiza= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'URQUIZA') & (df_gasoleosdapsa["CODPRODUCTO"] == 'GO'),:]
    urquiza = aplicar_comision_acumuladaDAPSA(urquiza)

    ## FUNCION PARA APLICAR COMISION EU
    def aplicar_comision_acumuladaDAPSAEU(df):
        df=df.sort_values(['UEN','FECHASQL'])
        df = df.merge(costoDapsaEU,on=['FECHASQL','CODPRODUCTO'],how='outer')
        # convertir la columna 'Precio Cartel' a un tipo de datos numérico
        df['Precio Cartel'] = pd.to_numeric(df['Precio Cartel'], errors='coerce')

        # reemplazar los valores 0 en la columna 'Precio Cartel' con valores nulos (NaN)
        df['Precio Cartel'] = df['Precio Cartel'].replace(0, np.nan)
        # llenar los valores nulos utilizando la función fillna de Pandas con el argumento method='ffill'
        df['Precio Cartel'] = df['Precio Cartel'].fillna(method='ffill')
        
        df['Comision']= df['Precio Cartel']-df['COSTO EU DAP']
        df['Comision $']= df['Comision']*df['Volumen Total Vendido']
        df['Ventas Acumuladas $'] = df['Volumen Total Vendido']*df['Precio Cartel']
        df = df.reindex(columns=['UEN', 'CODPRODUCTO','FECHASQL','Ventas Acumuladas $','Ventas Acumuladas', 'Volumen Total Vendido', 'Comision $', 'COMISION', 'Precio Cartel'])
        return df

    ### Aplico la comision a cada estacion para el producto EU

    amercado1= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'MERC GUAYMALLEN') & (df_gasoleosdapsa["CODPRODUCTO"] == 'EU'),:]
    amercado1 = aplicar_comision_acumuladaDAPSAEU(amercado1)
    amercado2= df_gasoleosdapsa.loc[(df_gasoleosdapsa["UEN"] == 'MERCADO 2') & (df_gasoleosdapsa["CODPRODUCTO"] == 'EU'),:]
    amercado2 = aplicar_comision_acumuladaDAPSAEU(amercado2)

    ### Concateno Tablas de Dapsa GO y EU
    mbcGODAPSA=pd.concat([las_heras, mercado1, mercado2, sarmiento, villa_Nueva, adolfo_Calle, mitre, urquiza,amercado1,amercado2], ignore_index=True)

    ### Concateno tablas de YPF y Dapsa TOTALES
    df_gasoleos= pd.concat([mbcGODAPSA, mbcYPF], ignore_index=True)
    df_gasoleos = df_gasoleos.dropna(subset=['UEN'])
    
    return df_gasoleos



def combustible_gnc():    
    df_gncYPF = pd.read_sql('''
                DECLARE @ayer DATETIME
            SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))
            DECLARE @inicioMesActual DATETIME
            SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

                DECLARE @inicioMesAnterior DATETIME
                SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

                --Divide por la cant de días del mes anterior y multiplica por la cant de días del
                --mes actual

                DECLARE @hoy DATETIME
                SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

                --Divide por la cantidad de días cursados del mes actual y multiplica por la cant
                --de días del mes actual

            SELECT UEN  
                ,SUM([VTATOTVOL]) AS 'VENTA TOTAL VOLUMEN'
                ,FECHASQL
                ,sum(VTAPRECARTELVOL) AS 'Ventas Efectivo'
                ,MAX(PRECARTEL) as 'Precio Cartel'
                ,sum(VTACTACTEVOL + VTAADELVOL) AS 'Venta Cta Cte'
                ,MAX(PREVTAADEL) AS 'Precio Cta Cte'
                ,sum(VTAPROMOSVOL) AS 'ventas Promos pc'
                ,CODPRODUCTO
            FROM [Rumaos].[dbo].[EmpVenta]
            WHERE FECHASQL >= '2022-01-01'
                AND FECHASQL < @hoy
                AND VTATOTVOL > '0'
                and CODPRODUCTO = 'GNC'
                group BY FECHASQL,UEN, CODPRODUCTO
                order by UEN

    '''      ,db_conex)
    df_gncYPF = df_gncYPF.convert_dtypes()


    ######################
    ### VOLUMEN PROMOS ###
    ######################

    df_GNCpromos = pd.read_sql(''' 
    DECLARE @ayer DATETIME
        SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))
        DECLARE @inicioMesActual DATETIME
        SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

        DECLARE @inicioMesAnterior DATETIME
        SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

        --Divide por la cant de días del mes anterior y multiplica por la cant de días del
        --mes actual
        
        DECLARE @hoy DATETIME
        SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)
        SELECT   emp.UEN,MAX(emp.PRECIO)as 'precio promo',sum(emp.VOLUMEN) as 'volumen promo',emp.FECHASQL
        FROM [Rumaos].[dbo].[EmpPromo] AS EmP
            INNER JOIN Promocio AS P 
                ON EmP.UEN = P.UEN 
                AND EmP.CODPROMO = P.CODPROMO
        WHERE FECHASQL >= '2022-01-01'
            AND FECHASQL < @hoy
            and (emp.CODPRODUCTO = 'GNC')
            AND (P.[DESCRIPCION] like '%PRUEBA%')
    GROUP BY FECHASQL,emp.UEN
    '''  ,db_conex)
    df_GNCpromos = df_GNCpromos.convert_dtypes()
    df_gncYPF=df_gncYPF.merge(df_GNCpromos,on=['UEN','FECHASQL'],how='outer')

    df_gncYPF['UEN']=df_gncYPF['UEN'].str.strip()
    df_gncYPF['CODPRODUCTO']=df_gncYPF['CODPRODUCTO'].str.strip()

    # convertir la columna 'Precio Cartel' a un tipo de datos numérico
    df_gncYPF['Precio Cartel'] = pd.to_numeric(df_gncYPF['Precio Cartel'], errors='coerce')

    # reemplazar los valores 0 en la columna 'Precio Cartel' con valores nulos (NaN)
    df_gncYPF['Precio Cartel'] = df_gncYPF['Precio Cartel'].replace(0, np.nan)
    # llenar los valores nulos utilizando la función fillna de Pandas con el argumento method='ffill'
    df_gncYPF['Precio Cartel'] = df_gncYPF['Precio Cartel'].fillna(method='ffill')
    df_gncYPF['Precio Cartel'] = df_gncYPF['Precio Cartel'].fillna(method='bfill')  

    df_gncYPF= df_gncYPF.loc[(df_gncYPF["FECHASQL"] <= ayer.strftime('%Y-%m-%d')),:]


    df_gncYPF=df_gncYPF.fillna(0)
    df_gncYPF['Total Vendido $'] = ((df_gncYPF['Ventas Efectivo']*df_gncYPF['Precio Cartel'])
                                        +(df_gncYPF['Venta Cta Cte']*df_gncYPF['Precio Cta Cte'])
                                        )
    df_gncYPF['Cantidad Vendida PC'] = (df_gncYPF['Ventas Efectivo']+
                                    +df_gncYPF['Venta Cta Cte']+
                                    df_gncYPF['ventas Promos pc']-
                                    df_gncYPF['volumen promo']
                                    )
    df_gncYPF=df_gncYPF.sort_values(['UEN','FECHASQL'])



    def mbcypf(df,estacion):
        df=df.sort_values(['UEN','FECHASQL'])
        costo = costoGNC.loc[(costoGNC['UEN'] == estacion)]
        df= df.merge(costo, on=['UEN', 'CODPRODUCTO', 'FECHASQL'], how= 'outer')
        df['Costo M3'] = df['Costo M3'].fillna(method='ffill')
        df['COMISION']=df['Precio Cartel']-df['Costo M3']
        df['Comision $']= df['COMISION']*df['Cantidad Vendida PC']
        df['Ventas Acumuladas $']= (df['Cantidad Vendida PC']*df['Precio Cartel'])
        df['Ventas Acumuladas'] = df.groupby(df['FECHASQL'].dt.to_period('M'))['Cantidad Vendida PC'].cumsum()    
        df['Volumen Total Vendido']=df['Cantidad Vendida PC']
        df = df.reindex(columns=['UEN',  'CODPRODUCTO','FECHASQL','Ventas Acumuladas $','Ventas Acumuladas', 'Volumen Total Vendido', 'Comision $', 'COMISION', 'Precio Cartel'])

        
        return df

    ### Aplico la comision a cada estacion para el producto NS
    perdriel1= df_gncYPF.loc[(df_gncYPF["UEN"] == 'PERDRIEL'),:]
    p1=perdriel1
    p1 = p1.reset_index()
    perdriel2= df_gncYPF.loc[(df_gncYPF["UEN"] == 'PERDRIEL2'),:]
    perdriel2=perdriel2.reset_index()
    for i in perdriel2.index:
        if perdriel2.loc[i,'precio promo']==0:
            perdriel2.loc[i,'precio promo'] = p1.loc[i,'precio promo']

    azcuenaga= df_gncYPF.loc[(df_gncYPF["UEN"] == 'AZCUENAGA'),:]
    san_Jose= df_gncYPF.loc[(df_gncYPF["UEN"] == 'SAN JOSE'),:]
    puente_Olive= df_gncYPF.loc[(df_gncYPF["UEN"] == 'PUENTE OLIVE'),:]
    lamadrid= df_gncYPF.loc[(df_gncYPF["UEN"] == 'LAMADRID'),:]
    las_heras= df_gncYPF.loc[(df_gncYPF["UEN"] == 'LAS HERAS'),:]
    mercado1= df_gncYPF.loc[(df_gncYPF["UEN"] == 'MERC GUAYMALLEN'),:]
    mercado2= df_gncYPF.loc[(df_gncYPF["UEN"] == 'MERCADO 2'),:]
    sarmiento= df_gncYPF.loc[(df_gncYPF["UEN"] == 'SARMIENTO'),:]
    villa_nueva= df_gncYPF.loc[(df_gncYPF["UEN"] == 'VILLANUEVA'),:]
    adolfo_Calle= df_gncYPF.loc[(df_gncYPF["UEN"] == 'ADOLFO CALLE'),:]
    mitre= df_gncYPF.loc[(df_gncYPF["UEN"] == 'MITRE'),:]
    urquiza= df_gncYPF.loc[(df_gncYPF["UEN"] == 'URQUIZA'),:]

    perdriel1 = mbcypf(perdriel1,'PERDRIEL')
    perdriel2 = mbcypf(perdriel2,'PERDRIEL2')
    azcuenaga = mbcypf(azcuenaga,'AZCUENAGA')
    san_Jose = mbcypf(san_Jose,'SAN JOSE')
    puente_Olive = mbcypf(puente_Olive,'PUENTE OLIVE')
    lamadrid = mbcypf(lamadrid,'LAMADRID')
    las_heras = mbcypf(las_heras,'LAS HERAS')
    mercado1 = mbcypf(mercado1,'MERC GUAYMALLEN')
    mercado2 = mbcypf(mercado2,'MERCADO 2')
    sarmiento = mbcypf(sarmiento,'SARMIENTO')
    villa_nueva = mbcypf(villa_nueva,'VILLANUEVA')
    adolfo_Calle = mbcypf(adolfo_Calle,'ADOLFO CALLE')
    mitre = mbcypf(mitre,'MITRE')
    urquiza = mbcypf(urquiza,'URQUIZA')

    df_gnc=pd.concat([perdriel1, perdriel2, azcuenaga, san_Jose, puente_Olive, las_heras, lamadrid, mercado1, mercado2, sarmiento, villa_nueva, adolfo_Calle, mitre, urquiza], ignore_index=True)
    df_gnc=df_gnc.rename({'Cantidad Vendida PC':'Volumen Total Vendido'},axis=1)
    df_gnc=df_gnc.sort_values(['UEN','FECHASQL'])
    
    return df_gnc


from oauth2client.service_account import ServiceAccountCredentials
import gspread
def main():
    df_nafta=combustible_nafta()
    df_gasoleos=combustible_gasoleos()
    df_gnc=combustible_gnc()
    
    #df_combustibles=pd.concat([df_nafta, df_gasoleos, df_gnc], ignore_index=True)
    
    scope= ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    ruta="C:/Users/gmartinez/Desktop/new/Informes/PRESUPUESTO/curious-athlete-393417-45edffc04e53.json"
    credenciales = ServiceAccountCredentials.from_json_keyfile_name(ruta, scope)
    cliente = gspread.authorize(credenciales)
    sheet= cliente.open("MBC sin calcular").sheet1

    df_nafta = df_nafta.fillna(0)
    df_gasoleos = df_gasoleos.fillna(0)
    df_gnc = df_gnc.fillna(0)


    
    
    #Reemplazar valores nulos por cero para evitar problemas en el proceso de actualización del excel
    sheet.clear()
    sheet.resize(1, 1)
    df_nafta["FECHASQL"] = pd.to_datetime(df_nafta["FECHASQL"])
    df_nafta["FECHASQL"] = df_nafta["FECHASQL"].dt.strftime("%Y-%m-%d")
    sheet.append_rows([df_nafta.columns.values.tolist()]+ df_nafta.values.tolist())
    
    df_gasoleos["FECHASQL"] = pd.to_datetime(df_gasoleos["FECHASQL"])
    df_gasoleos["FECHASQL"] = df_gasoleos["FECHASQL"].dt.strftime("%Y-%m-%d")
    sheet.append_rows([df_gasoleos.columns.values.tolist()]+ df_gasoleos.values.tolist())
    #sheet.add_rows([df_gasoleos.columns.values.tolist()]+ df_gasoleos.values.tolist())
    
    df_gnc["FECHASQL"] = pd.to_datetime(df_gnc["FECHASQL"])
    df_gnc["FECHASQL"] = df_gnc["FECHASQL"].dt.strftime("%Y-%m-%d")
    sheet.append_rows([df_gnc.columns.values.tolist()]+ df_gnc.values.tolist())
    #sheet.add_rows([df_gnc.columns.values.tolist()]+ df_gnc.values.tolist())
    
    print("Carga por lotes completada.")
    
if __name__ == "__main__":
    main()