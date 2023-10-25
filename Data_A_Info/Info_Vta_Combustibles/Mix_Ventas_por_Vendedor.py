from calendar import monthrange
import os
import dataframe_image as dfi
from DatosLogin import login
from Conectores import conectorMSSQL
import pandas as pd
import pyodbc #Library to connect to Microsoft SQL Server
from DatosLogin import login #Holds connection data to the SQL Server
import sys
import pathlib

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

    
#### Creo datos para volumen Proyectado
diasdelmes=(tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d")
mes=(tiempoInicio-pd.to_timedelta(1,"days")).strftime("%m")
año=(tiempoInicio-pd.to_timedelta(1,"days")).strftime("%y")
diasdelmes = int(diasdelmes)
mes=int(mes)
año=int(año)
num_days = monthrange(año,mes)[1] # num_days = 31.
num_days=int(num_days)


###########################################################################
##############################   Volumen por Vendedor Mes Actual ######################################
###########################################################################
volumXvendedor = pd.read_sql(
 ''' 
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
	select t.Vendedor,sum(t.GO) AS 'Ultra Diesel',sum(t.EU) as 'Infinia Diesel',sum(t.NS) as 'Nafta Super',sum(t.NU) as 'Infinia Nafta',sum(t.GNC) as 'GNC' from
	(select a.Vendedor, 
	IIF(a.Producto = 'NS',a.cantidad,0) as NS,
	IIF(a.Producto = 'NU',a.cantidad,0) as NU,
	IIF(a.Producto = 'GO',a.cantidad,0) as 'GO',
	IIF(a.Producto = 'EU',a.cantidad,0) as EU,
	IIF(a.Producto = 'GNC',a.cantidad,0) as GNC from
	(SELECT
            ISNULL(RTRIM(Vend.NOMBREVEND),'') as 'Vendedor'
			, sum(FRD.importe) as 'venta en cta/cte'
			,frd.CODPRODUCTO
			,sum(frd.CANTIDAD) as Cantidad

			, iif(frd.codproducto = 'G40VA' or frd.codproducto = 'GO40' or frd.codproducto = 'GOCA'
			or frd.codproducto = 'GOCAS' or frd.codproducto = 'GOCC' or frd.codproducto = 'GOIVA' or frd.codproducto = 'GORUT' or 
			frd.codproducto = 'GO' or frd.codproducto = 'GOYVA','GO'
			,IIF(frd.codproducto = 'E40VA' or frd.codproducto = 'EU' or frd.codproducto = 'EU40'
			or frd.codproducto = 'EUCA' or frd.codproducto = 'EUIVA' or frd.codproducto = 'EUMA' or frd.codproducto = 'EUYVA' or 
			frd.codproducto = 'GOYVA','EU',IIF(frd.codproducto = 'GNC' or frd.codproducto = 'GNC02' or frd.codproducto = 'GNC03'
			or frd.codproducto = 'GNCCA' or frd.codproducto = 'GNCCC' or frd.codproducto = 'GNCCO' or frd.codproducto = 'GNCVA','GNC',IIF(
			frd.codproducto = 'N40VA' or frd.codproducto = 'NS' or frd.codproducto = 'NS03'
			or frd.codproducto = 'NS40' or frd.codproducto = 'NS40V' or frd.codproducto = 'NSCA' or frd.codproducto = 'NSCC ','NS',IIF(
			frd.codproducto = 'NU' or frd.codproducto = 'NUCA' or frd.codproducto = 'NUCC','NU',''))))) as Producto
            FROM [Rumaos].[dbo].[FacRemDet] as FRD with (NOLOCK)
            INNER JOIN dbo.FacCli as Cli with (NOLOCK)
                ON FRD.NROCLIENTE = Cli.NROCLIPRO
            LEFT OUTER JOIN dbo.Vendedores as Vend with (NOLOCK)
	            ON Cli.NROVEND = Vend.NROVEND

            where FRD.NROCLIENTE > '100000'
                and frd.FECHASQL >= @inicioMesActual and frd.FECHASQL < @hoy
				and frd.CODPRODUCTO not in ('LUBCA','LUBCA','LUBCC','LUCA','LUF50','104.6','ANT','AZ10','AZ20','BA01')
				and frd.CANTIDAD > 0

            group by Vend.NOMBREVEND,frd.CODPRODUCTO) as a
			group by a.Vendedor,a.Cantidad,a.Producto) as t
            where t.vendedor != ''
			group by t.vendedor
 ''' 
  ,db_conex)
volumXvendedor = volumXvendedor.convert_dtypes()


volumXvendedor['MIX Gasoleos']= (volumXvendedor['Infinia Diesel']/diasdelmes*num_days)/((volumXvendedor['Ultra Diesel']/diasdelmes*num_days)+(volumXvendedor['Infinia Diesel']/diasdelmes*num_days))

volumXvendedor['MIX Naftas']= (volumXvendedor['Infinia Nafta']/diasdelmes*num_days)/((volumXvendedor['Nafta Super']/diasdelmes*num_days)+(volumXvendedor['Infinia Nafta']/diasdelmes*num_days))

volumXvendedor['Volumen Naftas'] = (volumXvendedor['Infinia Nafta']/diasdelmes*num_days) + (volumXvendedor['Nafta Super']/diasdelmes*num_days)

volumXvendedor['Volumen Gasoleos'] = (volumXvendedor['Infinia Diesel']/diasdelmes*num_days) + (volumXvendedor['Ultra Diesel']/diasdelmes*num_days)

###########################################################################
##############################   Volumen por Vendedor Mes Anterior ######################################
###########################################################################
volumXvendedorMesAnt = pd.read_sql(
 ''' 
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

    DECLARE @hoyMesPasado DATE
    SET @hoyMesPasado = CAST(DATEADD(month, -1, GETDATE()) AS DATE)
        --Divide por la cantidad de días cursados del mes actual y multiplica por la cant
        --de días del mes actual
	select t.Vendedor,sum(t.GO) AS 'Ultra Diesel Mes Anterior',sum(t.EU) as 'Infinia Diesel Mes Anterior',sum(t.NS) as 'Nafta Super Mes Anterior',sum(t.NU) as 'Infinia Nafta Mes Anterior',sum(t.GNC) as 'GNC Mes Anterior' from
	(select a.Vendedor, 
	IIF(a.Producto = 'NS',a.cantidad,0) as NS,
	IIF(a.Producto = 'NU',a.cantidad,0) as NU,
	IIF(a.Producto = 'GO',a.cantidad,0) as 'GO',
	IIF(a.Producto = 'EU',a.cantidad,0) as EU,
	IIF(a.Producto = 'GNC',a.cantidad,0) as GNC from
	(SELECT
            ISNULL(RTRIM(Vend.NOMBREVEND),'') as 'Vendedor'
			, sum(FRD.importe) as 'venta en cta/cte'
			,frd.CODPRODUCTO
			,sum(frd.CANTIDAD) as Cantidad

			, iif(frd.codproducto = 'G40VA' or frd.codproducto = 'GO40' or frd.codproducto = 'GOCA'
			or frd.codproducto = 'GOCAS' or frd.codproducto = 'GOCC' or frd.codproducto = 'GOIVA' or frd.codproducto = 'GORUT' or 
			frd.codproducto = 'GO' or frd.codproducto = 'GOYVA','GO'
			,IIF(frd.codproducto = 'E40VA' or frd.codproducto = 'EU' or frd.codproducto = 'EU40'
			or frd.codproducto = 'EUCA' or frd.codproducto = 'EUIVA' or frd.codproducto = 'EUMA' or frd.codproducto = 'EUYVA' or 
			frd.codproducto = 'GOYVA','EU',IIF(frd.codproducto = 'GNC' or frd.codproducto = 'GNC02' or frd.codproducto = 'GNC03'
			or frd.codproducto = 'GNCCA' or frd.codproducto = 'GNCCC' or frd.codproducto = 'GNCCO' or frd.codproducto = 'GNCVA','GNC',IIF(
			frd.codproducto = 'N40VA' or frd.codproducto = 'NS' or frd.codproducto = 'NS03'
			or frd.codproducto = 'NS40' or frd.codproducto = 'NS40V' or frd.codproducto = 'NSCA' or frd.codproducto = 'NSCC ','NS',IIF(
			frd.codproducto = 'NU' or frd.codproducto = 'NUCA' or frd.codproducto = 'NUCC','NU',''))))) as Producto
            FROM [Rumaos].[dbo].[FacRemDet] as FRD with (NOLOCK)
            INNER JOIN dbo.FacCli as Cli with (NOLOCK)
                ON FRD.NROCLIENTE = Cli.NROCLIPRO
            LEFT OUTER JOIN dbo.Vendedores as Vend with (NOLOCK)
	            ON Cli.NROVEND = Vend.NROVEND

            where FRD.NROCLIENTE > '100000'
                and frd.FECHASQL >= @inicioMesAnterior and frd.FECHASQL < @inicioMesActual
				and frd.CODPRODUCTO not in ('LUBCA','LUBCA','LUBCC','LUCA','LUF50','104.6','ANT','AZ10','AZ20','BA01')
				and frd.CANTIDAD > 0

            group by Vend.NOMBREVEND,frd.CODPRODUCTO) as a
			group by a.Vendedor,a.Cantidad,a.Producto) as t
            where t.vendedor != ''
			group by t.vendedor
 ''' 
  ,db_conex)
volumXvendedorMesAnt = volumXvendedorMesAnt.convert_dtypes()

volumXvendedorMesAnt['MIX Gasoleos Intermensual']= volumXvendedorMesAnt['Infinia Diesel Mes Anterior']/(volumXvendedorMesAnt['Ultra Diesel Mes Anterior']+volumXvendedorMesAnt['Infinia Diesel Mes Anterior'])
volumXvendedorMesAnt['MIX Naftas Intermensual']= volumXvendedorMesAnt['Infinia Nafta Mes Anterior']/(volumXvendedorMesAnt['Nafta Super Mes Anterior']+volumXvendedorMesAnt['Infinia Nafta Mes Anterior'])

volumXvendedorMesAnt['Volumen Naftas Intermensual']= volumXvendedorMesAnt['Infinia Nafta Mes Anterior']+volumXvendedorMesAnt['Nafta Super Mes Anterior']
volumXvendedorMesAnt['Volumen Gasoleos Intermensual']= volumXvendedorMesAnt['Infinia Diesel Mes Anterior']+volumXvendedorMesAnt['Ultra Diesel Mes Anterior']

###########################################################################
##############################   Volumen por Vendedor Mes Actual Año Anterior ######################################
###########################################################################
volumXvendedorAñoAnt = pd.read_sql(
 ''' 
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

    -- Inicio del mes actual del año anterior
    DECLARE @inicioMesActualAñoAnterior DATETIME
    SET @inicioMesActualAñoAnterior = DATEADD(year, -1, @inicioMesActual)

    -- Fin del mes actual del año anterior
    DECLARE @finMesActualAñoAnterior DATETIME
    SET @finMesActualAñoAnterior = DATEADD(DAY, -1, EOMONTH(@inicioMesActualAñoAnterior))

    DECLARE @hoyAñoAnterior DATE
    SET @hoyAñoAnterior = CAST(DATEADD(year, -1, GETDATE()) AS DATE)

        --Divide por la cantidad de días cursados del mes actual y multiplica por la cant
        --de días del mes actual
	select t.Vendedor,sum(t.GO) AS 'Ultra Diesel Interanual',sum(t.EU) as 'Infinia Diesel Interanual',sum(t.NS) as 'Nafta Super Interanual',sum(t.NU) as 'Infinia Nafta Interanual',sum(t.GNC) as 'GNC Interanual' from
	(select a.Vendedor, 
	IIF(a.Producto = 'NS',a.cantidad,0) as NS,
	IIF(a.Producto = 'NU',a.cantidad,0) as NU,
	IIF(a.Producto = 'GO',a.cantidad,0) as 'GO',
	IIF(a.Producto = 'EU',a.cantidad,0) as EU,
	IIF(a.Producto = 'GNC',a.cantidad,0) as GNC from
	(SELECT
            ISNULL(RTRIM(Vend.NOMBREVEND),'') as 'Vendedor'
			, sum(FRD.importe) as 'venta en cta/cte'
			,frd.CODPRODUCTO
			,sum(frd.CANTIDAD) as Cantidad

			, iif(frd.codproducto = 'G40VA' or frd.codproducto = 'GO40' or frd.codproducto = 'GOCA'
			or frd.codproducto = 'GOCAS' or frd.codproducto = 'GOCC' or frd.codproducto = 'GOIVA' or frd.codproducto = 'GORUT' or 
			frd.codproducto = 'GO' or frd.codproducto = 'GOYVA','GO'
			,IIF(frd.codproducto = 'E40VA' or frd.codproducto = 'EU' or frd.codproducto = 'EU40'
			or frd.codproducto = 'EUCA' or frd.codproducto = 'EUIVA' or frd.codproducto = 'EUMA' or frd.codproducto = 'EUYVA' or 
			frd.codproducto = 'GOYVA','EU',IIF(frd.codproducto = 'GNC' or frd.codproducto = 'GNC02' or frd.codproducto = 'GNC03'
			or frd.codproducto = 'GNCCA' or frd.codproducto = 'GNCCC' or frd.codproducto = 'GNCCO' or frd.codproducto = 'GNCVA','GNC',IIF(
			frd.codproducto = 'N40VA' or frd.codproducto = 'NS' or frd.codproducto = 'NS03'
			or frd.codproducto = 'NS40' or frd.codproducto = 'NS40V' or frd.codproducto = 'NSCA' or frd.codproducto = 'NSCC ','NS',IIF(
			frd.codproducto = 'NU' or frd.codproducto = 'NUCA' or frd.codproducto = 'NUCC','NU',''))))) as Producto
            FROM [Rumaos].[dbo].[FacRemDet] as FRD with (NOLOCK)
            INNER JOIN dbo.FacCli as Cli with (NOLOCK)
                ON FRD.NROCLIENTE = Cli.NROCLIPRO
            LEFT OUTER JOIN dbo.Vendedores as Vend with (NOLOCK)
	            ON Cli.NROVEND = Vend.NROVEND

            where FRD.NROCLIENTE > '100000'
                and frd.FECHASQL >= @inicioMesActualAñoAnterior and frd.FECHASQL <= @finMesActualAñoAnterior
				and frd.CODPRODUCTO not in ('LUBCA','LUBCA','LUBCC','LUCA','LUF50','104.6','ANT','AZ10','AZ20','BA01')
				and frd.CANTIDAD > 0

            group by Vend.NOMBREVEND,frd.CODPRODUCTO) as a
			group by a.Vendedor,a.Cantidad,a.Producto) as t
            where t.vendedor != ''
			group by t.vendedor
 ''' 
  ,db_conex)
volumXvendedorAñoAnt = volumXvendedorAñoAnt.convert_dtypes()


volumXvendedorAñoAnt['MIX Gasoleos Interanual']= volumXvendedorAñoAnt['Infinia Diesel Interanual']/(volumXvendedorAñoAnt['Ultra Diesel Interanual']+volumXvendedorAñoAnt['Infinia Diesel Interanual'])
volumXvendedorAñoAnt['MIX Naftas Interanual']= volumXvendedorAñoAnt['Infinia Nafta Interanual']/(volumXvendedorAñoAnt['Nafta Super Interanual']+volumXvendedorAñoAnt['Infinia Nafta Interanual'])
volumXvendedorAñoAnt['Volumen Naftas Interanual']= volumXvendedorAñoAnt['Infinia Nafta Interanual']+volumXvendedorAñoAnt['Nafta Super Interanual']
volumXvendedorAñoAnt['Volumen Gasoleos Interanual']= volumXvendedorAñoAnt['Infinia Diesel Interanual']+volumXvendedorAñoAnt['Ultra Diesel Interanual']

df_volumVendedor=volumXvendedor.merge(volumXvendedorMesAnt,on=['Vendedor'],how='outer')
df_volumVendedor=df_volumVendedor.merge(volumXvendedorAñoAnt,on=['Vendedor'],how='outer')

df_volumVendedor=df_volumVendedor.fillna(0)
# BANDERA
def _bandera(Vendedor):
    if Vendedor in ["CADILE ALEJANDRO"]:
        return "CADILE ALEJANDRO"
    elif Vendedor in ["RICARDO GAVIO"]:
        return "RICARDO GAVIO"
    elif Vendedor in ["GONZALEZ GERARD0"]:
        return "GONZALEZ GERARD0"
    elif Vendedor in ["EDUARDO VILLAR"]:
        return "EDUARDO VILLAR"
    else:
        return "OTROS"

### Creo una columna con el nombre de la bandera
df_volumVendedor["VENDEDOR"] = df_volumVendedor.apply(
    lambda row: _bandera(row["Vendedor"])
        , axis= 1
)

df_volumVendedor = df_volumVendedor.groupby("VENDEDOR")[['Volumen Gasoleos','MIX Gasoleos','Volumen Gasoleos Intermensual'
                                              ,'MIX Gasoleos Intermensual','Volumen Gasoleos Interanual','MIX Gasoleos Interanual',
                                               'Infinia Diesel Interanual','Infinia Diesel Mes Anterior','Infinia Diesel'
                                                ,'Ultra Diesel Interanual','Ultra Diesel Mes Anterior','Ultra Diesel',
                                                'Volumen Naftas','MIX Naftas','Volumen Naftas Intermensual'
                                              ,'MIX Naftas Intermensual','Volumen Naftas Interanual','MIX Naftas Interanual',
                                               'Infinia Nafta Interanual','Infinia Nafta Mes Anterior','Infinia Nafta'
                                                ,'Nafta Super Interanual','Nafta Super Mes Anterior','Nafta Super']].sum().reset_index()


################## NAFTAS ####################
df_volVendedorNaftas=df_volumVendedor.reindex(columns=['VENDEDOR','Volumen Naftas','MIX Naftas','Volumen Naftas Intermensual'
                                              ,'MIX Naftas Intermensual','Volumen Naftas Interanual','MIX Naftas Interanual',
                                               'Infinia Nafta Interanual','Infinia Nafta Mes Anterior','Infinia Nafta'
                                                ,'Nafta Super Interanual','Nafta Super Mes Anterior','Nafta Super'])
df_volVendedorNaftas= df_volVendedorNaftas.loc[df_volVendedorNaftas["Volumen Naftas"] > 1,:]

for col in df_volVendedorNaftas.columns:
    if col != 'VENDEDOR':
        df_volVendedorNaftas[col] = df_volVendedorNaftas[col].astype(str).replace('nan', '0').astype(float)
        
df_volVendedorNaftas['Desvio Mix Intermensual %']=((df_volVendedorNaftas['MIX Naftas'])/df_volVendedorNaftas['MIX Naftas Intermensual'])-1
df_volVendedorNaftas['Desvio Mix Interanual %']=((df_volVendedorNaftas['MIX Naftas'])/df_volVendedorNaftas['MIX Naftas Interanual'])-1 

df_volVendedorNaftas['Desvio Volumen Intermensual %']=((df_volVendedorNaftas['Volumen Naftas'])/df_volVendedorNaftas['Volumen Naftas Intermensual'])-1
df_volVendedorNaftas['Desvio Volumen Interanual %']= ((df_volVendedorNaftas['Volumen Naftas'])/df_volVendedorNaftas['Volumen Naftas Interanual'])-1


for col in df_volVendedorNaftas.columns:
    if col != 'VENDEDOR':
        df_volVendedorNaftas[col] = df_volVendedorNaftas[col].astype(str).replace('nan', '0').astype(float)
        df_volVendedorNaftas[col] = df_volVendedorNaftas[col].astype(str).replace('inf', '1').astype(float)

df_volVendedorNaftas=df_volVendedorNaftas.sort_values('Volumen Naftas', ascending=False)

def _objetivoNafta(Vendedor):
    if Vendedor in ["CADILE ALEJANDRO"]:
        return 0.36
    elif Vendedor in ["RICARDO GAVIO"]:
        return 0.31
    elif Vendedor in ["GONZALEZ GERARD0"]:
        return 0.36
    elif Vendedor in ["EDUARDO VILLAR"]:
        return 0.36
    else:
        return 0.36

### Creo una columna con el nombre de la bandera
df_volVendedorNaftas["Objetivo"] = df_volVendedorNaftas.apply(
    lambda row: _objetivoNafta(row["VENDEDOR"])
        , axis= 1
)
df_volVendedorNaftas['Desvio Objetivo']=(df_volVendedorNaftas['MIX Naftas']-df_volVendedorNaftas['Objetivo'])/df_volVendedorNaftas['Objetivo']
df_volVendedorNaftas= df_volVendedorNaftas.loc[(df_volVendedorNaftas["VENDEDOR"] != 'OTROS'),:]

######### Totales Naftas
df_volVendedorNaftas.loc["colTOTAL"]= pd.Series(
    df_volVendedorNaftas.sum()
    , index=['Infinia Nafta Interanual','Infinia Nafta Mes Anterior','Infinia Nafta','Nafta Super Interanual','Nafta Super Mes Anterior','Nafta Super'
            ,'Volumen Naftas Intermensual','Volumen Naftas'
            ,'Volumen Naftas Interanual']
)
df_volVendedorNaftas.fillna({"VENDEDOR":"TOTAL"}, inplace=True)
df_volVendedorNaftas.fillna({"Objetivo":0.36}, inplace=True)

# Totales Mix

tasa = (df_volVendedorNaftas.loc["colTOTAL",'Infinia Nafta']/diasdelmes*num_days)/((df_volVendedorNaftas.loc["colTOTAL",'Infinia Nafta']/diasdelmes*num_days)+(df_volVendedorNaftas.loc["colTOTAL",'Nafta Super']/diasdelmes*num_days))
df_volVendedorNaftas.fillna({'MIX Naftas':tasa}, inplace=True)

tasa2 = (df_volVendedorNaftas.loc["colTOTAL",'Infinia Nafta Mes Anterior'] /(
    df_volVendedorNaftas.loc["colTOTAL",'Infinia Nafta Mes Anterior']+df_volVendedorNaftas.loc["colTOTAL",'Nafta Super Mes Anterior']))
df_volVendedorNaftas.fillna({'MIX Naftas Intermensual':tasa2}, inplace=True)       
        
tasa3 = (df_volVendedorNaftas.loc["colTOTAL",'Infinia Nafta Interanual'] /(
    df_volVendedorNaftas.loc["colTOTAL",'Infinia Nafta Interanual']+df_volVendedorNaftas.loc["colTOTAL",'Nafta Super Interanual']))
df_volVendedorNaftas.fillna({'MIX Naftas Interanual':tasa2}, inplace=True)    
#Totales Desvios
tasa4 = ((df_volVendedorNaftas.loc["colTOTAL",'MIX Naftas']) /
    df_volVendedorNaftas.loc["colTOTAL",'MIX Naftas Intermensual'])-1
df_volVendedorNaftas.fillna({'Desvio Mix Intermensual %':tasa4}, inplace=True)       
        
tasa5 = ((df_volVendedorNaftas.loc["colTOTAL",'MIX Naftas']) /
    df_volVendedorNaftas.loc["colTOTAL",'MIX Naftas Interanual'])-1
df_volVendedorNaftas.fillna({'Desvio Mix Interanual %':tasa5}, inplace=True) 


tasa6 = ((df_volVendedorNaftas.loc["colTOTAL",'Volumen Naftas']) /
    df_volVendedorNaftas.loc["colTOTAL",'Volumen Naftas Intermensual'])-1
df_volVendedorNaftas.fillna({'Desvio Volumen Intermensual %':tasa6}, inplace=True)       
        
tasa7 = ((df_volVendedorNaftas.loc["colTOTAL",'Volumen Naftas']) /
    df_volVendedorNaftas.loc["colTOTAL",'Volumen Naftas Interanual'])-1
df_volVendedorNaftas.fillna({'Desvio Volumen Interanual %':tasa7}, inplace=True) 

tasa7 = ((df_volVendedorNaftas.loc["colTOTAL",'MIX Naftas']) /
    df_volVendedorNaftas.loc["colTOTAL",'Objetivo'])-1
df_volVendedorNaftas.fillna({'Desvio Objetivo':tasa7}, inplace=True) 



df_volVendedorNaftas=df_volVendedorNaftas.reindex(columns=['VENDEDOR','Volumen Naftas','MIX Naftas','Objetivo','Desvio Objetivo'
                                            ,'Desvio Volumen Intermensual %','Desvio Mix Intermensual %'
                                            ,'Desvio Volumen Interanual %','Desvio Mix Interanual %'])        

df_volVendedorNaftas=df_volVendedorNaftas.rename({'Volumen Naftas':'Volumen Naftas Proyectado Mensual'
                                                      ,'MIX Naftas':'MIX Naftas Proyectado Mensual'
                                                     ,'MIX Naftas Intermensual':'MIX Naftas Mes Anterior'
                                                     ,'Volumen Naftas Intermensual':'Volumen Naftas Mes Anterior'
                                                     ,'MIX Naftas Interanual':'MIX Naftas Año Anterior'
                                                     ,'Volumen Naftas Interanual':'Volumen Naftas Año Anterior'
                                                     ,'Desvio Volumen Interanual %': 'Evolucion Volumen Interanual %'
                                                     ,'Desvio Volumen Intermensual %':'Evolucion Volumen Intermensual %'
                                                     ,'Desvio Mix Intermensual %':'Evolucion Mix Intermensual %'
                                                     ,'Desvio Mix Interanual %':'Evolucion Mix Interanual %'},axis=1)
        

    
################## GASOLEOS #################### 

df_volVendedorGasoleos=df_volumVendedor.reindex(columns=['VENDEDOR','Volumen Gasoleos','MIX Gasoleos','Volumen Gasoleos Intermensual'
                                              ,'MIX Gasoleos Intermensual','Volumen Gasoleos Interanual','MIX Gasoleos Interanual',
                                               'Infinia Diesel Interanual','Infinia Diesel Mes Anterior','Infinia Diesel'
                                                ,'Ultra Diesel Interanual','Ultra Diesel Mes Anterior','Ultra Diesel'])

for col in df_volVendedorGasoleos.columns:
    if col != 'VENDEDOR':
        df_volVendedorGasoleos[col] = df_volVendedorGasoleos[col].astype(str).replace('nan', '0').astype(float)
        
df_volVendedorGasoleos['Desvio Mix Intermensual %']=((df_volVendedorGasoleos['MIX Gasoleos'])/df_volVendedorGasoleos['MIX Gasoleos Intermensual'])-1
df_volVendedorGasoleos['Desvio Mix Interanual %']=((df_volVendedorGasoleos['MIX Gasoleos'])/df_volVendedorGasoleos['MIX Gasoleos Interanual'])-1 

df_volVendedorGasoleos['Desvio Volumen Intermensual %']=((df_volVendedorGasoleos['Volumen Gasoleos'])/df_volVendedorGasoleos['Volumen Gasoleos Intermensual'])-1
df_volVendedorGasoleos['Desvio Volumen Interanual %']= ((df_volVendedorGasoleos['Volumen Gasoleos'])/df_volVendedorGasoleos['Volumen Gasoleos Interanual'])-1


for col in df_volVendedorGasoleos.columns:
    if col != 'VENDEDOR':
        df_volVendedorGasoleos[col] = df_volVendedorGasoleos[col].astype(str).replace('nan', '0').astype(float)
        df_volVendedorGasoleos[col] = df_volVendedorGasoleos[col].astype(str).replace('inf', '1').astype(float)

df_volVendedorGasoleos= df_volVendedorGasoleos.loc[df_volVendedorGasoleos["Volumen Gasoleos"] > 1,:]
df_volVendedorGasoleos=df_volVendedorGasoleos.sort_values('Volumen Gasoleos', ascending=False)

def _objetivo(Vendedor):
    if Vendedor in ["CADILE ALEJANDRO"]:
        return 0.31
    elif Vendedor in ["RICARDO GAVIO"]:
        return 0.25
    elif Vendedor in ["GONZALEZ GERARD0"]:
        return 0.31
    elif Vendedor in ["EDUARDO VILLAR"]:
        return 0.31
    else:
        return 0.3

### Creo una columna con el nombre de la bandera
df_volVendedorGasoleos["Objetivo"] = df_volVendedorGasoleos.apply(
    lambda row: _objetivo(row["VENDEDOR"])
        , axis= 1
)
df_volVendedorGasoleos['Desvio Objetivo']=(df_volVendedorGasoleos['MIX Gasoleos']-df_volVendedorGasoleos['Objetivo'])/df_volVendedorGasoleos['Objetivo']
df_volVendedorGasoleos= df_volVendedorGasoleos.loc[(df_volVendedorGasoleos["VENDEDOR"] != 'OTROS'),:]

######### Totales GAsoleos
df_volVendedorGasoleos.loc["colTOTAL"]= pd.Series(
    df_volVendedorGasoleos.sum()
    , index=['Infinia Diesel Interanual','Infinia Diesel Mes Anterior','Infinia Diesel','Ultra Diesel Interanual','Ultra Diesel Mes Anterior','Ultra Diesel'
            ,'Volumen Gasoleos Intermensual','Volumen Gasoleos'
            ,'Volumen Gasoleos Interanual']
)
df_volVendedorGasoleos.fillna({"VENDEDOR":"TOTAL"}, inplace=True)
df_volVendedorGasoleos.fillna({"Objetivo":0.31}, inplace=True)
# Totales Mix

tasa = (df_volVendedorGasoleos.loc["colTOTAL",'Infinia Diesel']/diasdelmes*num_days)/((df_volVendedorGasoleos.loc["colTOTAL",'Infinia Diesel']/diasdelmes*num_days)+(df_volVendedorGasoleos.loc["colTOTAL",'Ultra Diesel']/diasdelmes*num_days))
df_volVendedorGasoleos.fillna({'MIX Gasoleos':tasa}, inplace=True)

tasa2 = (df_volVendedorGasoleos.loc["colTOTAL",'Infinia Diesel Mes Anterior'] /(
    df_volVendedorGasoleos.loc["colTOTAL",'Infinia Diesel Mes Anterior']+df_volVendedorGasoleos.loc["colTOTAL",'Ultra Diesel Mes Anterior']))
df_volVendedorGasoleos.fillna({'MIX Gasoleos Intermensual':tasa2}, inplace=True)       
        
tasa3 = (df_volVendedorGasoleos.loc["colTOTAL",'Infinia Diesel Interanual'] /(
    df_volVendedorGasoleos.loc["colTOTAL",'Infinia Diesel Interanual']+df_volVendedorGasoleos.loc["colTOTAL",'Ultra Diesel Interanual']))
df_volVendedorGasoleos.fillna({'MIX Gasoleos Interanual':tasa2}, inplace=True)    
#Totales Desvios
tasa4 = ((df_volVendedorGasoleos.loc["colTOTAL",'MIX Gasoleos']) /
    df_volVendedorGasoleos.loc["colTOTAL",'MIX Gasoleos Intermensual'])-1
df_volVendedorGasoleos.fillna({'Desvio Mix Intermensual %':tasa4}, inplace=True)       
        
tasa5 = ((df_volVendedorGasoleos.loc["colTOTAL",'MIX Gasoleos']) /
    df_volVendedorGasoleos.loc["colTOTAL",'MIX Gasoleos Interanual'])-1
df_volVendedorGasoleos.fillna({'Desvio Mix Interanual %':tasa5}, inplace=True) 


tasa6 = ((df_volVendedorGasoleos.loc["colTOTAL",'Volumen Gasoleos']) /
    df_volVendedorGasoleos.loc["colTOTAL",'Volumen Gasoleos Intermensual'])-1
df_volVendedorGasoleos.fillna({'Desvio Volumen Intermensual %':tasa6}, inplace=True)       
        
tasa7 = ((df_volVendedorGasoleos.loc["colTOTAL",'Volumen Gasoleos']) /
    df_volVendedorGasoleos.loc["colTOTAL",'Volumen Gasoleos Interanual'])-1
df_volVendedorGasoleos.fillna({'Desvio Volumen Interanual %':tasa7}, inplace=True) 

tasa8 = ((df_volVendedorGasoleos.loc["colTOTAL",'MIX Gasoleos']) /
    df_volVendedorGasoleos.loc["colTOTAL",'Objetivo'])-1
df_volVendedorGasoleos.fillna({'Desvio Objetivo':tasa8}, inplace=True) 


df_volVendedorGasoleos=df_volVendedorGasoleos.reindex(columns=['VENDEDOR','Volumen Gasoleos','MIX Gasoleos','Objetivo','Desvio Objetivo'
                                            ,'Desvio Volumen Intermensual %','Desvio Mix Intermensual %'
                                            ,'Desvio Volumen Interanual %','Desvio Mix Interanual %'])        

df_volVendedorGasoleos=df_volVendedorGasoleos.rename({'Volumen Gasoleos':'Volumen Gasoleos Proyectado Mensual'
                                                      ,'MIX Gasoleos':'MIX Gasoleos Proyectado Mensual'
                                                     ,'MIX Gasoleos Intermensual':'MIX Gasoleos Mes Anterior'
                                                     ,'Volumen Gasoleos Intermensual':'Volumen Gasoleos Mes Anterior'
                                                     ,'MIX Gasoleos Interanual':'MIX Gasoleos Año Anterior'
                                                     ,'Volumen Gasoleos Interanual':'Volumen Gasoleos Año Anterior'
                                                     ,'Desvio Volumen Interanual %': 'Evolucion Volumen Interanual %'
                                                     ,'Desvio Volumen Intermensual %':'Evolucion Volumen Intermensual %'
                                                     ,'Desvio Mix Intermensual %':'Evolucion Mix Intermensual %'
                                                     ,'Desvio Mix Interanual %':'Evolucion Mix Interanual %'
                                                     
                                                     
                                                     },axis=1)



######### LE DOY FORMATO AL DATAFRAME
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
        .format("{:,.2%}", subset=list_Col_Num) \
        .format("{:,.1f} L", subset=list_Col_Perc) \
        .hide_index() \
        .set_caption(
            titulo
            + "<br>"
            + ((pd.to_datetime("today")-pd.to_timedelta(1,"days"))
            .strftime("%d/%m/%y"))
            + "<br>") \
        .applymap(lambda x: 'color: blue' if x >= 0 else 'color: #FF0000', subset=pd.IndexSlice[:, ['Evolucion Mix Intermensual %']]) \
        .applymap(lambda x: 'color: blue' if x >= 0 else 'color: #FF0000', subset=pd.IndexSlice[:, ['Evolucion Mix Interanual %']]) \
        .applymap(lambda x: 'color: blue' if x >= 0 else 'color: #FF0000', subset=pd.IndexSlice[:, ['Evolucion Volumen Intermensual %']]) \
        .applymap(lambda x: 'color: blue' if x >= 0 else 'color: #FF0000', subset=pd.IndexSlice[:, ['Evolucion Volumen Interanual %']]) \
        .applymap(lambda x: 'color: blue' if x >= 0 else 'color: #FF0000', subset=pd.IndexSlice[:, ['Desvio Objetivo']]) \
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
  
    return resultado

columN = ['Volumen Naftas Proyectado Mensual']

colnumN=['MIX Naftas Proyectado Mensual','Evolucion Mix Intermensual %'
         ,'Evolucion Mix Interanual %'
       ,'Evolucion Volumen Intermensual %','Evolucion Volumen Interanual %','Objetivo','Desvio Objetivo']

columG = ['Volumen Gasoleos Proyectado Mensual']

colnumG=['MIX Gasoleos Proyectado Mensual','Evolucion Mix Intermensual %'
         ,'Evolucion Mix Interanual %'
       ,'Evolucion Volumen Intermensual %','Evolucion Volumen Interanual %','Objetivo','Desvio Objetivo']

df_volVendedorNaftas = _estiladorVtaTituloD(df_volVendedorNaftas,colnumN, columN, "Volumen Vendido por Vendedor Naftas")

df_volVendedorGasoleos = _estiladorVtaTituloD(df_volVendedorGasoleos,colnumG, columG, "Volumen Vendido por Vendedor Gasoleos")


###### DEFINO NOMBRE Y UBICACION DE DONDE SE VA A GUARDAR LA IMAGEN

ubicacion = "C:/Informes/InfoVtaCombustibles/"
nombrepngGO = "Mix_Ventas_Vendedor_Gasoleos.png"
nombrepngNS = "Mix_Ventas_Vendedor_Naftas.png"
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

df_to_image(df_volVendedorGasoleos, ubicacion, nombrepngGO)
df_to_image(df_volVendedorNaftas, ubicacion, nombrepngNS)

#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)