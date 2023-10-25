
from DatosLogin import login
from Conectores import conectorMSSQL
import pandas as pd
import pyodbc #Library to connect to Microsoft SQL Server
from DatosLogin import login #Holds connection data to the SQL Server
import sys
import pathlib
from datetime import datetime
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


    #######################################
################ VENTAS POR COMERCIAL #####
########################################

deuda = pd.read_sql('''
 SELECT
            CAST(FRD.[NROCLIENTE] as VARCHAR) as 'NROCLIENTE'
            ,RTRIM(Cli.NOMBRE) as 'NOMBRE'

           ,CAST(ROUND(MIN(Cli.SALDOPREPAGO - Cli.SALDOREMIPENDFACTU),0) as int) as 'SALDOCUENTA'

            , ISNULL(RTRIM(Vend.NOMBREVEND),'') as 'Vendedor'
			, cli.LIMITECREDITO as 'Acuerdo de Descubierto $'
			, sum(FRD.importe) as 'venta en cta/cte'
            FROM [Rumaos].[dbo].[FacRemDet] as FRD with (NOLOCK)
            INNER JOIN dbo.FacCli as Cli with (NOLOCK)
                ON FRD.NROCLIENTE = Cli.NROCLIPRO
            LEFT OUTER JOIN dbo.Vendedores as Vend with (NOLOCK)
	            ON Cli.NROVEND = Vend.NROVEND

            where FRD.NROCLIENTE > '100000'
                AND (Cli.SALDOPREPAGO - Cli.SALDOREMIPENDFACTU) < -10000
                and Cli.ListaSaldoCC = 1
                and FECHASQL >= '20140101' and FECHASQL <= CAST(GETDATE() as date)
            group by FRD.NROCLIENTE, Cli.NOMBRE, Vend.NOMBREVEND,cli.LIMITECREDITO
            order by Cli.NOMBRE
                               
    ''' ,db_conex)
deuda = deuda.convert_dtypes()
deuda['SALDOCUENTA']=deuda['SALDOCUENTA']*-1
# Obtener una lista de números de clientes sin corchetes
lista_clientes = deuda['NROCLIENTE'].tolist()

# Convertir la lista en una cadena separada por comas
clientes_deudores = ','.join(str(clientes) for clientes in lista_clientes)

ventas = pd.read_sql(f'''

         select sum(r.IMPORTE) as IMPORTE,SUM(r.cantidad) as Cantidad,vend.NOMBREVEND,FECHASQL,CLI.NOMBRE,r.NROCLIENTE from FacRemDet as r
    left JOIN dbo.FacCli as Cli with (NOLOCK)
        ON r.NROCLIENTE = Cli.NROCLIPRO
    LEFT JOIN dbo.Vendedores as Vend with (NOLOCK)
    ON Cli.NROVEND = Vend.NROVEND

    where r.FECHASQL >= '2023-01-01'
    and r.FECHASQL <= DATEADD(day, 0, CAST(GETDATE() AS date))
    and r.NROCLIENTE IN ({clientes_deudores})
	GROUP BY FECHASQL,NROCLIENTE,NOMBREVEND,NOMBRE
	ORDER BY FECHASQL DESC
 ''' ,db_conex)
ventas = ventas.convert_dtypes()
ventas['NROCLIENTE'] = ventas['NROCLIENTE'].astype(int)
ventas['IMPORTE'] = ventas['IMPORTE'].astype(int)
deuda['NROCLIENTE'] = deuda['NROCLIENTE'].astype(int)


# Convertir las columnas de fecha a tipo datetime
ventas['FECHASQL'] = pd.to_datetime(ventas['FECHASQL'])

# Crear un diccionario para almacenar las fechas de inicio de deuda de cada cliente
fechas_inicio_deuda = {}

# Crear una lista para almacenar los resultados como diccionarios
resultados = []

# Iterar sobre cada cliente en la tabla deudas
for i in deuda.index:
    cliente = deuda.loc[i,'NROCLIENTE']
    deuda_actual = deuda.loc[i,'SALDOCUENTA']
    
    # Filtrar las ventas del cliente en cuestión
    ventas_cliente = ventas.loc[ventas['NROCLIENTE'] == cliente,:]
    # Ordenar las ventas por fecha en orden descendente (desde hoy hacia atrás)
    ventas_cliente = ventas_cliente.sort_values(by='FECHASQL', ascending=False)
    
    # Calcular la fecha de inicio de la deuda
    suma_ventas = 0
    suma_cantidad =0
    fecha_inicio = None
    ventas_cliente=ventas_cliente.reset_index()
    for e in ventas_cliente.index:
        if suma_ventas <= deuda_actual:
            suma_ventas += ventas_cliente.loc[e,'IMPORTE']
            suma_cantidad += ventas_cliente.loc[e,'Cantidad']
            fecha_inicio = ventas_cliente.loc[e,'FECHASQL']
            fechas_inicio_deuda[cliente] = fecha_inicio
            cantidad = suma_cantidad
            diferencia = suma_ventas - deuda_actual
            porcentaje_pagado = (diferencia)/ventas_cliente.loc[e,'IMPORTE']
            cantidad_pagada = porcentaje_pagado*ventas_cliente.loc[e,'Cantidad']
            cantidad_adeudada = cantidad - cantidad_pagada
            
    # Calcular la diferencia en días entre la fecha de inicio y la fecha actual
    if fecha_inicio:
        fecha_actual = datetime.now()
        dias_diferencia = (fecha_actual - fecha_inicio).days
    else:
        dias_diferencia = 500
        
    # Almacenar la información en la lista de resultados
    resultados.append({'NROCLIENTE': cliente, 'FechaInicioDeuda': fecha_inicio, 'Dias de Venta Adeudado': dias_diferencia, 'Cantidad Adeudada': cantidad_adeudada})

    
resultados_df = pd.DataFrame(resultados)
resultados_df=resultados_df.fillna('2022-01-01')
dias_Deuda=resultados_df