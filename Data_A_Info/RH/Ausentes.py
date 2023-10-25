import os
import mysql.connector
import pandas as pd
import dataframe_image as dfi
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

##### Conexion a MySQL Database Recursos Humanos
serverrh = "13.77.120.129"
portrhh='3306'
databaserh = "rumaos"
usernamerh = "mmagistretti" 
passwordrh = "R3dmer0s#r"

loginMySQLRH = [usernamerh,passwordrh,serverrh,databaserh,portrhh]
db_conex = mysql.connector.connect(
        user=loginMySQLRH[0]
        ,password=loginMySQLRH[1]
        ,host=loginMySQLRH[2]
        ,database=loginMySQLRH[3]
        ,port= loginMySQLRH[4]
)


'''
######### LECTURA DEL EXCEL COSTO DE GNC
ubicacion = str(pathlib.Path(__file__).parent)+"\\"
aux_semanal = "CostoGNC.xlsx"
costoGNC =pd.read_excel(ubicacion+aux_semanal,sheet_name='CostoM3')
costoGNC = costoGNC.convert_dtypes()
'''



################################################
#########   Ausentes De Ayer
################################################
try:
    df_ausentesAyer = pd.read_sql('''

        select distinct(a.id),e.nombre,e.apellido,e.legajo,a.fecha,a.tipo
        from ausentes as a join empleados as e on  a.empleado_id = e.id 
        join turnos as t on e.id = t.empleado_id
        where a.fecha = current_date()-1
        and e.nombre not like 'VANINA JACQUELINE BELEN'
        and e.apellido not like 'VEGA FERREYRA'
        and (a.tipo = 'ART' or a.tipo ='Sin Justificacion'or a.tipo = 'Suspendido'or a.tipo = 'Justificacion Medica' 
    or a.tipo = 'Paternidad' or a.tipo ='Ausente Justificado')

    ''',db_conex)
    df_ausentesAyer = df_ausentesAyer.convert_dtypes()

    ################################################
    #########   Ausentes Del ultimo Año
    ################################################

    ## Creo listas de nombre y apellido
    listaAusentesNombre=[]
    listaAusentesApellido=[]
    ## Voy llenando esas listas con los nombres y apellidos de los ausentes de ayer
    for i in df_ausentesAyer.index:
        listaAusentesNombre.append(df_ausentesAyer.loc[i,'nombre'])
        listaAusentesApellido.append(df_ausentesAyer.loc[i,'apellido'])

    ## Le saco los corchetes a las listas para poder usarlas dentro de la consulta sql
    nombre = str(listaAusentesNombre)[1:-1]
    apellido = str(listaAusentesApellido)[1:-1]
    ##########################
    ### AUSENTES EN EL AÑO ###
    ##########################
    df_ausentesAño = pd.read_sql(f'''

    select count(distinct(a.id)) as 'Ausentes en el Año',e.nombre,e.apellido
    from ausentes as a join empleados as e on  a.empleado_id = e.id 
    join turnos as t on e.id = t.empleado_id
    where a.fecha >= DATE_ADD(DATE_ADD(LAST_DAY(NOW()), INTERVAL 1 DAY),INTERVAL -1 year)
    AND e.nombre in ({nombre})
    AND e.apellido in ({apellido})
    and (a.tipo = 'ART' or a.tipo ='Sin Justificacion'or a.tipo = 'Suspendido'or a.tipo = 'Justificacion Medica' 
    or a.tipo = 'Paternidad' or a.tipo ='Ausente Justificado')
    group by e.nombre,e.apellido


    ''',db_conex)
    df_ausentesAño = df_ausentesAño.convert_dtypes()

    #######################################
    ### AUSENTES JUSTIFICADOS EN EL AÑO ###
    #######################################

    df_ausentesInjusAño = pd.read_sql(f'''

    select count(distinct(a.id)) as 'Ausentes Injustificados',e.nombre,e.apellido
    from ausentes as a join empleados as e on  a.empleado_id = e.id 
    join turnos as t on e.id = t.empleado_id
    where a.fecha >= DATE_ADD(DATE_ADD(LAST_DAY(NOW()), INTERVAL 1 DAY),INTERVAL -1 year)
    AND e.nombre in ({nombre})
    AND e.apellido in ({apellido})
    and a.tipo ='Sin Justificacion'
    group by e.nombre,e.apellido


    ''',db_conex)
    df_ausentesInjusAño = df_ausentesInjusAño.convert_dtypes()

    if 'Ausentes Injustificados' in df_ausentesInjusAño:
        x=1
    else:
        df_ausentesInjusAño['Ausentes Injustificados']= 0

    ##############################
    ### SUSPENCIONES EN EL AÑO ###
    ##############################

    df_suspencionesAño = pd.read_sql(f'''

    select count(distinct(a.id)) as 'Suspensiones',e.nombre,e.apellido
    from ausentes as a join empleados as e on  a.empleado_id = e.id 
    join turnos as t on e.id = t.empleado_id
    where a.fecha >= DATE_ADD(DATE_ADD(LAST_DAY(NOW()), INTERVAL 1 DAY),INTERVAL -1 year)
    AND e.nombre in ({nombre})
    AND e.apellido in ({apellido})
    and a.tipo = 'Suspendido'
    group by e.nombre,e.apellido


    ''',db_conex)
    df_suspencionesAño = df_suspencionesAño.convert_dtypes()

    if 'Suspensiones' in df_suspencionesAño:
        x=1
    else:
        df_suspencionesAño['Suspensiones']= 0


    ## Concateno Tablas
    df_ausentesAyer = df_ausentesAyer.merge(df_ausentesAño,on=['nombre','apellido'],how='outer')
    df_ausentesAyer = df_ausentesAyer.merge(df_ausentesInjusAño,on=['nombre','apellido'],how='outer')
    df_ausentesAyer = df_ausentesAyer.merge(df_suspencionesAño,on=['nombre','apellido'],how='outer')
    ## elimino Columnas Que no entren en el informe
    df_ausentesAyer = df_ausentesAyer.loc[:,df_ausentesAyer.columns!="legajo"]
    df_ausentesAyer = df_ausentesAyer.loc[:,df_ausentesAyer.columns!="fecha"]
    df_ausentesAyer = df_ausentesAyer.loc[:,df_ausentesAyer.columns!="id"]

    ################################################
    #########   ranking ausentes totales     ############
    ################################################

    df_totalausentesAño = pd.read_sql('''


        select count(distinct(a.id)) as 'ausentes del año',e.nombre,e.apellido
        from ausentes as a join empleados as e on  a.empleado_id = e.id 
        join turnos as t on e.id = t.empleado_id
        where a.fecha >= DATE_ADD(DATE_ADD(LAST_DAY(NOW()), INTERVAL 1 DAY),INTERVAL -1 year)
        and e.nombre not like 'VANINA JACQUELINE BELEN'
        and e.apellido not like 'VEGA FERREYRA'
        and e.fecha_de_baja < '2008-01-01'
        and (a.tipo = 'ART' or a.tipo ='Sin Justificacion'or a.tipo = 'Suspendido'or a.tipo = 'Justificacion Medica' 
        or a.tipo = 'Paternidad' or a.tipo ='Ausente Justificado')
        group by e.nombre,e.apellido

    ''',db_conex)

    df_totalausentesAño = df_totalausentesAño.convert_dtypes()
    df_totalausentesAño = df_totalausentesAño.sort_values('ausentes del año',ascending=False)
    df_totalausentesAño = df_totalausentesAño.reset_index()
    df_totalausentesAño = df_totalausentesAño.reset_index()
    df_totalausentesAño = df_totalausentesAño.loc[:,df_totalausentesAño.columns!="index"]

    for i in df_totalausentesAño.index:
        for h in df_totalausentesAño.index:
            if df_totalausentesAño.loc[i,'ausentes del año'] == df_totalausentesAño.loc[h,'ausentes del año']:
                df_totalausentesAño.loc[h,'level_0'] = df_totalausentesAño.loc[i,'level_0']

    h=1

    for i in df_totalausentesAño.index:
        if i >= df_totalausentesAño.loc[i,'level_0']:
            if df_totalausentesAño.loc[i,'level_0'] != h:
                g=df_totalausentesAño.loc[i,'level_0']
                h=i+1
                df_totalausentesAño=df_totalausentesAño.replace({g:h})

    for i in df_ausentesAyer.index:
        for n in df_totalausentesAño.index:
            if df_totalausentesAño.loc[n,'nombre'] == df_ausentesAyer.loc[i,'nombre']:
                df_ausentesAyer.loc[i,'Ranking Total Ausencias'] = df_totalausentesAño.loc[n,'level_0']





    #ausentestotales = df_totalausentesAño.loc['ausentes del año'].sum
    #promediototales= ausentestotales/310

    ################################################
    ####### RANKING INJUSTIFICADOS DEL AÑO  ########
    ################################################

    df_injusausentesAño = pd.read_sql('''


        select count(distinct(a.id)) as 'injustificados',e.nombre,e.apellido
        from ausentes as a join empleados as e on  a.empleado_id = e.id 
        join turnos as t on e.id = t.empleado_id
        where a.fecha >= DATE_ADD(DATE_ADD(LAST_DAY(NOW()), INTERVAL 1 DAY),INTERVAL -1 year)
        and e.nombre not like 'VANINA JACQUELINE BELEN'
        and e.apellido not like 'VEGA FERREYRA'
        and a.tipo ='Sin Justificacion'
        and e.fecha_de_baja < '2008-01-01'
        group by e.nombre,e.apellido
        order by 'ausentes del año'

    ''',db_conex)

    df_injusausentesAño = df_injusausentesAño.convert_dtypes()
    df_injusausentesAño = df_injusausentesAño.sort_values('injustificados',ascending=False)
    df_injusausentesAño = df_injusausentesAño.reset_index()
    df_injusausentesAño = df_injusausentesAño.reset_index()
    df_injusausentesAño = df_injusausentesAño.loc[:,df_injusausentesAño.columns!="index"]

    for i in df_injusausentesAño.index:
        for h in df_injusausentesAño.index:
            if df_injusausentesAño.loc[i,'injustificados'] == df_injusausentesAño.loc[h,'injustificados']:
                df_injusausentesAño.loc[h,'level_0'] = df_injusausentesAño.loc[i,'level_0']
    h=1

    for i in df_injusausentesAño.index:
        if i >= df_injusausentesAño.loc[i,'level_0']:
            if df_injusausentesAño.loc[i,'level_0'] != h:
                g=df_injusausentesAño.loc[i,'level_0']
                h=i+1
                df_injusausentesAño=df_injusausentesAño.replace({g:h})



    for i in df_ausentesAyer.index:
        for n in df_injusausentesAño.index:
            if df_injusausentesAño.loc[n,'nombre'] == df_ausentesAyer.loc[i,'nombre']:
                df_ausentesAyer.loc[i,'Ranking Ausencias Injustificadas'] = df_injusausentesAño.loc[n,'level_0']




    ## Renombro columnas
    df_ausentesAyer = df_ausentesAyer.rename({'nombre':'Nombre','apellido':'Apellido','tipo':'Tipo'},axis=1)
    ## Completo con 0 donde no hayan valores
    df_ausentesAyer = df_ausentesAyer.fillna(0)
    df_ausentesAyer=df_ausentesAyer.reindex(columns=['Apellido','Nombre','Tipo','Ausentes en el Año','Ausentes Injustificados','Suspensiones','Ranking Total Ausencias','Ranking Ausencias Injustificadas'])

    df_totalausentesAño = df_injusausentesAño.head(30)
    df_injusausentesAño = df_injusausentesAño.loc[:,df_injusausentesAño.columns!="index"]

    def _estiladorImagen(df, list_Col_Num,list_Col_Vol,columnas,titulo):

        def assd(columnas):
            
            
            return ['background-color: red' if i <=10  else 'background-color: green light' for i in columnas]
        def asd(columnas):

        
            return ['background-color: green' if i == 0  else 'background-color: green light' for i in columnas]
        resultado = df.style \
            .format("{0:,.0f}", subset=list_Col_Num+columnas) \
            .hide_index() \
            .set_caption(
                titulo
                + "<br>"
                + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
                + "<br>") \
            .set_properties(subset= list_Col_Num + list_Col_Vol + columnas
                , **{"text-align": "center", "width": "150px"}) \
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
            .apply(assd, subset=columnas, axis=0)\
            .apply(asd, subset=columnas, axis=0)
        return resultado

    #  columnas $
    numCols = [ 'Ausentes en el Año'
            ,'Suspensiones'
            ,'Ausentes Injustificados'
            ]

    #Col Volumen

    colcaract = ['Nombre'
            ,'Apellido'
            ,'Tipo'
    ]

    # Columnas Porcentaje
    percColsPen = [
    ]
    # Columnas Ranking
    colum = ['Ranking Total Ausencias'
    ,'Ranking Ausencias Injustificadas']
    ### APLICO EL FORMATO A LA TABLA
    df_ausentesAyer = _estiladorImagen(df_ausentesAyer,numCols,colcaract,colum, "Ausentes")

    #### DEFINO EL DESTINO DONDE SE GUARDARA LA IMAGEN Y EL NOMBRE
    ubicacion = "C:/Informes/RH/"
    nombrePen = "Ausentes.png"
    nombrePenDiario = "ddddd.png"


    ### IMPRIMO LA IMAGEN 

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

    df_to_image(df_ausentesAyer, ubicacion, nombrePen)
    #df_to_image(df_totalausentesAño,ubicacion,nombrePenDiario)
except:
    
    def _estiladorImagen(df, list_Col_Num,list_Col_Vol,columnas,titulo):

        resultado = df.style \
            .format("{0:,.0f}", subset=list_Col_Num+columnas) \
            .hide_index() \
            .set_caption(
                titulo
                + "<br>"
                + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
                + "<br>") \
            .set_properties(subset= list_Col_Num + list_Col_Vol + columnas
                , **{"text-align": "center", "width": "150px"}) \
            .set_properties(border= "2px solid black") \
            
        return resultado

    ubicacion = "C:/Informes/RH/"
    nombrePen = "Ausentes.png"
    nombrePenDiario = "ddddd.png"

    noAusentes= pd.DataFrame()
    noAusentes['No Hubo Ausentes Ayer']=""
    numCols=[]
    colcaract=[]
    colum=[]
    noAusentes = _estiladorImagen(noAusentes,numCols,colcaract,colum, "Ausentes")
    ### IMPRIMO LA IMAGEN 

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

    df_to_image(noAusentes, ubicacion, nombrePen)

#########
# Timer #
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)

