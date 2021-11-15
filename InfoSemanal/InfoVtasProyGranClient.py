###################################
#
#     INFORME Ventas Proyectadas Grandes Clientes
#             (Solo Consumos en Baja)
#       15/11/21 - 
###################################

import os
import sys
import pathlib

# Allow imports from the top folder
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))

import pandas as pd
import dataframe_image as dfi
from PIL import Image

from DatosLogin import login
from Conectores import conectorMSSQL



def pre_VtaProyGClient(conexMSSQL):
    '''
    Will calculate and return 

    conexMSSQL: conection to SQL Server
    '''
    ##########################################
    # Aux tables with filters
    ##########################################

    ubicacion = str(pathlib.Path(__file__).parent)+"\\"
    aux_semanal = "Auxiliar_Info_Semanal.xlsx"

    df_codprod_ok = pd.read_excel(
        ubicacion + aux_semanal
        , "cod_prod"
    )
    df_nrocli_omit = pd.read_excel(
        ubicacion + aux_semanal
        , "clientes_omitidos"
    )
    df_codprod_ok = df_codprod_ok.convert_dtypes()



    ##########################################
    # Get "remitos" of previous month
    ##########################################

    df_vta_ctas_m_ant = pd.read_sql(
        """
        DECLARE @inicioMesActual DATETIME
        SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, CURRENT_TIMESTAMP), 0)

        DECLARE @inicioMesAnterior DATETIME
        SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

        --Divide por la cant de días del mes anterior y multiplica por la cant de días del
        --mes actual
        DECLARE @pondMesAnt FLOAT
        SET @pondMesAnt = CAST(DAY(EOMONTH(CURRENT_TIMESTAMP)) AS float) /
            CAST(DAY(EOMONTH(@inicioMesAnterior)) AS float)

        SELECT 
            RTRIM(FRD.[CODPRODUCTO]) AS 'CODPRODUCTO'
            ,FRD.[NROCLIENTE]
            ,RTRIM(FC.NOMBRE) AS 'NOMBRE'
            ,sum((FRD.[CANTIDAD] * @pondMesAnt)) AS 'Mes Anterior'
        FROM [Rumaos].[dbo].[FacRemDet] as FRD
        JOIN Rumaos.dbo.FacCli as FC
            on FRD.NROCLIENTE = FC.NROCLIPRO
        WHERE FRD.FECHASQL >= @inicioMesAnterior
            AND FRD.FECHASQL < @inicioMesActual
            AND FRD.NROCLIENTE >= '100000'
            AND FRD.CANTIDAD  > '0'
            AND FRD.CODPRODUCTO NOT IN (
                'GNC'
                ,'GNC01'
                ,'GNC02'
                ,'GNC03'
                ,'GNCA'
                ,'GNCCA'
                ,'GNCCC'
                ,'GNCCO'
                ,'GNCRM'
                ,'GNCVA'
                ,'GNNC1'
            )
            GROUP BY FRD.NROCLIENTE, FC.NOMBRE, FRD.CODPRODUCTO
            ORDER BY NROCLIENTE, CODPRODUCTO
        """
        ,conexMSSQL)

    df_vta_ctas_m_ant = df_vta_ctas_m_ant.convert_dtypes()

    ##########################################
    # Get "remitos" of current month
    ##########################################

    df_vta_ctas_m_act = pd.read_sql(
        """
        DECLARE @inicioMesActual DATETIME
        SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, CURRENT_TIMESTAMP), 0)

        DECLARE @hoy DATETIME
        SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

        --Divide por la cantidad de días cursados del mes actual y multiplica por la cant
        --de días del mes actual
        DECLARE @pondMesAct FLOAT
        SET @pondMesAct = CAST(DAY(EOMONTH(CURRENT_TIMESTAMP)) AS float) /
            (CAST(DAY(CURRENT_TIMESTAMP) AS float)-1)

        SELECT 
            RTRIM(FRD.[CODPRODUCTO]) AS 'CODPRODUCTO'
            ,FRD.[NROCLIENTE]
            ,RTRIM(FC.NOMBRE) AS 'NOMBRE'
            ,sum((FRD.[CANTIDAD] * @pondMesAct)) AS 'Mes Actual'
        FROM [Rumaos].[dbo].[FacRemDet] as FRD
        JOIN Rumaos.dbo.FacCli as FC
            on FRD.NROCLIENTE = FC.NROCLIPRO
        WHERE FRD.FECHASQL >= @inicioMesActual
            AND FRD.FECHASQL < @hoy
            AND FRD.NROCLIENTE >= '100000'
            AND FRD.CANTIDAD > 0
            AND FRD.CODPRODUCTO NOT IN (
                'GNC'
                ,'GNC01'
                ,'GNC02'
                ,'GNC03'
                ,'GNCA'
                ,'GNCCA'
                ,'GNCCC'
                ,'GNCCO'
                ,'GNCRM'
                ,'GNCVA'
                ,'GNNC1'
            )
            GROUP BY FRD.NROCLIENTE, FC.NOMBRE, FRD.CODPRODUCTO
            ORDER BY NROCLIENTE, CODPRODUCTO
        """
        ,conexMSSQL)

    df_vta_ctas_m_act = df_vta_ctas_m_act.convert_dtypes()


    ##########################################
    # Function to refine dataframes
    ##########################################

    def _filtrador(df):
        '''
        -Parameters-
        df: Dataframe to be filtered
                
        -Returns-
        Dataframe
        '''
        # Inner merge to select only valid "CODPRODUCTO" items and dropping
        # column "CODPRODUCTO"
        df = pd.merge(
            df
            , df_codprod_ok
            , on="CODPRODUCTO"
            , how="inner"
        ).drop(columns="CODPRODUCTO")

        # Left merge with indicator to be able to filter "NROCLIENTE"
        # on both tables and dropping column "NROCLIENTE"
        df = pd.merge(
            df
            , df_nrocli_omit
            , on="NROCLIENTE"
            , how="left"
            , indicator=True
        )

        # Selecting rows "left_only" and dropping column "_merge"
        df = df.query("_merge == 'left_only'").drop(columns="_merge")

                
        return df


    ##########################################
    # Function that allow the filtering of DFs, grouping and calculated
    # columns
    ##########################################

    def _preparador(df_ant, df_act):
        '''
        -Parameters-
        df_ant: Dataframe of previous period
        df_act: Dataframe of current period
                
        -Return-
        Dataframe
        '''
        # Filtering and grouping of df_ant
        df_ant = _filtrador(df_ant)
        df_ant = df_ant.groupby(["NROCLIENTE", "NOMBRE", "PRODUCTO"]
            , as_index=False).sum()
        # Filtering and grouping of df_act
        df_act = _filtrador(df_act)
        df_act = df_act.groupby(["NROCLIENTE", "NOMBRE", "PRODUCTO"]
            , as_index=False).sum()
        # merge df_ant and df_act
        df_merge = pd.merge(
            df_ant
            , df_act
            , how="left"
            , on=["NROCLIENTE", "NOMBRE", "PRODUCTO"]
        )

        # Filter rows with "Mes Anterior" < 1000
        df_merge = df_merge[df_merge["Mes Anterior"] >= 1000]
        # Fill NaNs with 0
        df_merge.fillna({"Mes Actual":0}, inplace=True)
        # Column "Intermensual %"
        df_merge["Intermensual %"] = \
            (df_merge["Mes Actual"] / df_merge["Mes Anterior"] - 1)
        # Column "Intermensual Volumen"
        df_merge["Intermensual Volumen"] = \
            (df_merge["Mes Actual"] - df_merge["Mes Anterior"])
        # Filter rows with "Intermensual Volumen" > 0
        df_merge = df_merge[df_merge["Intermensual Volumen"] < 0]
        
        return df_merge

    
    ##########################################
    # Cleaning, merging and sorting dataframes
    ##########################################

    df_mes_vtas_ctas = _preparador(df_vta_ctas_m_ant, df_vta_ctas_m_act)

    df_mes_vtas_ctas = df_mes_vtas_ctas.sort_values(by=[#"Intermensual Volumen"
         "NOMBRE", "PRODUCTO"])

    
    return df_mes_vtas_ctas
    

    
##########################################
# STYLING of the dataframe
##########################################

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
        .format("{0:,.0f}", subset=list_Col_Num) \
        .format("{:,.2%}", subset=list_Col_Perc) \
        .hide_index() \
        .set_caption(
            titulo
            + "<br>"
            + "Semana Actual "
            + ((pd.to_datetime("today")-pd.to_timedelta(8,"days"))
            .strftime("%d/%m/%y"))
            + " al "
            + ((pd.to_datetime("today")-pd.to_timedelta(1,"days"))
            .strftime("%d/%m/%y"))
        ) \
        .set_properties(subset=list_Col_Num + list_Col_Perc
            , **{"text-align": "center", "width": "80px"}) \
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
        # .apply(lambda x: ["background: black" if x.name == df.index[-1] 
        #     else "" for i in x]
        #     , axis=1) \
        # .apply(lambda x: ["color: white" if x.name == df.index[-1]
        #     else "" for i in x]
        #     , axis=1)

    
    return resultado



##########################################
# PRINTING dataframe as an image
##########################################

# This will print the df with a unique name and will erase the old image 
# everytime the script is run

def _df_to_image(df, ubicacion, nombre):
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






if __name__ == "__main__":

    conexMSSQL = conectorMSSQL(login)
    a=pre_VtaProyGClient(conexMSSQL)

    # Numeric Columns
    numCols = ["Mes Anterior", "Mes Actual", "Intermensual Volumen"]
    # Percentage Columns
    percCols = ["Intermensual %"]
    a=_estiladorVtaTitulo(a,numCols,percCols,"Titulito")
    display(a)
    
