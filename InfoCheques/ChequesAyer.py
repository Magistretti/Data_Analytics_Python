###################################
#
#     INFORME Cheques Recibidos Ayer
#             
#        24/11/21 - 24/11/21
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

import logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)



##########################################
# Get "remitos" of previous week
##########################################

def cheques_ayer():
    '''
    This function will create the file "Cheques_UENs.xlsx" with a list of
    cheques received since yesterday or 3 days back from now in case of a monday
    '''

    # Timer
    tiempoInicio = pd.to_datetime("today")

    conexMSSQL = conectorMSSQL(login)

    df_cheques = pd.read_sql(
        """
        SET NOCOUNT ON --Needed for Pandas query due to temp list @lista

        DECLARE @fecha as smalldatetime
        -- Filtering by datetime >= @fecha will go from 8 AM of previous day
        -- to the time of the report
        IF DATENAME(WEEKDAY,GETDATE()) = 'lunes'
            SET @fecha = dateadd(HOUR,8,cast(dateadd(DAY,-3,cast(getdate() as date)) as datetime))
        ELSE
            SET @fecha = dateadd(HOUR,8,cast(dateadd(DAY,-1,cast(getdate() as date)) as datetime))

        SELECT
            RTRIM(CR.[UEN]) AS 'UEN'
            ,CAST(CR.[NRORECIBO] as nvarchar) AS 'NRORECIBO'
            ,CAST(RV.NROCLIENTE as nvarchar) AS 'NROCLIENTE'
            ,RTRIM(FC.NOMBRE) AS 'NOMBRE'
            ,RTRIM(CR.[BANCO]) AS 'BANCO'
            ,CAST(CR.[NROCHEQUE] as nvarchar) AS 'NROCHEQUE'
            ,CR.[IMPORTE]
            ,CAST(CR.[FECHAVTOSQL] as date) AS 'FECHA VENCIMIENTO'
            ,RTRIM(V.NOMBREVEND) AS 'VENDEDOR'
            ,RTRIM(RV.USUARIO) AS 'USUARIO SGES'
            ,CAST(RV.FECHASQL as smalldatetime) AS 'FECHA INGRESO'
        FROM [Rumaos].[dbo].[CCRec02] AS CR
        join Rumaos.dbo.RecVenta AS RV 
            ON CR.UEN = RV.UEN
            AND CR.PTOVTAREC = RV.PTOVTA
            AND CR.NRORECIBO = RV.NRORECIBO
        join Rumaos.dbo.FacCli as FC
            ON RV.NROCLIENTE = FC.NROCLIPRO
        left join Rumaos.dbo.Vendedores as V
            ON FC.NROVEND = V.NROVEND
        where (CR.mediopago = 4 OR CR.nrocheque <> 0)
        and RV.FECHASQL >= @fecha
        order by CR.UEN,RV.FECHASQL
        """
            ,conexMSSQL)
    df_cheques = df_cheques.convert_dtypes()
    # print(df_cheques.info())
    # print(df_cheques.head())

    ubicacion = str(pathlib.Path(__file__).parent)+"\\"
    nombreExcel = "Cheques_UENs.xlsx"

    # Removing .xlsx file if exists to avoid permission error when
    # creating new file
    if os.path.exists(ubicacion + nombreExcel):
        os.remove(ubicacion + nombreExcel)

        writer = pd.ExcelWriter(ubicacion + nombreExcel)
        df_cheques.to_excel(writer, sheet_name="Cheques", index=False, na_rep="")

        # Auto-adjust columns width
        for column in df_cheques:
            column_width = max(df_cheques[column].astype(str).map(len).max()
                , len(column)
            )
            col_idx = df_cheques.columns.get_loc(column)
            writer.sheets["Cheques"].set_column(col_idx, col_idx, column_width)

        writer.save()
        
    else:
        writer = pd.ExcelWriter(ubicacion + nombreExcel)
        df_cheques.to_excel(writer, sheet_name="Cheques", index=False, na_rep="")

        # Auto-adjust columns width
        for column in df_cheques:
            column_width = max(df_cheques[column].astype(str).map(len).max()
                , len(column)
            )
            col_idx = df_cheques.columns.get_loc(column)
            writer.sheets["Cheques"].set_column(col_idx, col_idx, column_width)
            
        writer.save()
    

    
    # Timer
    tiempoFinal = pd.to_datetime("today")
    logger.info(
        "\nInfo Red Control Liq"
        + "\nTiempo de Ejecucion Total: "
        + str(tiempoFinal-tiempoInicio)
    )



if __name__ == "__main__":
    cheques_ayer()