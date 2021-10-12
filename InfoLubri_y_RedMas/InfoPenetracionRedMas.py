###################################
#
#     INFORME Penetración RedMás
#             
#               07/10/21
###################################

import os
import sys
import pathlib

from pandas.core.reshape.pivot import pivot_table
# Allow imports from the top folder
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))

import pandas as pd
import dataframe_image as dfi

from DatosLogin import login
from Conectores import conectorMSSQL

#timer
tiempoInicio = pd.to_datetime("today")


conexMSSQL = conectorMSSQL(login)

df_desp_x_turno_liq = pd.read_sql(
    """
        SELECT
            Despapro.UEN,
            Despapro.TURNO,
            COUNT(Despapro.VOLUMEN) AS 'Despachos RedMas',
            (SELECT
                COUNT(D.VOLUMEN) AS 'Despachos'
            FROM Rumaos.dbo.Despapro as D
            WHERE D.uen = Despapro.uen
                AND D.TURNO = Despapro.TURNO
                AND	D.TARJETA NOT LIKE 'cc%'
                AND D.FECHASQL = DATEADD(DAY,-1,CAST(GETDATE() AS date))
                AND D.VOLUMEN > '0'
                AND D.CODPRODUCTO <> 'GNC'
            GROUP BY D.UEN ,D.TURNO
            ) AS 'Despachos'
        FROM Rumaos.dbo.Despapro
        WHERE Despapro.TARJETA like 'i%'
            AND Despapro.FECHASQL = DATEADD(DAY,-1,CAST(GETDATE() AS date))
            AND Despapro.VOLUMEN > '0'
            AND Despapro.CODPRODUCTO <> 'GNC'
        GROUP BY Despapro.UEN,Despapro.TURNO
    """
    ,conexMSSQL)
df_desp_x_turno_liq = df_desp_x_turno_liq.convert_dtypes()
df_desp_x_turno_liq["UEN"] = df_desp_x_turno_liq["UEN"].str.strip()
df_desp_x_turno_liq["TURNO"] = df_desp_x_turno_liq["TURNO"].str.strip()

# pivot_desp_totales_x_turno_liq = pd.pivot_table(df_desp_x_turno_liq
#     , values=["Despachos RedMas","Despachos"]
#     , columns="TURNO"
#     , aggfunc="sum"
# )
# print(pivot_desp_totales_x_turno_liq.head())


#Get "PENETRACIÓN" by "UEN" and "TURNO"
df_desp_x_turno_liq["PENETRACIÓN"] = df_desp_x_turno_liq.apply(
    lambda row: (row["Despachos RedMas"])/(row["Despachos"])
    , axis= 1
)
pivot_desp_x_turno_liq = pd.pivot_table(df_desp_x_turno_liq
    , values="PENETRACIÓN"
    , index="UEN"
    , columns="TURNO"
    , aggfunc="sum"
)
pivot_desp_x_turno_liq.reset_index(inplace=True)
print(pivot_desp_x_turno_liq.head())

# Get "TOTAL" of "PENETRACIÓN" by "UEN"
pivot_desp_liq_total = pd.pivot_table(df_desp_x_turno_liq
    , values=["Despachos RedMas","Despachos"]
    , index="UEN"
    , aggfunc="sum"
)
pivot_desp_liq_total.reset_index(inplace=True)
pivot_desp_liq_total["TOTAL"] = pivot_desp_liq_total.apply(
    lambda row: (row["Despachos RedMas"])/(row["Despachos"])
    , axis= 1
)
pivot_desp_liq_total = pivot_desp_liq_total[["UEN","TOTAL"]]

