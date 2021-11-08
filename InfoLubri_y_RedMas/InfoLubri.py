###################################
#
#     INFORME Lubricantes Ayer
#             
#               05/11/21
###################################

import os
import sys
import pathlib

# Allow imports from the top folder
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))

import pandas as pd
import dataframe_image as dfi

from DatosLogin import login
from Conectores import conectorMSSQL

conexMSSQL = conectorMSSQL(login)

df_lubri = pd.read_sql(
    '''
        SELECT
            [CODPRODUCTO] as 'CODIGO'
            ,CAST(sum(-[CANTIDAD]) AS smallint) as 'UNIDADES'
        FROM [Rumaos].[dbo].[VMovDet]
        WHERE VMovDet.TIPOMOVIM = '3'
            AND VMovDet.CODPRODUCTO BETWEEN 100 AND 5000
            --Filtramos azul32 y agua destilada
            AND VMovDet.CODPRODUCTO NOT IN ('1027','1146', '1175', '2000')
            AND VMovDet.FECHASQL = DATEADD(DAY,-1,CAST(GETDATE() AS date))
        GROUP BY VMovDet.CODPRODUCTO
    '''
    , conexMSSQL)
df_lubri = df_lubri.convert_dtypes()
df_lubri["CODIGO"] = df_lubri["CODIGO"].str.strip()


ubicacion = str(pathlib.Path(__file__).parent)+"\\"
aux_lubri = "Auxiliar_Objetivos_y_Envases.xlsx"

df_lts_x_envase = pd.read_excel(
    ubicacion + aux_lubri
    , "Lts_x_Envase"
    , dtype={"CODIGO": str}
)
df_lts_x_envase.convert_dtypes()
df_lts_x_envase["CODIGO"] = df_lts_x_envase["CODIGO"].str.strip()


df_lubri_lts = pd.merge(df_lubri, df_lts_x_envase, how="left")
df_lubri_lts = df_lubri_lts[df_lubri_lts["Lts_x_Envase"] >= 1]
df_lubri_lts["LITROS"] = df_lubri_lts["UNIDADES"] * df_lubri_lts["Lts_x_Envase"]
df_lubri_lts = df_lubri_lts[["Lts_x_Envase","LITROS"]]
df_lubri_lts = df_lubri_lts.groupby(by=["Lts_x_Envase"], as_index=False).sum()
df_lubri_lts = df_lubri_lts.astype({"Lts_x_Envase": "int", "LITROS": "int"})
df_lubri_lts = df_lubri_lts.rename(columns={"Lts_x_Envase": "ENVASE (lts)"})


df_objetivos = pd.read_excel(
    ubicacion + aux_lubri
    , "Objetivos_x_Envase"
    , dtype={"CODIGO": str}
)


df_lubri_lts_obj = pd.merge(df_lubri_lts, df_objetivos)
df_lubri_lts_obj["CUMPLIMIENTO"] = \
    (df_lubri_lts_obj["LITROS"] / df_lubri_lts_obj["OBJETIVO (lts)"])

print(df_lubri_lts_obj)