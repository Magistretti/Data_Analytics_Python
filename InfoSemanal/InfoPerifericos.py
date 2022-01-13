###################################
#
#     INFORME Periféricos
#             
#          13/01/22 
###################################

import os
import sys
import pathlib

# Allow imports from the top folder
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))

import pandas as pd
import dataframe_image as dfi
from PIL import Image

from DatosLogin import login, loginAZMilenium, loginPIMilenium
from Conectores import conectorMSSQL

import logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)


conexCentral = conectorMSSQL(login)
conexAZMil = conectorMSSQL(loginAZMilenium)
conexPIMil = conectorMSSQL(loginPIMilenium)


##########################################################
# Getting "Servicompras" and "FULLs" dataframes
##########################################################

def _get_SCFull(conexCentral, conexAZMil, conexPIMil):
    """
    df of general store sales of actual and previous week
    """

    df_SC = pd.read_sql(
        """
        -- Venta SC Sin Fulls con unidades vendidas
        -- Se va a crear una tabla temporal para la SCProduen con el objetivo de
        -- generar un clustered index que agilice el join entre SCEgreso y SCProduen

        SET NOCOUNT ON --Esto evita el mensaje de confirmación después de cada
            -- ejecución permitiendo a pandas generar el df

        -- Si la tabla temporal existe, la borra
        if OBJECT_ID('tempdb..#ProductosSC') is not null
        DROP TABLE #ProductosSC
        --GO

        -- Seleccionamos solo las columnas a usar de las estaciones con SC
        -- para generar la tabla temporal
        select UEN, CODIGO, AGRUPACION
        into #ProductosSC
        from dbo.scproduen
        where UEN IN (
                'AZCUENAGA'
                ,'LAMADRID'
                ,'MERCADO 2'
                ,'PERDRIEL'
                ,'PERDRIEL2'
                ,'PUENTE OLIVE'
                ,'SAN JOSE'
            )
        ;

        -- Creamos el clustered index de CODIGO-UEN para la tabla temporal
        create clustered index ix_Clustered1
            ON #ProductosSC (CODIGO Asc, UEN Asc);


        DECLARE @ayer date
        set @ayer = GETDATE()-1
        DECLARE @semanaAtras date
        set @semanaAtras = GETDATE()-8
        DECLARE @dosSemanasAtras date
        set @dosSemanasAtras = GETDATE()-15;


        -- Ejecutamos la consulta a la tabla SCEgreso pero hay que recordar restar
        -- las ventas del Grill de Perdriel I que salen a través de la Agrup 'Promos'

        WITH actual as( -- CTE con ventas de la semana actual
            SELECT
                t.UEN
                ,sum(t.[Unid Vendidas]) as 'Unid Vend Sem Actual'
                ,sum(t.[Importe Total]) as 'Importe Total Sem Actual'
            FROM(
                SELECT    
                    RTRIM(SCEg.[UEN]) as 'UEN'
                    ,sum(SCEg.CANTIDAD) as 'Unid Vendidas'
                    ,sum(SCEg.IMPORTE) as 'Importe Total'

                FROM [Rumaos].[dbo].[SCEgreso] as SCEg WITH (NOLOCK)
                -- Join a la tabla temporal
                INNER JOIN #ProductosSC as SCPr WITH (NOLOCK)
                    ON (SCEg.UEN = SCPr.UEN AND SCEg.CODIGO = SCPr.CODIGO)

                where FECHASQL > @semanaAtras
                    AND FECHASQL <= @ayer
                    AND SCPr.AGRUPACION NOT IN (
                        'FILTROS'
                        ,'PREMIOS RED MAS DIFERIDOS'
                        ,'REGALOS O CORTESIAS'
                        ,'VENDING2'
                    )
                    AND SCPr.CODIGO NOT IN (
                        'MED'
                        ,'TOR'
                        ,'DOCML'
                        ,'DOCTO'
                        ,'MEDML'
                        ,'MEDTO'
                        ,'MEDMR'
                        ,'DOCMR'
                        ,'MEDRE'
                        ,'DOCMIX'
                        ,'MEDMIX'
                        ,'MIX'
                    )
                GROUP BY SCEg.UEN

                UNION ALL

                (SELECT
                    RTRIM(SCEg.[UEN]) as 'UEN'
                    ,-sum([CANTIDAD]) as 'unid vend'
                    ,-sum([IMPORTE]) as 'Importe Total'
                FROM [Rumaos].[dbo].[SCEgreso] as SCEg WITH (NOLOCK)
                INNER JOIN #ProductosSC as SCPr WITH (NOLOCK)
                    ON (SCEg.UEN = SCPr.UEN AND SCEg.CODIGO = SCPr.CODIGO)
                where SCEg.CODIGO not in (
                    'MED'
                    ,'TOR'
                    ,'DOCML'
                    ,'DOCTO'
                    ,'MEDML'
                    ,'MEDTO'
                    ,'DOCMIX'
                    ,'DOCMR'
                    ,'MEDMIX'
                    ,'MEDMR'
                    ,'MEDRE'
                    ,'MIX'
                )
                    AND SCPr.AGRUPACION = 'PROMOS'
                    AND SCEg.UEN = 'PERDRIEL'
                    AND FECHASQL > @semanaAtras
                    AND FECHASQL <= @ayer
                GROUP BY SCEg.UEN)
            ) as t
            GROUP BY UEN
        ),

        anterior as( -- CTE con ventas de la semana anterior
            SELECT
                t.UEN
                ,sum(t.[Unid Vendidas]) as 'Unid Vend Sem Ant'
                ,sum(t.[Importe Total]) as 'Importe Total Sem Ant'
            FROM(
                SELECT    
                    RTRIM(SCEg.[UEN]) as 'UEN'
                    ,sum(SCEg.CANTIDAD) as 'Unid Vendidas'
                    ,sum(SCEg.IMPORTE) as 'Importe Total'

                FROM [Rumaos].[dbo].[SCEgreso] as SCEg WITH (NOLOCK)
                -- Join a la tabla temporal
                INNER JOIN #ProductosSC as SCPr WITH (NOLOCK)
                    ON (SCEg.UEN = SCPr.UEN AND SCEg.CODIGO = SCPr.CODIGO)

                where FECHASQL > @dosSemanasAtras
                    AND FECHASQL <= @semanaAtras
                    AND SCPr.AGRUPACION NOT IN (
                        'FILTROS'
                        ,'PREMIOS RED MAS DIFERIDOS'
                        ,'REGALOS O CORTESIAS'
                        ,'VENDING2'
                    )
                    AND SCPr.CODIGO NOT IN (
                        'MED'
                        ,'TOR'
                        ,'DOCML'
                        ,'DOCTO'
                        ,'MEDML'
                        ,'MEDTO'
                        ,'MEDMR'
                        ,'DOCMR'
                        ,'MEDRE'
                        ,'DOCMIX'
                        ,'MEDMIX'
                        ,'MIX'
                    )
                GROUP BY SCEg.UEN

                UNION ALL

                (SELECT
                    RTRIM(SCEg.[UEN]) as 'UEN'
                    ,-sum([CANTIDAD]) as 'unid vend'
                    ,-sum([IMPORTE]) as 'Importe Total'
                FROM [Rumaos].[dbo].[SCEgreso] as SCEg WITH (NOLOCK)
                INNER JOIN #ProductosSC as SCPr WITH (NOLOCK)
                    ON (SCEg.UEN = SCPr.UEN AND SCEg.CODIGO = SCPr.CODIGO)
                where SCEg.CODIGO not in (
                    'MED'
                    ,'TOR'
                    ,'DOCML'
                    ,'DOCTO'
                    ,'MEDML'
                    ,'MEDTO'
                    ,'DOCMIX'
                    ,'DOCMR'
                    ,'MEDMIX'
                    ,'MEDMR'
                    ,'MEDRE'
                    ,'MIX'
                )
                    AND SCPr.AGRUPACION = 'PROMOS'
                    AND SCEg.UEN = 'PERDRIEL'
                    AND FECHASQL > @dosSemanasAtras
                    AND FECHASQL <= @semanaAtras
                GROUP BY SCEg.UEN)
            ) as t
            GROUP BY UEN
        )

        SELECT -- Resultado Final
            actual.UEN
            ,anterior.[Unid Vend Sem Ant]
            ,actual.[Unid Vend Sem Actual]
            ,anterior.[Importe Total Sem Ant]
            ,actual.[Importe Total Sem Actual]

        FROM actual
        LEFT JOIN anterior
            ON actual.UEN = anterior.UEN
        ORDER BY UEN
        """
        , conexCentral
    )


    df_fullAZ = pd.read_sql(
        """
        
        DECLARE @ayer date
        set @ayer = GETDATE()-1
        DECLARE @semanaAtras date
        set @semanaAtras = GETDATE()-8
        DECLARE @dosSemanasAtras date
        set @dosSemanasAtras = GETDATE()-15;


        SELECT
            actual.UEN
            ,anterior.[Unid Vend Sem Ant]
            ,actual.[Unid Vend Sem Actual]
            ,anterior.[Importe Total Sem Ant]
            ,actual.[Importe Total Sem Actual]

        FROM (
            SELECT
                [UEN]
                ,sum([Cantidad]) as 'Unid Vend Sem Actual'
                ,sum([ImporteTotal]) as 'Importe Total Sem Actual'

            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]
            where FECHA > @semanaAtras
                AND FECHA <= @ayer
                AND Rubro <> 'Panaderia'
                
            GROUP BY UEN
        ) as actual

        LEFT JOIN (
            SELECT
                [UEN]
                ,sum([Cantidad]) as 'Unid Vend Sem Ant'
                ,sum([ImporteTotal]) as 'Importe Total Sem Ant'

            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]
            where FECHA > @dosSemanasAtras
                AND FECHA <= @semanaAtras
                AND Rubro <> 'Panaderia'
                
            GROUP BY UEN
        ) as anterior

            ON actual.UEN = anterior.UEN
        """
        , conexAZMil
    )


    df_fullPI = pd.read_sql(
        """
        
        DECLARE @ayer date
        set @ayer = GETDATE()-1
        DECLARE @semanaAtras date
        set @semanaAtras = GETDATE()-8
        DECLARE @dosSemanasAtras date
        set @dosSemanasAtras = GETDATE()-15;


        SELECT
            actual.UEN
            ,anterior.[Unid Vend Sem Ant]
            ,actual.[Unid Vend Sem Actual]
            ,anterior.[Importe Total Sem Ant]
            ,actual.[Importe Total Sem Actual]

        FROM (
            SELECT
                [UEN]
                ,sum([Cantidad]) as 'Unid Vend Sem Actual'
                ,sum([ImporteTotal]) as 'Importe Total Sem Actual'

            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]
            where FECHA > @semanaAtras
                AND FECHA <= @ayer
                AND Rubro <> 'Panaderia'
                
            GROUP BY UEN
        ) as actual

        LEFT JOIN (
            SELECT
                [UEN]
                ,sum([Cantidad]) as 'Unid Vend Sem Ant'
                ,sum([ImporteTotal]) as 'Importe Total Sem Ant'

            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]
            where FECHA > @dosSemanasAtras
                AND FECHA <= @semanaAtras
                AND Rubro <> 'Panaderia'
                
            GROUP BY UEN
        ) as anterior

            ON actual.UEN = anterior.UEN
        """
        , conexPIMil
    )

    # print(df_SC, df_fullAZ, df_fullPI)


    ##########################################################
    # Adding "FULL" data to "SGES" data
    ##########################################################

    # Concat, group by UEN (sum) and sort by UEN
    df_SCFull = pd.concat([df_SC, df_fullAZ, df_fullPI])
    df_SCFull = df_SCFull.groupby("UEN", as_index=False).sum()
    df_SCFull = df_SCFull.sort_values(by="UEN")

    # Creating Total Row
    _temp_tot = df_SCFull.drop(columns=["UEN"]).sum()
    _temp_tot["UEN"] = "TOTAL"

    # Appending Total Row
    df_SCFull = df_SCFull.append(_temp_tot, ignore_index=True)

    # Create columns "Var % Cantidad"
    df_SCFull["Var % Cantidad"] = \
        df_SCFull["Unid Vend Sem Actual"] / df_SCFull["Unid Vend Sem Ant"] -1

    # Create columns "Var % Importe"
    df_SCFull["Var % Importe"] = (
        df_SCFull["Importe Total Sem Actual"] 
        / df_SCFull["Importe Total Sem Ant"] 
        -1
    )

    # print(df_SCFull)

    return df_SCFull


##########################################################
# Get dataframes of bakery sales of "Servicompras" and "FULLs"
##########################################################

def _get_pan(conexCentral, conexAZMil, conexPIMil):
    """
    df of bakery sales of actual and previous week
    """

    df_panSC = pd.read_sql(
        """
        -- Importe Total y total de docenas vendidas de panadería SGES


        DECLARE @ayer date
        set @ayer = GETDATE()-1
        DECLARE @semanaAtras date
        set @semanaAtras = GETDATE()-8
        DECLARE @dosSemanasAtras date
        set @dosSemanasAtras = GETDATE()-15;


        WITH actual as ( -- CTE con ventas de la semana actual
            SELECT
                RTRIM(t.UEN) as 'UEN'
                ,sum(t.[Doc Vendidas]) as 'Doc Vend Sem Actual'
                ,sum(t.[Importe Total]) as 'Importe Total Sem Actual'
            FROM(
                SELECT -- Facturas módulo panadería medidas en docenas
                    pan.[UEN]
                    ,ROUND(sum(pan.CANTIDAD), 0) as 'Doc Vendidas'
                    ,sum(pan.cantidad * pan.precio) as 'Importe Total'

                FROM [Rumaos].[dbo].[PanSalDe] as pan
                INNER JOIN dbo.PanSalGe as filtro
                    ON (pan.UEN = filtro.UEN AND pan.NROCOMP = filtro.NROCOMP)
                where pan.fechasql > @semanaAtras
                    and pan.fechasql <= @ayer
                    and filtro.NROCLIENTE = '30'
                    and pan.PRECIO > '0'

                group by pan.UEN

                UNION ALL

                SELECT -- Todas las facturas vendidas por unidad
                    [UEN]
                    ,ROUND(sum(CANTIDAD)/12, 0) as 'Doc Vendidas'
                    ,sum(importe) as 'Importe Total'
        
                FROM [Rumaos].[dbo].[VIEW_VENTAS_POR_EMPLEADO] WITH (NOLOCK)
                where fechasql > @semanaAtras
                    and fechasql <= @ayer
                AND UEN IN (
                    'AZCUENAGA'
                    ,'LAMADRID'
                    ,'PERDRIEL'
                    ,'PERDRIEL2'
                    ,'PUENTE OLIVE'
                    ,'SAN JOSE'
                )
                AND AGRUPACION IN (
                    'PANIFICADOS'
                    ,'PROMOS'
                )
                AND CODIGO IN (
                    'MED'
                    ,'TOR'
                    ,'MEDRE'
                )
                group by UEN

                UNION ALL

                SELECT -- Todas las facturas vendidas por media docena
                    [UEN]
                    ,ROUND(sum(CANTIDAD)/2, 0) as 'Doc Vendidas'
                    ,sum(importe) as 'Importe Total'
        
                FROM [Rumaos].[dbo].[VIEW_VENTAS_POR_EMPLEADO] WITH (NOLOCK)
                where fechasql > @semanaAtras
                    and fechasql <= @ayer
                AND UEN IN (
                    'AZCUENAGA'
                    ,'LAMADRID'
                    ,'PERDRIEL'
                    ,'PERDRIEL2'
                    ,'PUENTE OLIVE'
                    ,'SAN JOSE'
                )
                AND AGRUPACION IN (
                    'PANIFICADOS'
                    ,'PROMOS'
                )
                AND CODIGO IN (
                    'MEDML'
                    ,'MEDTO'
                    ,'MEDMR'
                    ,'MEDMIX'
                )
                group by UEN

                UNION ALL

                SELECT -- Todas las facturas vendidas por docena
                    [UEN]
                    ,sum(CANTIDAD) as 'Doc Vendidas'
                    ,sum(importe) as 'Importe Total'
        
                FROM [Rumaos].[dbo].[VIEW_VENTAS_POR_EMPLEADO] WITH (NOLOCK)
                where fechasql > @semanaAtras
                    and fechasql <= @ayer
                AND UEN IN (
                    'AZCUENAGA'
                    ,'LAMADRID'
                    ,'PERDRIEL'
                    ,'PERDRIEL2'
                    ,'PUENTE OLIVE'
                    ,'SAN JOSE'
                )
                AND AGRUPACION IN (
                    'PANIFICADOS'
                    ,'PROMOS'
                )
                AND CODIGO IN (
                    'DOCML'
                    ,'DOCTO'
                    ,'DOCMR'
                    ,'DOCMIX'
                    ,'MIX'
                )
                group by UEN
            ) as t
            GROUP BY UEN
        ),

        anterior as ( -- CTE con ventas de la semana anterior
            SELECT
                RTRIM(t.UEN) as 'UEN'
                ,sum(t.[Doc Vendidas]) as 'Doc Vend Sem Ant'
                ,sum(t.[Importe Total]) as 'Importe Total Sem Ant'
            FROM(
                SELECT -- Facturas módulo panadería medidas en docenas
                    pan.[UEN]
                    ,ROUND(sum(pan.CANTIDAD), 0) as 'Doc Vendidas'
                    ,sum(pan.cantidad * pan.precio) as 'Importe Total'

                FROM [Rumaos].[dbo].[PanSalDe] as pan
                INNER JOIN dbo.PanSalGe as filtro
                    ON (pan.UEN = filtro.UEN AND pan.NROCOMP = filtro.NROCOMP)
                where pan.fechasql > @dosSemanasAtras
                    and pan.fechasql <= @semanaAtras
                    and filtro.NROCLIENTE = '30'
                    and pan.PRECIO > '0'

                group by pan.UEN

                UNION ALL

                SELECT -- Todas las facturas vendidas por unidad
                    [UEN]
                    ,ROUND(sum(CANTIDAD)/12, 0) as 'Doc Vendidas'
                    ,sum(importe) as 'Importe Total'
        
                FROM [Rumaos].[dbo].[VIEW_VENTAS_POR_EMPLEADO] WITH (NOLOCK)
                where fechasql > @dosSemanasAtras
                    and fechasql <= @semanaAtras
                AND UEN IN (
                    'AZCUENAGA'
                    ,'LAMADRID'
                    ,'PERDRIEL'
                    ,'PERDRIEL2'
                    ,'PUENTE OLIVE'
                    ,'SAN JOSE'
                )
                AND AGRUPACION IN (
                    'PANIFICADOS'
                    ,'PROMOS'
                )
                AND CODIGO IN (
                    'MED'
                    ,'TOR'
                    ,'MEDRE'
                )
                group by UEN

                UNION ALL

                SELECT -- Todas las facturas vendidas por media docena
                    [UEN]
                    ,ROUND(sum(CANTIDAD)/2, 0) as 'Doc Vendidas'
                    ,sum(importe) as 'Importe Total'
        
                FROM [Rumaos].[dbo].[VIEW_VENTAS_POR_EMPLEADO] WITH (NOLOCK)
                where fechasql > @dosSemanasAtras
                    and fechasql <= @semanaAtras
                AND UEN IN (
                    'AZCUENAGA'
                    ,'LAMADRID'
                    ,'PERDRIEL'
                    ,'PERDRIEL2'
                    ,'PUENTE OLIVE'
                    ,'SAN JOSE'
                )
                AND AGRUPACION IN (
                    'PANIFICADOS'
                    ,'PROMOS'
                )
                AND CODIGO IN (
                    'MEDML'
                    ,'MEDTO'
                    ,'MEDMR'
                    ,'MEDMIX'
                )
                group by UEN

                UNION ALL

                SELECT -- Todas las facturas vendidas por docena
                    [UEN]
                    ,sum(CANTIDAD) as 'Doc Vendidas'
                    ,sum(importe) as 'Importe Total'
        
                FROM [Rumaos].[dbo].[VIEW_VENTAS_POR_EMPLEADO] WITH (NOLOCK)
                where fechasql > @dosSemanasAtras
                    and fechasql <= @semanaAtras
                AND UEN IN (
                    'AZCUENAGA'
                    ,'LAMADRID'
                    ,'PERDRIEL'
                    ,'PERDRIEL2'
                    ,'PUENTE OLIVE'
                    ,'SAN JOSE'
                )
                AND AGRUPACION IN (
                    'PANIFICADOS'
                    ,'PROMOS'
                )
                AND CODIGO IN (
                    'DOCML'
                    ,'DOCTO'
                    ,'DOCMR'
                    ,'DOCMIX'
                    ,'MIX'
                )
                group by UEN
            ) as t
            GROUP BY UEN
        )

        SELECT -- Resultado final
            actual.UEN
            ,anterior.[Doc Vend Sem Ant]
            ,actual.[Doc Vend Sem Actual]
            ,anterior.[Importe Total Sem Ant]
            ,actual.[Importe Total Sem Actual]

        FROM actual
        LEFT JOIN anterior
            ON actual.UEN = anterior.UEN
        ORDER BY UEN
        """
        , conexCentral
    )

    # print(df_panSC.info(), df_panSC)

    df_panFullAZ = pd.read_sql(
        """
        -- Cantidad de docenas de panificados vendidos e importe total de ventas
        -- de los full


        DECLARE @ayer date
        set @ayer = GETDATE()-1
        DECLARE @semanaAtras date
        set @semanaAtras = GETDATE()-8
        DECLARE @dosSemanasAtras date
        set @dosSemanasAtras = GETDATE()-15;


        WITH actual as ( -- Definiendo la primer CTE
            SELECT
                t.UEN
                ,sum(t.[Doc Vendidas]) as 'Doc Vend Sem Actual'
                ,sum(t.[Importe Total]) as 'Importe Total Sem Actual'
            FROM(
                SELECT -- Todas las facturas vendidas por unidad
                    [UEN]
                    ,ROUND(sum([Cantidad])/12, 0) as 'Doc Vendidas'
                    ,sum([ImporteTotal]) as 'Importe Total'

                FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]
                where FECHA > @semanaAtras
                    AND FECHA <= @ayer
                    AND Rubro = 'Panaderia'
                    AND IDARTI NOT IN (
                        '00365' -- Facturas x12
                        ,'00366' -- Facturas x6
                    )
                GROUP BY UEN

                UNION ALL

                SELECT -- Todas las facturas vendidas por media docena
                    [UEN]
                    ,ROUND(sum([Cantidad])/2, 0) as 'Doc Vendidas'
                    ,sum([ImporteTotal]) as 'Importe Total'

                FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]
                where FECHA > @semanaAtras
                    AND FECHA <= @ayer
                    AND Rubro = 'Panaderia'
                    AND IDARTI = '00366' -- Facturas x6
                
                GROUP BY UEN

                UNION ALL

                SELECT -- Todas las facturas vendidas por docena
                    [UEN]
                    ,sum([Cantidad]) as 'Doc Vendidas'
                    ,sum([ImporteTotal]) as 'Importe Total'

                FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]
                where FECHA > @semanaAtras
                    AND FECHA <= @ayer
                    AND Rubro = 'Panaderia'
                    AND IDARTI = '00365' -- Facturas x12
                
                GROUP BY UEN
            ) as t
            GROUP BY UEN
        ),

        anterior as ( -- Definiendo la segunda CTE
            SELECT
                t.UEN
                ,sum(t.[Doc Vendidas]) as 'Doc Vend Sem Ant'
                ,sum(t.[Importe Total]) as 'Importe Total Sem Ant'
            FROM(
                SELECT -- Todas las facturas vendidas por unidad
                    [UEN]
                    ,ROUND(sum([Cantidad])/12, 0) as 'Doc Vendidas'
                    ,sum([ImporteTotal]) as 'Importe Total'

                FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]
                where FECHA > @dosSemanasAtras
                    AND FECHA <= @semanaAtras
                    AND Rubro = 'Panaderia'
                    AND IDARTI NOT IN (
                        '00365' -- Facturas x12
                        ,'00366' -- Facturas x6
                    )
                GROUP BY UEN

                UNION ALL

                SELECT -- Todas las facturas vendidas por media docena
                    [UEN]
                    ,ROUND(sum([Cantidad])/2, 0) as 'Doc Vendidas'
                    ,sum([ImporteTotal]) as 'Importe Total'

                FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]
                where FECHA > @dosSemanasAtras
                    AND FECHA <= @semanaAtras
                    AND Rubro = 'Panaderia'
                    AND IDARTI = '00366' -- Facturas x6
                
                GROUP BY UEN

                UNION ALL

                SELECT -- Todas las facturas vendidas por docena
                    [UEN]
                    ,sum([Cantidad]) as 'Doc Vendidas'
                    ,sum([ImporteTotal]) as 'Importe Total'

                FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]
                where FECHA > @dosSemanasAtras
                    AND FECHA <= @semanaAtras
                    AND Rubro = 'Panaderia'
                    AND IDARTI = '00365' -- Facturas x12
                
                GROUP BY UEN
            ) as t
            GROUP BY UEN
        )


        SELECT -- Resultado final
            actual.UEN
            ,anterior.[Doc Vend Sem Ant]
            ,actual.[Doc Vend Sem Actual]
            ,anterior.[Importe Total Sem Ant]
            ,actual.[Importe Total Sem Actual]

        FROM actual
        left join anterior
            on actual.UEN = anterior.UEN
        """
        , conexAZMil
    )

    # print(df_panFullAZ.info(), df_panFullAZ)


    df_panFullPI = pd.read_sql(
        """
        -- Cantidad de docenas de panificados vendidos e importe total de ventas
        -- de los full


        DECLARE @ayer date
        set @ayer = GETDATE()-1
        DECLARE @semanaAtras date
        set @semanaAtras = GETDATE()-8
        DECLARE @dosSemanasAtras date
        set @dosSemanasAtras = GETDATE()-15;


        WITH actual as ( -- Definiendo la primer CTE
            SELECT
                t.UEN
                ,sum(t.[Doc Vendidas]) as 'Doc Vend Sem Actual'
                ,sum(t.[Importe Total]) as 'Importe Total Sem Actual'
            FROM(
                SELECT -- Todas las facturas vendidas por unidad
                    [UEN]
                    ,ROUND(sum([Cantidad])/12, 0) as 'Doc Vendidas'
                    ,sum([ImporteTotal]) as 'Importe Total'

                FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]
                where FECHA > @semanaAtras
                    AND FECHA <= @ayer
                    AND Rubro = 'Panaderia'
                    AND IDARTI NOT IN (
                        '00365' -- Facturas x12
                        ,'00366' -- Facturas x6
                    )
                GROUP BY UEN

                UNION ALL

                SELECT -- Todas las facturas vendidas por media docena
                    [UEN]
                    ,ROUND(sum([Cantidad])/2, 0) as 'Doc Vendidas'
                    ,sum([ImporteTotal]) as 'Importe Total'

                FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]
                where FECHA > @semanaAtras
                    AND FECHA <= @ayer
                    AND Rubro = 'Panaderia'
                    AND IDARTI = '00366' -- Facturas x6
                
                GROUP BY UEN

                UNION ALL

                SELECT -- Todas las facturas vendidas por docena
                    [UEN]
                    ,sum([Cantidad]) as 'Doc Vendidas'
                    ,sum([ImporteTotal]) as 'Importe Total'

                FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]
                where FECHA > @semanaAtras
                    AND FECHA <= @ayer
                    AND Rubro = 'Panaderia'
                    AND IDARTI = '00365' -- Facturas x12
                
                GROUP BY UEN
            ) as t
            GROUP BY UEN
        ),

        anterior as ( -- Definiendo la segunda CTE
            SELECT
                t.UEN
                ,sum(t.[Doc Vendidas]) as 'Doc Vend Sem Ant'
                ,sum(t.[Importe Total]) as 'Importe Total Sem Ant'
            FROM(
                SELECT -- Todas las facturas vendidas por unidad
                    [UEN]
                    ,ROUND(sum([Cantidad])/12, 0) as 'Doc Vendidas'
                    ,sum([ImporteTotal]) as 'Importe Total'

                FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]
                where FECHA > @dosSemanasAtras
                    AND FECHA <= @semanaAtras
                    AND Rubro = 'Panaderia'
                    AND IDARTI NOT IN (
                        '00365' -- Facturas x12
                        ,'00366' -- Facturas x6
                    )
                GROUP BY UEN

                UNION ALL

                SELECT -- Todas las facturas vendidas por media docena
                    [UEN]
                    ,ROUND(sum([Cantidad])/2, 0) as 'Doc Vendidas'
                    ,sum([ImporteTotal]) as 'Importe Total'

                FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]
                where FECHA > @dosSemanasAtras
                    AND FECHA <= @semanaAtras
                    AND Rubro = 'Panaderia'
                    AND IDARTI = '00366' -- Facturas x6
                
                GROUP BY UEN

                UNION ALL

                SELECT -- Todas las facturas vendidas por docena
                    [UEN]
                    ,sum([Cantidad]) as 'Doc Vendidas'
                    ,sum([ImporteTotal]) as 'Importe Total'

                FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]
                where FECHA > @dosSemanasAtras
                    AND FECHA <= @semanaAtras
                    AND Rubro = 'Panaderia'
                    AND IDARTI = '00365' -- Facturas x12
                
                GROUP BY UEN
            ) as t
            GROUP BY UEN
        )


        SELECT -- Resultado final
            actual.UEN
            ,anterior.[Doc Vend Sem Ant]
            ,actual.[Doc Vend Sem Actual]
            ,anterior.[Importe Total Sem Ant]
            ,actual.[Importe Total Sem Actual]

        FROM actual
        left join anterior
            on actual.UEN = anterior.UEN
        """
        , conexPIMil
    )

    # print(df_panFullPI.info(), df_panFullPI)


    ##########################################################
    # Adding "FULL" data to "SGES" data
    ##########################################################

    # Concat, group by UEN (sum) and sort by UEN
    df_panSCFull = pd.concat([df_panSC, df_panFullAZ, df_panFullPI])
    df_panSCFull = df_panSCFull.groupby("UEN", as_index=False).sum()
    df_panSCFull = df_panSCFull.sort_values(by="UEN")

    # Creating Total Row
    _temp_tot = df_panSCFull.drop(columns=["UEN"]).sum()
    _temp_tot["UEN"] = "TOTAL"

    # Appending Total Row
    df_panSCFull = df_panSCFull.append(_temp_tot, ignore_index=True)

    # Create columns "Var % Cantidad"
    df_panSCFull["Var % Cantidad"] = \
        df_panSCFull["Doc Vend Sem Actual"] / df_panSCFull["Doc Vend Sem Ant"] -1

    # Create columns "Var % Importe"
    df_panSCFull["Var % Importe"] = (
        df_panSCFull["Importe Total Sem Actual"] 
        / df_panSCFull["Importe Total Sem Ant"] 
        -1
    )

    # print(df_panSC,df_panFullAZ,df_panFullPI)
    # print(df_panSCFull)

    return df_panSCFull



##########################################################
# Get dataframes of "Lubriplaya" sales
##########################################################

def _get_lubriplaya(conexCentral):
    """
    Sales of filters and lubes of actual week and previous week
    """

    df_lubri = pd.read_sql(
        """
        -- Cantidades e importes totales vendidos de Lubriplaya para 
        -- un período determinado


        DECLARE @ayer date
        set @ayer = GETDATE()-1
        DECLARE @semanaAtras date
        set @semanaAtras = GETDATE()-8
        DECLARE @dosSemanasAtras date
        set @dosSemanasAtras = GETDATE()-15;


        WITH actual as ( -- CTE con datos de la semana actual
            SELECT
                RTRIM(t.UEN) as 'UEN'
                ,sum(t.[Unid Vendidas]) as 'Unid Vend Sem Actual'
                ,sum(t.[Importe Total]) as 'Importe Total Sem Actual'
            FROM(
                SELECT 
                [UEN]

                ,sum([CANTIDAD]) as 'Unid Vendidas'

                ,sum([IMPORTE]) as 'Importe Total'

            FROM [Rumaos].[dbo].[VIEW_VENTAS_POR_EMPLEADO] WITH (NOLOCK)
            WHERE CAST(FECHASQL as date) > @semanaAtras
                and CAST(FECHASQL as date) <= @ayer
                and AGRUPACION = 'FILTROS'
                AND UEN = 'PUENTE OLIVE'
            GROUP BY UEN

            UNION ALL

            SELECT 
                [UEN]

                ,sum(-[CANTIDAD]) as 'Unid Vendidas'

                ,sum([IMPORTE]) as 'Importe Total'

            FROM [Rumaos].[dbo].[VMovDet] WITH (NOLOCK)
            WHERE CAST(FECHASQL as date) > @semanaAtras
                and CAST(FECHASQL as date) <= @ayer
                and IMPORTE > '0'
            GROUP BY UEN
        ) as t
        GROUP BY UEN
        ),

        anterior as ( -- CTE con datos de la semana anterior
            SELECT
                RTRIM(t.UEN) as 'UEN'
                ,sum(t.[Unid Vendidas]) as 'Unid Vend Sem Ant'
                ,sum(t.[Importe Total]) as 'Importe Total Sem Ant'
            FROM(
                SELECT 
                [UEN]

                ,sum([CANTIDAD]) as 'Unid Vendidas'

                ,sum([IMPORTE]) as 'Importe Total'

            FROM [Rumaos].[dbo].[VIEW_VENTAS_POR_EMPLEADO] WITH (NOLOCK)
            WHERE CAST(FECHASQL as date) > @dosSemanasAtras
                and CAST(FECHASQL as date) <= @semanaAtras
                and AGRUPACION = 'FILTROS'
                AND UEN = 'PUENTE OLIVE'
            GROUP BY UEN

            UNION ALL

            SELECT 
                [UEN]

                ,sum(-[CANTIDAD]) as 'Unid Vendidas'

                ,sum([IMPORTE]) as 'Importe Total'

            FROM [Rumaos].[dbo].[VMovDet] WITH (NOLOCK)
            WHERE CAST(FECHASQL as date) > @dosSemanasAtras
                and CAST(FECHASQL as date) <= @semanaAtras
                and IMPORTE > '0'
            GROUP BY UEN
        ) as t
        GROUP BY UEN
        )


        select
            actual.UEN
            ,anterior.[Unid Vend Sem Ant]
            ,actual.[Unid Vend Sem Actual]
            ,anterior.[Importe Total Sem Ant]
            ,actual.[Importe Total Sem Actual]

        FROM actual
        Left join anterior
            ON actual.UEN = anterior.UEN
        Order by UEN
        """
        , conexCentral
    )

    # print(df_lubri.info(), df_lubri)


    ##########################################################
    # Adding Total Row and "Var %" columns
    ##########################################################

    # Creating Total Row
    _temp_tot = df_lubri.drop(columns=["UEN"]).sum()
    _temp_tot["UEN"] = "TOTAL"

    # Appending Total Row
    df_lubri = df_lubri.append(_temp_tot, ignore_index=True)

    # Create columns "Var % Cantidad"
    df_lubri["Var % Cantidad"] = \
        df_lubri["Unid Vend Sem Actual"] / df_lubri["Unid Vend Sem Ant"] -1

    # Create columns "Var % Importe"
    df_lubri["Var % Importe"] = (
        df_lubri["Importe Total Sem Actual"] 
        / df_lubri["Importe Total Sem Ant"] 
        -1
    )

    # print(df_lubri)

    return df_lubri



##########################################################
# Get dataframes of "Grill" sales
##########################################################