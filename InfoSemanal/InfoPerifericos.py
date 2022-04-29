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



##########################################################
# Getting "Servicompras" and "FULLs" dataframes
##########################################################


    ##########################################################
    # SC and FULL monthly 
    ##########################################################

def _get_SCFull_monthly(conexCentral, conexAZMil, conexPIMil):
    """
    df of general store sales of previous month and forecasting of actual month
    """

    df_SC = pd.read_sql(
        """
        
        /* Esto evita el mensaje de confirmación después de cada
            ejecución permitiendo a pandas generar el df */
        SET NOCOUNT ON 

        /* Variables Fecha Dinámicas*/
        DECLARE @ayer date
        set @ayer = GETDATE()-1
        DECLARE @semanaAtras date
        set @semanaAtras = GETDATE()-8
        DECLARE @dosSemanasAtras date
        set @dosSemanasAtras = GETDATE()-15

        DECLARE @finMesActual date
        set @finMesActual = EOMONTH(GETDATE(),0)

        DECLARE @multiplicadorProyeccion decimal(18,5)
        /* Recordar que la división de números enteros genera enteros, por lo que
            es necesario convertir los enteros en decimales */
        set @multiplicadorProyeccion = (DAY(@finMesActual)+0.0)/(DAY(@ayer)+0.0)

        DECLARE @finMesAnterior date
        set @finMesAnterior = EOMONTH(GETDATE(),-1)

        DECLARE @inicioMesAnterior date
        set @inicioMesAnterior = DATEADD(DAY,1,EOMONTH(GETDATE(),-2));



        /*Tabla temporal de costos con el objetivo de incluir un indice agrupado
        para acelerar JOINs*/
        if OBJECT_ID('tempdb..#costos') is not null
                DROP TABLE #costos

        SELECT [UEN]
            ,[CODIGO]
            ,[FECHASQL]
            ,[CBrutPromPond]
        INTO #costos
        FROM [Rumaos].[dbo].[View_Aprox_CostoHistPromPond] WITH (NOLOCK)

        --create clustered index CI_CostoHist_Uen_Fecha_Cod
            --ON #costos (UEN Asc, FECHASQL Asc, CODIGO ASC);
        create clustered index CI_CostoHist_Cod_Uen_Fecha
            ON #costos (CODIGO ASC, UEN Asc, FECHASQL Asc);


        /*Tabla temporal de descripciones de productos con el objetivo de incluir un 
        indice agrupado para acelerar JOINs*/
        if OBJECT_ID('tempdb..#productos') is not null
                DROP TABLE #productos

        SELECT [UEN]
            ,[CODIGO]
            ,[AGRUPACION]
            ,[ENVASE]
        INTO #productos
        FROM [Rumaos].[dbo].scproduen WITH (NOLOCK)
        --Azcuenaga no se incluye por venta 100% Milenium
        WHERE UEN IN (
            'LAMADRID'
            ,'MERCADO 2'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
            ,'SAN JOSE'
        )

        create clustered index CI_Productos_Cod_Uen
            ON #productos (CODIGO ASC, UEN ASC);


        /*CTE*/
        with SCEgreso_ConCosto as (
            SELECT
                RTRIM(SCE.UEN) as 'UEN'
                ,SCE.FECHASQL
                ,RTRIM(SCE.CODIGO) as 'CODIGO'
                ,sum(SCE.CANTIDAD) as 'Sum of Cantidad'
                ,sum(SCE.IMPORTE) as 'Sum of Importe'
                --,sum(SCE.IMPORTE) / sum(SCE.CANTIDAD) as 'PUnit Prom'
                ,isnull(
                    isnull(
                        max(c.CBrutPromPond)
                        ,(select max(cbrutprompond) as 'cbpp'
                            from #costos as c WITH (NOLOCK)
                            where c.uen = sce.uen
                                and c.codigo = sce.codigo
                                and c.FECHASQL <= sce.FECHASQL
                            group by c.uen, c.codigo)
                    )
                    ,(select
                        max((p.PRECOSTO-p.IMPINT)*1.21 + p.IMPINT) as 'cbpp'
                        from dbo.SCProduen as p WITH (NOLOCK)
                        where p.UEN = sce.UEN
                            and p.CODIGO = sce.CODIGO
                        group by p.UEN, p.CODIGO
                    )
                ) as 'CBrutPromPond'

            FROM SCEgreso as SCE WITH (NOLOCK)
            Left outer join #costos as c
                on sce.UEN = c.UEN
                    and sce.FECHASQL = c.FECHASQL
                    and sce.CODIGO = c.CODIGO
            WHERE SCE.UEN IN (
                'LAMADRID'
                ,'MERCADO 2'
                ,'PERDRIEL'
                ,'PERDRIEL2'
                ,'PUENTE OLIVE'
                ,'SAN JOSE'
            )
            GROUP BY SCE.UEN, SCE.FECHASQL, SCE.CODIGO
        )
        , SCEgreso_Margen as (
        select
            SCEC.UEN
            ,SCEC.FECHASQL
            ,SCEC.CODIGO
            ,p.AGRUPACION
            ,SCEC.[Sum of Cantidad] * p.ENVASE as 'Sum of Cantidad'
            --,SCEC.[PUnit Prom]
            ,SCEC.[Sum of Importe]
            --,SCEC.CBrutPromPond
            ,SCEC.CBrutPromPond * SCEC.[Sum of Cantidad] as 'Sum of Costo'
            --,SCEC.[PUnit Prom] - SCEC.CBrutPromPond as 'PUnit Margen'
            ,SCEC.[Sum of Importe] - (SCEC.CBrutPromPond*SCEC.[Sum of Cantidad]) as 'Margen'

        from SCEgreso_ConCosto as SCEC
        INNER JOIN #productos as p
            on p.UEN = SCEC.UEN
                and p.CODIGO = SCEC.CODIGO
        )

        , mesActualSC as ( -- Mes Actual PROYECTADO
            SELECT
                cte.UEN
                ,filtro.Negocio
                --,cte.AGRUPACION
                ,CAST(ROUND(
                    sum(cte.[Sum of Cantidad])*@multiplicadorProyeccion
                    , 0
                ) as int) as 'Intermensual Ventas'
                --,sum(cte.[Sum of Importe]) as 'Importe Total'
                --,sum(cte.[Sum of Costo]) as 'Costo Total'
                ,CAST(ROUND(
                    sum(cte.Margen)*@multiplicadorProyeccion
                    , 0
                ) as int) as 'Intermensual Margen'
                ,'Proyectado' as 'Orden'
            FROM SCEgreso_Margen as cte

            OUTER APPLY

            (SELECT
                CASE
                    WHEN cte.CODIGO = 'MINI04' AND cte.UEN = 'PERDRIEL' THEN 'Regalos' --hasta que se revise con Eduardo
                    WHEN cte.AGRUPACION IN ('COCINA', 'COCINA PROMOS') THEN 'SANDWICHES'
                    WHEN cte.AGRUPACION IN ('PANIFICADOS', 'PANIFICADOS PROMOS') THEN 'PANADERIA'
                    WHEN cte.AGRUPACION IN ('VENDING', 'VENDING PROMOS') THEN 'CAFETERIA'
                    WHEN cte.AGRUPACION = 'GRILL' THEN 'GRILL'
                    WHEN cte.AGRUPACION IN ('ACCESORIOS AUTOMOTOR', 'FILTROS', 'PRODUCTOS LUBRI') THEN 'LUBRIPLAYA'
                    WHEN cte.AGRUPACION IN ('VENDING2','REGALOS O CORTESIAS','REDMAS','RED PAGO') THEN 'Regalos'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            where cte.FECHASQL > @finMesAnterior 
                and cte.FECHASQL <= @ayer
                and Negocio <> 'Regalos'
            group by Negocio, UEN
            order by Negocio, UEN OFFSET 0 ROWS
        )

        , mesAnteriorSC as ( 
            SELECT
                cte.UEN
                ,filtro.Negocio
                --,cte.AGRUPACION
                ,sum(cte.[Sum of Cantidad]) as 'Intermensual Ventas'
                --,sum(cte.[Sum of Importe]) as 'Importe Total'
                --,sum(cte.[Sum of Costo]) as 'Costo Total'
                ,sum(cte.Margen) as 'Intermensual Margen'
                ,'Anterior' as 'Orden'
            FROM SCEgreso_Margen as cte

            OUTER APPLY

            (SELECT
                CASE
                    WHEN cte.CODIGO = 'MINI04' AND cte.UEN = 'PERDRIEL' THEN 'Regalos' --hasta que se revise con Eduardo
                    WHEN cte.AGRUPACION IN ('COCINA', 'COCINA PROMOS') THEN 'SANDWICHES'
                    WHEN cte.AGRUPACION IN ('PANIFICADOS', 'PANIFICADOS PROMOS') THEN 'PANADERIA'
                    WHEN cte.AGRUPACION IN ('VENDING', 'VENDING PROMOS') THEN 'CAFETERIA'
                    WHEN cte.AGRUPACION = 'GRILL' THEN 'GRILL'
                    WHEN cte.AGRUPACION IN ('ACCESORIOS AUTOMOTOR', 'FILTROS', 'PRODUCTOS LUBRI') THEN 'LUBRIPLAYA'
                    WHEN cte.AGRUPACION IN ('VENDING2','REGALOS O CORTESIAS','REDMAS','RED PAGO') THEN 'Regalos'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            where cte.FECHASQL >= @inicioMesAnterior
                and cte.FECHASQL <= @finMesAnterior
                and Negocio <> 'Regalos'
            group by Negocio, UEN
            order by Negocio, UEN OFFSET 0 ROWS
        )

        ,Lubri as ( -- CTE con datos generales de lubri

        SELECT 
            RTRIM(lub.[UEN]) as 'UEN'
            , CAST(lub.FECHASQL as date) as 'FECHASQL'
            , 'LUBRIPLAYA' as 'Negocio'
            , sum(-lub.[CANTIDAD]) as 'Cantidad Total'
            , sum(lub.[IMPORTE]) as 'Importe Total'
            , sum(-lub.[CANTIDAD] * cost.PRECOSTO * 1.21) as 'Costo Total'
            , sum(lub.[IMPORTE]) - sum(-lub.[CANTIDAD] * cost.PRECOSTO * 1.21) as 'Margen Total'

        FROM [Rumaos].[dbo].[VMovDet] as lub WITH (NOLOCK)
        Left outer JOIN dbo.PLPRODUC as cost WITH (NOLOCK)
            on lub.UEN = cost.UEN
                AND lub.CODPRODUCTO = cost.CODIGO

        WHERE lub.IMPORTE > '0'
            AND (
                    cost.AGRUPACION IN (
                        'LUBRICANTES'
                        , 'LUBRICENTRO'
                        , 'OTROS PRODUCTOS LUBRIPLAYA'
                    )
                    -- Códigos de 'VARIOS' que forman parte de lubriplaya
                    OR cost.CODIGO IN ('0001','0002','0003','0004','1190')
                )

        GROUP BY lub.UEN, lub.FECHASQL

        ),

        mesActualLubri as ( -- Mes Actual PROYECTADO
            SELECT
                Lubri.UEN
                , Lubri.Negocio
                , CAST(ROUND(
                    sum(Lubri.[Cantidad Total])*@multiplicadorProyeccion
                    , 0
                ) as int) as 'Intermensual Ventas'
                --, sum(Lubri.[Importe Total]) as 'Importe Total'
                --, sum(Lubri.[Costo Total]) as 'Costo Total'
                , CAST(ROUND(
                    sum(Lubri.[Margen Total])*@multiplicadorProyeccion
                    , 0
                ) as int) as 'Intermensual Margen'
                , 'Proyectado' as 'Orden'

            FROM Lubri
            WHERE Lubri.FECHASQL > @finMesAnterior
                AND Lubri.FECHASQL <= @ayer
                    
            GROUP BY UEN, Negocio
        ),

        mesAnteriorLubri as ( 
            SELECT
                Lubri.UEN
                , Lubri.Negocio
                , sum(Lubri.[Cantidad Total]) as 'Intermensual Ventas'
                --, sum(Lubri.[Importe Total]) as 'Importe Total'
                --, sum(Lubri.[Costo Total]) as 'Costo Total'
                , sum(Lubri.[Margen Total])  as 'Intermensual Margen'
                , 'Anterior' as 'Orden'

            FROM Lubri
            WHERE CAST(FECHASQL as date) >= @inicioMesAnterior
                and CAST(FECHASQL as date) <= @finMesAnterior
                    
            GROUP BY UEN, Negocio
        )

        ,codpan as ( --Equivalencias entre "Servicompra" y "Panadería"
            SELECT *
            FROM (
                VALUES
                    ('DOCML', 12)
                    , ('DOCMR', 10)
                    , ('DOCMR', 11)
                    , ('DOCTO', 14)
                    , ('DOCTO', 15)
                    , ('DOCTO', 16)
            ) as temp(CODServi, CODPan)
        )

        , pancosto as (-- Costo y unidades para códigos de panadería
            SELECT 
                costo.[CODIGO] as 'CODServi' --Códigos módulo "Servicompra"
                , cod.CODPan --Códigos módulo "Panadería"
                ,MAX(costo.[DESCRIPCION]) as 'DESCRIPCION'
                ,MAX(costo.[ENVASE]) as 'ENVASE'
                ,MAX(costo.[PRECOSTO]) as 'PRECOSTO' --Costos por docena
                    
            FROM [Rumaos].[dbo].[scproduen] as costo

            JOIN codpan as cod
                ON costo.CODIGO = cod.CODServi

            WHERE costo.UEN IN ('AZCUENAGA','LAMADRID','PERDRIEL','PERDRIEL2','PUENTE OLIVE','LAMADRID')
            AND costo.CODIGO in ('DOCML','DOCTO','DOCMR')
                    

            GROUP BY costo.CODIGO, cod.CODPan
            --ORDER BY costo.CODIGO, cod.CODPan
        )

        , mesActualPan as (-- Mes Actual PROYECTADO 
            SELECT
                RTRIM(pan.[UEN]) as 'UEN'
                ,'PANADERIA' as 'Negocio'
                ,CAST(ROUND(
                        sum(pan.CANTIDAD * costo.ENVASE)*@multiplicadorProyeccion
                        , 0
                ) as int) as 'Intermensual Ventas' --En unidades
                --,CAST(
                --    ROUND(sum(pan.CANTIDAD * pan.precio), 0)
                --    as int) as 'Importe Total'
                --,CAST(
                --    ROUND(sum(pan.CANTIDAD * costo.PRECOSTO * 1.21), 0)
                --    as int) as 'Costo Total'
                ,CAST(ROUND(
                        sum(pan.CANTIDAD * (pan.precio - (costo.PRECOSTO * 1.21)))*@multiplicadorProyeccion
                    , 0
                ) as int) as 'Intermensual Margen'
                ,'Proyectado' as 'Orden'

            FROM [Rumaos].[dbo].[PanSalDe] as pan

            INNER JOIN dbo.PanSalGe as filtro
                ON (pan.UEN = filtro.UEN AND pan.NROCOMP = filtro.NROCOMP)
            INNER JOIN pancosto as costo
                ON pan.CODIGO = costo.CODPan

            WHERE pan.fechasql > @finMesAnterior
                        and pan.fechasql <= @ayer
                        and filtro.NROCLIENTE = '30'
                        and pan.PRECIO > '0'
                        and pan.UEN IN ('AZCUENAGA','LAMADRID','PERDRIEL','PERDRIEL2','PUENTE OLIVE','LAMADRID')

            GROUP BY pan.UEN
        )

        , mesAnteriorPan as ( 
            SELECT
                RTRIM(pan.[UEN]) as 'UEN'
                ,'PANADERIA' as 'Negocio'
                ,CAST(ROUND(
                    sum(pan.CANTIDAD * costo.ENVASE)
                    , 0
                ) as int) as 'Intermensual Ventas' --En unidades
                --,CAST(
                --    ROUND(sum(pan.CANTIDAD * pan.precio), 0)
                --    as int) as 'Importe Total'
                --,CAST(
                --    ROUND(sum(pan.CANTIDAD * costo.PRECOSTO * 1.21), 0)
                --    as int) as 'Costo Total'
                ,CAST(ROUND(
                    sum(pan.CANTIDAD * (pan.precio - (costo.PRECOSTO * 1.21)))
                    , 0
                ) as int) as 'Intermensual Margen'
                ,'Anterior' as 'Orden'

            FROM [Rumaos].[dbo].[PanSalDe] as pan

            INNER JOIN dbo.PanSalGe as filtro
                ON (pan.UEN = filtro.UEN AND pan.NROCOMP = filtro.NROCOMP)
            INNER JOIN pancosto as costo
                ON pan.CODIGO = costo.CODPan

            WHERE pan.fechasql >= @inicioMesAnterior
                        and pan.fechasql <= @finMesAnterior
                        and filtro.NROCLIENTE = '30'
                        and pan.PRECIO > '0'
                        and pan.UEN IN ('AZCUENAGA','LAMADRID','PERDRIEL','PERDRIEL2','PUENTE OLIVE','LAMADRID')

            GROUP BY pan.UEN
        )

        , uniontable AS(
            SELECT *
            FROM mesActualSC

            UNION ALL

            SELECT *
            FROM mesAnteriorSC

            UNION ALL

            SELECT *
            FROM mesActualPan

            UNION ALL

            SELECT *
            FROM mesAnteriorPan

            UNION ALL

            SELECT *
            FROM mesActualLubri

            UNION ALL

            SELECT *
            FROM mesAnteriorLubri
        )

        SELECT
            RTRIM(ut.UEN) as 'UEN'
            ,ut.Negocio
            ,sum(ut.[Intermensual Ventas]) as 'Intermensual Ventas'
            --,sum(ut.[Importe Total]) as 'Importe Total'
            --,sum(ut.[Costo Total]) as 'Costo Total'
            ,sum(ut.[Intermensual Margen]) as 'Intermensual Margen'
            ,ut.Orden

        FROM uniontable as ut

        GROUP BY ut.UEN, ut.Negocio, ut.Orden

        ORDER BY Orden, Negocio, UEN
        """
        , conexCentral
    )


    ####################################################


    df_fullAZ = pd.read_sql(
        """
        /* Extrae datos de los FULL de la base de datos MILLENIUM */

        DECLARE @ayer date
        set @ayer = GETDATE()-1
        DECLARE @semanaAtras date
        set @semanaAtras = GETDATE()-8
        DECLARE @dosSemanasAtras date
        set @dosSemanasAtras = GETDATE()-15

        DECLARE @finMesActual date
        set @finMesActual = EOMONTH(GETDATE(),0)

        DECLARE @multiplicadorProyeccion decimal(18,5)
        /* Recordar que la división de números enteros genera enteros, por lo que
            es necesario convertir los enteros en decimales */
        set @multiplicadorProyeccion = (DAY(@finMesActual)+0.0)/(DAY(@ayer)+0.0)

        DECLARE @finMesAnterior date
        set @finMesAnterior = EOMONTH(GETDATE(),-1)

        DECLARE @inicioMesAnterior date
        set @inicioMesAnterior = DATEADD(DAY,1,EOMONTH(GETDATE(),-2));


        WITH pan_mes_actual as (

            -- Consulta de panificados por unidad
            (SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                ,sum([MargenBruto]) as 'Margen Total'
                ,'Proyectado' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA > @finMesAnterior
                AND FECHA <= @ayer
                AND Rubro = 'Panaderia' -- filtrando panificados por unidad
                AND IDARTI NOT IN ('00365','00366','00335','00336')
                        
            GROUP BY Negocio, UEN

        )

        UNION ALL

        (-- Consulta de panificados por docena en unidades
            SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]*12) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                ,sum([MargenBruto]) as 'Margen Total'
                ,'Proyectado' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA > @finMesAnterior
                AND FECHA <= @ayer
                AND Rubro = 'Panaderia' 
                AND IDARTI IN ('00365','00335')
                        
            GROUP BY Negocio, UEN

        )

        UNION ALL

        (-- Consulta de panificados por media docena en unidades
            SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]*6) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                ,sum([MargenBruto]) as 'Margen Total'
                ,'Proyectado' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA > @finMesAnterior
                AND FECHA <= @ayer
                AND Rubro = 'Panaderia' 
                AND IDARTI IN ('00366','00336')
                        
            GROUP BY Negocio, UEN

        )

        UNION ALL

        (--Consulta de rubros adicionales a panadería
            SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                ,sum([MargenBruto]) as 'Margen Total'
                ,'Proyectado' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA > @finMesAnterior
                AND FECHA <= @ayer
                AND Rubro <> 'Panaderia' 
                        
            GROUP BY Negocio, UEN

        )


        )

        , pan_mes_anterior as (

            -- Consulta de panificados por unidad
            (SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                ,sum([MargenBruto]) as 'Margen Total'
                ,'Anterior' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA >= @inicioMesAnterior
                AND FECHA <= @finMesAnterior
                AND Rubro = 'Panaderia' -- filtrando panificados por unidad
                AND IDARTI NOT IN ('00365','00366','00335','00336')
                        
            GROUP BY Negocio, UEN

        )

        UNION ALL

        (-- Consulta de panificados por docena en unidades
            SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]*12) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                ,sum([MargenBruto]) as 'Margen Total'
                ,'Anterior' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA >= @inicioMesAnterior
                AND FECHA <= @finMesAnterior
                AND Rubro = 'Panaderia'
                AND IDARTI IN ('00365','00335')
                        
            GROUP BY Negocio, UEN

        )

        UNION ALL

        (-- Consulta de panificados por media docena en unidades
            SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]*6) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                ,sum([MargenBruto]) as 'Margen Total'
                ,'Anterior' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA >= @inicioMesAnterior
                AND FECHA <= @finMesAnterior
                AND Rubro = 'Panaderia'
                AND IDARTI IN ('00366','00336')
                        
            GROUP BY Negocio, UEN

        )

        UNION ALL

        (-- Consulta de rubros adicionales a panadería
            SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                ,sum([MargenBruto]) as 'Margen Total'
                ,'Anterior' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA >= @inicioMesAnterior
                AND FECHA <= @finMesAnterior
                AND Rubro <> 'Panaderia'
                        
            GROUP BY Negocio, UEN

        )

        )

        , mes_actual as (
        SELECT
                RTRIM([UEN]) as 'UEN'
                ,Negocio
                ,sum([Cantidad Total])*@multiplicadorProyeccion as 'Intermensual Ventas'
                --,sum([Importe Total]) as 'Importe Total'
                --,sum([Costo Total]) as 'Costo Total'
                ,sum([Margen Total])*@multiplicadorProyeccion as 'Intermensual Margen'
                ,Orden
            FROM pan_mes_actual
            GROUP BY UEN, Negocio, Orden
        )

        , mes_anterior as (
        SELECT
                RTRIM([UEN]) as 'UEN'
                ,Negocio
                ,sum([Cantidad Total]) as 'Intermensual Ventas'
                --,sum([Importe Total]) as 'Importe Total'
                --,sum([Costo Total]) as 'Costo Total'
                ,sum([Margen Total]) as 'Intermensual Margen'
                ,Orden
            FROM pan_mes_anterior
            GROUP BY UEN, Negocio, Orden
        )


        Select
            act.UEN
            , act.Negocio
            , act.[Intermensual Ventas]
            --, act.[Importe Total]
            --, act.[Costo Total]
            , act.[Intermensual Margen]
            , act.Orden
        from mes_actual as act

        UNION ALL

        select 
            ant.UEN
            , ant.Negocio
            , ant.[Intermensual Ventas]
            --, ant.[Importe Total]
            --, ant.[Costo Total]
            , ant.[Intermensual Margen]
            , ant.Orden
        from mes_anterior as ant

        order by Orden, Negocio, UEN
        """
        , conexAZMil
    )


    ####################################################


    df_fullPI = pd.read_sql(
        """
        /* Extrae datos de los FULL de la base de datos MILLENIUM */

        DECLARE @ayer date
        set @ayer = GETDATE()-1
        DECLARE @semanaAtras date
        set @semanaAtras = GETDATE()-8
        DECLARE @dosSemanasAtras date
        set @dosSemanasAtras = GETDATE()-15

        DECLARE @finMesActual date
        set @finMesActual = EOMONTH(GETDATE(),0)

        DECLARE @multiplicadorProyeccion decimal(18,5)
        /* Recordar que la división de números enteros genera enteros, por lo que
            es necesario convertir los enteros en decimales */
        set @multiplicadorProyeccion = (DAY(@finMesActual)+0.0)/(DAY(@ayer)+0.0)

        DECLARE @finMesAnterior date
        set @finMesAnterior = EOMONTH(GETDATE(),-1)

        DECLARE @inicioMesAnterior date
        set @inicioMesAnterior = DATEADD(DAY,1,EOMONTH(GETDATE(),-2));


        WITH pan_mes_actual as (

            -- Consulta de panificados por unidad
            (SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                ,sum([MargenBruto]) as 'Margen Total'
                ,'Proyectado' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA > @finMesAnterior
                AND FECHA <= @ayer
                AND Rubro = 'Panaderia' -- filtrando panificados por unidad
                AND IDARTI NOT IN ('00365','00366','00335','00336')
                        
            GROUP BY Negocio, UEN

        )

        UNION ALL

        (-- Consulta de panificados por docena en unidades
            SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]*12) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                ,sum([MargenBruto]) as 'Margen Total'
                ,'Proyectado' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA > @finMesAnterior
                AND FECHA <= @ayer
                AND Rubro = 'Panaderia' 
                AND IDARTI IN ('00365','00335')
                        
            GROUP BY Negocio, UEN

        )

        UNION ALL

        (-- Consulta de panificados por media docena en unidades
            SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]*6) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                ,sum([MargenBruto]) as 'Margen Total'
                ,'Proyectado' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA > @finMesAnterior
                AND FECHA <= @ayer
                AND Rubro = 'Panaderia' 
                AND IDARTI IN ('00366','00336')
                        
            GROUP BY Negocio, UEN

        )

        UNION ALL

        (--Consulta de rubros adicionales a panadería
            SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                ,sum([MargenBruto]) as 'Margen Total'
                ,'Proyectado' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA > @finMesAnterior
                AND FECHA <= @ayer
                AND Rubro <> 'Panaderia' 
                        
            GROUP BY Negocio, UEN

        )


        )

        , pan_mes_anterior as (

            -- Consulta de panificados por unidad
            (SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                ,sum([MargenBruto]) as 'Margen Total'
                ,'Anterior' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA >= @inicioMesAnterior
                AND FECHA <= @finMesAnterior
                AND Rubro = 'Panaderia' -- filtrando panificados por unidad
                AND IDARTI NOT IN ('00365','00366','00335','00336')
                        
            GROUP BY Negocio, UEN

        )

        UNION ALL

        (-- Consulta de panificados por docena en unidades
            SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]*12) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                ,sum([MargenBruto]) as 'Margen Total'
                ,'Anterior' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA >= @inicioMesAnterior
                AND FECHA <= @finMesAnterior
                AND Rubro = 'Panaderia'
                AND IDARTI IN ('00365','00335')
                        
            GROUP BY Negocio, UEN

        )

        UNION ALL

        (-- Consulta de panificados por media docena en unidades
            SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]*6) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                ,sum([MargenBruto]) as 'Margen Total'
                ,'Anterior' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA >= @inicioMesAnterior
                AND FECHA <= @finMesAnterior
                AND Rubro = 'Panaderia'
                AND IDARTI IN ('00366','00336')
                        
            GROUP BY Negocio, UEN

        )

        UNION ALL

        (-- Consulta de rubros adicionales a panadería
            SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                ,sum([MargenBruto]) as 'Margen Total'
                ,'Anterior' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA >= @inicioMesAnterior
                AND FECHA <= @finMesAnterior
                AND Rubro <> 'Panaderia'
                        
            GROUP BY Negocio, UEN

        )

        )

        , mes_actual as (
        SELECT
                RTRIM([UEN]) as 'UEN'
                ,Negocio
                ,sum([Cantidad Total])*@multiplicadorProyeccion as 'Intermensual Ventas'
                --,sum([Importe Total]) as 'Importe Total'
                --,sum([Costo Total]) as 'Costo Total'
                ,sum([Margen Total])*@multiplicadorProyeccion as 'Intermensual Margen'
                ,Orden
            FROM pan_mes_actual
            GROUP BY UEN, Negocio, Orden
        )

        , mes_anterior as (
        SELECT
                RTRIM([UEN]) as 'UEN'
                ,Negocio
                ,sum([Cantidad Total]) as 'Intermensual Ventas'
                --,sum([Importe Total]) as 'Importe Total'
                --,sum([Costo Total]) as 'Costo Total'
                ,sum([Margen Total]) as 'Intermensual Margen'
                ,Orden
            FROM pan_mes_anterior
            GROUP BY UEN, Negocio, Orden
        )


        Select
            act.UEN
            , act.Negocio
            , act.[Intermensual Ventas]
            --, act.[Importe Total]
            --, act.[Costo Total]
            , act.[Intermensual Margen]
            , act.Orden
        from mes_actual as act

        UNION ALL

        select 
            ant.UEN
            , ant.Negocio
            , ant.[Intermensual Ventas]
            --, ant.[Importe Total]
            --, ant.[Costo Total]
            , ant.[Intermensual Margen]
            , ant.Orden
        from mes_anterior as ant

        order by Orden, Negocio, UEN
        """
        , conexPIMil
    )

    # print(df_SC, df_fullAZ, df_fullPI)


    ##########################################################
    # Adding "FULL" data to "SGES" data
    ##########################################################

    # Concat, group by UEN (sum) and sort by UEN
    df_SCFull_monthly = pd.concat([df_SC, df_fullAZ, df_fullPI])
    df_SCFull_monthly = df_SCFull_monthly.groupby(
        ["UEN","Negocio","Orden"]
        , as_index=False
    ).sum()
    df_SCFull_monthly = df_SCFull_monthly.sort_values(by=["Orden","Negocio","UEN"])

    # # Creating Total Row
    # _temp_tot = df_SCFull.drop(columns=["UEN"]).sum()
    # _temp_tot["UEN"] = "TOTAL"

    # # Appending Total Row
    # df_SCFull = df_SCFull.append(_temp_tot, ignore_index=True)

    # # Create columns "Var % Cantidad"
    # df_SCFull["Var % Cantidad"] = \
    #     df_SCFull["Unid Vend Sem Actual"] / df_SCFull["Unid Vend Sem Ant"] -1

    # # Create columns "Var % Importe"
    # df_SCFull["Var % Importe"] = (
    #     df_SCFull["Importe Total Sem Actual"] 
    #     / df_SCFull["Importe Total Sem Ant"] 
    #     -1
    # )

    # print(df_SCFull)

    return df_SCFull_monthly



    ##########################################################
    # SC and FULL weekly
    ##########################################################

def _get_SCFull_weekly(conexCentral, conexAZMil, conexPIMil):
    """
    df of general store sales of previous week and actual week
    """

    df_SC = pd.read_sql(
        """
        
        /* Esto evita el mensaje de confirmación después de cada
            ejecución permitiendo a pandas generar el df */
        SET NOCOUNT ON 

        /* Variables Fecha Dinámicas*/
        DECLARE @ayer date
        set @ayer = GETDATE()-1
        DECLARE @semanaAtras date
        set @semanaAtras = GETDATE()-8
        DECLARE @dosSemanasAtras date
        set @dosSemanasAtras = GETDATE()-15;


        /*Tabla temporal de costos con el objetivo de incluir un indice agrupado
        para acelerar JOINs*/
        if OBJECT_ID('tempdb..#costos') is not null
                DROP TABLE #costos

        SELECT [UEN]
            ,[CODIGO]
            ,[FECHASQL]
            ,[CBrutPromPond]
        INTO #costos
        FROM [Rumaos].[dbo].[View_Aprox_CostoHistPromPond] WITH (NOLOCK)

        --create clustered index CI_CostoHist_Uen_Fecha_Cod
            --ON #costos (UEN Asc, FECHASQL Asc, CODIGO ASC);
        create clustered index CI_CostoHist_Cod_Uen_Fecha
            ON #costos (CODIGO ASC, UEN Asc, FECHASQL Asc);


        /*Tabla temporal de descripciones de productos con el objetivo de incluir un 
        indice agrupado para acelerar JOINs*/
        if OBJECT_ID('tempdb..#productos') is not null
                DROP TABLE #productos

        SELECT [UEN]
            ,[CODIGO]
            ,[AGRUPACION]
            ,[ENVASE]
        INTO #productos
        FROM [Rumaos].[dbo].scproduen WITH (NOLOCK)
        --Azcuenaga no se incluye por venta 100% Milenium
        WHERE UEN IN (
            'LAMADRID'
            ,'MERCADO 2'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
            ,'SAN JOSE'
        )

        create clustered index CI_Productos_Cod_Uen
            ON #productos (CODIGO ASC, UEN ASC);


        /*CTE*/
        with SCEgreso_ConCosto as (
            SELECT
                RTRIM(SCE.UEN) as 'UEN'
                ,SCE.FECHASQL
                ,RTRIM(SCE.CODIGO) as 'CODIGO'
                ,sum(SCE.CANTIDAD) as 'Sum of Cantidad'
                ,sum(SCE.IMPORTE) as 'Sum of Importe'
                --,sum(SCE.IMPORTE) / sum(SCE.CANTIDAD) as 'PUnit Prom'
                ,isnull(
                    isnull(
                        max(c.CBrutPromPond)
                        ,(select max(cbrutprompond) as 'cbpp'
                            from #costos as c WITH (NOLOCK)
                            where c.uen = sce.uen
                                and c.codigo = sce.codigo
                                and c.FECHASQL <= sce.FECHASQL
                            group by c.uen, c.codigo)
                    )
                    ,(select
                        max((p.PRECOSTO-p.IMPINT)*1.21 + p.IMPINT) as 'cbpp'
                        from dbo.SCProduen as p WITH (NOLOCK)
                        where p.UEN = sce.UEN
                            and p.CODIGO = sce.CODIGO
                        group by p.UEN, p.CODIGO
                    )
                ) as 'CBrutPromPond'

            FROM SCEgreso as SCE WITH (NOLOCK)
            Left outer join #costos as c
                on sce.UEN = c.UEN
                    and sce.FECHASQL = c.FECHASQL
                    and sce.CODIGO = c.CODIGO
            WHERE SCE.UEN IN (
                'LAMADRID'
                ,'MERCADO 2'
                ,'PERDRIEL'
                ,'PERDRIEL2'
                ,'PUENTE OLIVE'
                ,'SAN JOSE'
            )
            GROUP BY SCE.UEN, SCE.FECHASQL, SCE.CODIGO
        )
        , SCEgreso_Margen as (
        select
            SCEC.UEN
            ,SCEC.FECHASQL
            ,SCEC.CODIGO
            ,p.AGRUPACION
            ,SCEC.[Sum of Cantidad] * p.ENVASE as 'Sum of Cantidad'
            --,SCEC.[PUnit Prom]
            ,SCEC.[Sum of Importe]
            --,SCEC.CBrutPromPond
            ,SCEC.CBrutPromPond * SCEC.[Sum of Cantidad] as 'Sum of Costo'
            --,SCEC.[PUnit Prom] - SCEC.CBrutPromPond as 'PUnit Margen'
            ,SCEC.[Sum of Importe] - (SCEC.CBrutPromPond*SCEC.[Sum of Cantidad]) as 'Margen'

        from SCEgreso_ConCosto as SCEC
        INNER JOIN #productos as p
            on p.UEN = SCEC.UEN
                and p.CODIGO = SCEC.CODIGO
        )

        , actualSC as ( --semana actual
            SELECT
                cte.UEN
                ,filtro.Negocio
                --,cte.AGRUPACION
                ,sum(cte.[Sum of Cantidad]) as 'Cantidad Total'
                --,sum(cte.[Sum of Importe]) as 'Importe Total'
                --,sum(cte.[Sum of Costo]) as 'Costo Total'
                --,sum(cte.Margen) as 'Margen Total'
                ,'Semanal' as 'Orden'
            FROM SCEgreso_Margen as cte

            OUTER APPLY

            (SELECT
                CASE
                    WHEN cte.CODIGO = 'MINI04' AND cte.UEN = 'PERDRIEL' THEN 'Regalos' --hasta que se revise con Eduardo
                    WHEN cte.AGRUPACION IN ('COCINA', 'COCINA PROMOS') THEN 'SANDWICHES'
                    WHEN cte.AGRUPACION IN ('PANIFICADOS', 'PANIFICADOS PROMOS') THEN 'PANADERIA'
                    WHEN cte.AGRUPACION IN ('VENDING', 'VENDING PROMOS') THEN 'CAFETERIA'
                    WHEN cte.AGRUPACION = 'GRILL' THEN 'GRILL'
                    WHEN cte.AGRUPACION IN ('ACCESORIOS AUTOMOTOR', 'FILTROS', 'PRODUCTOS LUBRI') THEN 'LUBRIPLAYA'
                    WHEN cte.AGRUPACION IN ('VENDING2','REGALOS O CORTESIAS','REDMAS','RED PAGO') THEN 'Regalos'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            where cte.FECHASQL > @semanaAtras 
                and cte.FECHASQL <= @ayer
                and Negocio <> 'Regalos'
            group by Negocio, UEN
            order by Negocio, UEN OFFSET 0 ROWS
        )



        ,Lubri as ( -- CTE con datos generales de lubri

        SELECT 
            RTRIM(lub.[UEN]) as 'UEN'
            , CAST(lub.FECHASQL as date) as 'FECHASQL'
            , 'LUBRIPLAYA' as 'Negocio'
            , sum(-lub.[CANTIDAD]) as 'Cantidad Total'
            , sum(lub.[IMPORTE]) as 'Importe Total'
            , sum(-lub.[CANTIDAD] * cost.PRECOSTO * 1.21) as 'Costo Total'
            , sum(lub.[IMPORTE]) - sum(-lub.[CANTIDAD] * cost.PRECOSTO * 1.21) as 'Margen Total'

        FROM [Rumaos].[dbo].[VMovDet] as lub WITH (NOLOCK)
        Left outer JOIN dbo.PLPRODUC as cost WITH (NOLOCK)
            on lub.UEN = cost.UEN
                AND lub.CODPRODUCTO = cost.CODIGO

        WHERE lub.IMPORTE > '0'
            AND (
                    cost.AGRUPACION IN (
                        'LUBRICANTES'
                        , 'LUBRICENTRO'
                        , 'OTROS PRODUCTOS LUBRIPLAYA'
                    )
                    -- Códigos de 'VARIOS' que forman parte de lubriplaya
                    OR cost.CODIGO IN ('0001','0002','0003','0004','1190')
                )

        GROUP BY lub.UEN, lub.FECHASQL

        ),

        actualLubri as ( -- CTE con datos de la semana actual
            SELECT
                Lubri.UEN
                , Lubri.Negocio
                , sum(Lubri.[Cantidad Total]) as 'Cantidad Total'
                --, sum(Lubri.[Importe Total]) as 'Importe Total'
                --, sum(Lubri.[Costo Total]) as 'Costo Total'
                --, sum(Lubri.[Margen Total]) as 'Margen Total'
                , 'Semanal' as 'Orden'

            FROM Lubri
            WHERE Lubri.FECHASQL > @semanaAtras
                AND Lubri.FECHASQL <= @ayer
                    
            GROUP BY UEN, Negocio
        )



        , codpan as ( --Equivalencias entre "Servicompra" y "Panadería"
            SELECT *
            FROM (
                VALUES
                    ('DOCML', 12)
                    , ('DOCMR', 10)
                    , ('DOCMR', 11)
                    , ('DOCTO', 14)
                    , ('DOCTO', 15)
                    , ('DOCTO', 16)
            ) as temp(CODServi, CODPan)
        )

        , pancosto as (-- Costo y unidades para códigos de panadería
            SELECT 
                costo.[CODIGO] as 'CODServi' --Códigos módulo "Servicompra"
                , cod.CODPan --Códigos módulo "Panadería"
                ,MAX(costo.[DESCRIPCION]) as 'DESCRIPCION'
                ,MAX(costo.[ENVASE]) as 'ENVASE'
                ,MAX(costo.[PRECOSTO]) as 'PRECOSTO' --Costos por docena
                    
            FROM [Rumaos].[dbo].[scproduen] as costo

            JOIN codpan as cod
                ON costo.CODIGO = cod.CODServi

            WHERE costo.UEN IN ('AZCUENAGA','LAMADRID','PERDRIEL','PERDRIEL2','PUENTE OLIVE','LAMADRID')
            AND costo.CODIGO in ('DOCML','DOCTO','DOCMR')
                    

            GROUP BY costo.CODIGO, cod.CODPan
            --ORDER BY costo.CODIGO, cod.CODPan
        )

        , actualPan as (--Semana actual 
            SELECT
                RTRIM(pan.[UEN]) as 'UEN'
                ,'PANADERIA' as 'Negocio'
                ,CAST(
                    ROUND(sum(pan.CANTIDAD * costo.ENVASE), 0)
                    as int) as 'Cantidad Total' --En unidades
                --,CAST(
                --    ROUND(sum(pan.CANTIDAD * pan.precio), 0)
                --    as int) as 'Importe Total'
                --,CAST(
                --    ROUND(sum(pan.CANTIDAD * costo.PRECOSTO * 1.21), 0)
                --    as int) as 'Costo Total'
                --,CAST(
                --    ROUND(sum(pan.CANTIDAD * (pan.precio - (costo.PRECOSTO * 1.21))), 0)
                --    as int) as 'Margen Total'
                ,'Semanal' as 'Orden'

            FROM [Rumaos].[dbo].[PanSalDe] as pan

            INNER JOIN dbo.PanSalGe as filtro
                ON (pan.UEN = filtro.UEN AND pan.NROCOMP = filtro.NROCOMP)
            INNER JOIN pancosto as costo
                ON pan.CODIGO = costo.CODPan

            WHERE pan.fechasql > @semanaAtras
                        and pan.fechasql <= @ayer
                        and filtro.NROCLIENTE = '30'
                        and pan.PRECIO > '0'

            GROUP BY pan.UEN
        )


        , uniontable AS(
            SELECT *
            FROM actualSC

            UNION ALL

            SELECT *
            FROM actualPan

            UNION ALL

            SELECT *
            FROM actualLubri

        )

        SELECT
            RTRIM(ut.UEN) as 'UEN'
            ,ut.Negocio
            ,sum(ut.[Cantidad Total]) as 'Ventas'
            --,sum(ut.[Importe Total]) as 'Importe Total'
            --,sum(ut.[Costo Total]) as 'Costo Total'
            --,sum(ut.[Margen Total]) as 'Margen Total'
            ,ut.Orden

        FROM uniontable as ut

        GROUP BY ut.UEN, ut.Negocio, ut.Orden

        ORDER BY Orden, Negocio, UEN
        """
        , conexCentral
    )


    ####################################################


    df_fullAZ = pd.read_sql(
        """
        /* Extrae datos de los FULL de la base de datos MILLENIUM */

        DECLARE @ayer date
        set @ayer = GETDATE()-1
        DECLARE @semanaAtras date
        set @semanaAtras = GETDATE()-8
        DECLARE @dosSemanasAtras date
        set @dosSemanasAtras = GETDATE()-15;



        WITH pan_semanal_actual as (

            -- Consulta de panificados por unidad
            (SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                --,sum([MargenBruto]) as 'Margen Total'
                ,'Semanal' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA > @semanaAtras
                AND FECHA <= @ayer
                AND Rubro = 'Panaderia' -- filtrando panificados por unidad
                AND IDARTI NOT IN ('00365','00366','00335','00336')
                        
            GROUP BY Negocio, UEN

        )

        UNION ALL

        (-- Consulta de panificados por docena en unidades
            SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]*12) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                --,sum([MargenBruto]) as 'Margen Total'
                ,'Semanal' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA > @semanaAtras
                AND FECHA <= @ayer
                AND Rubro = 'Panaderia' 
                AND IDARTI IN ('00365','00335')
                        
            GROUP BY Negocio, UEN

        )

        UNION ALL

        (-- Consulta de panificados por media docena en unidades
            SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]*6) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                --,sum([MargenBruto]) as 'Margen Total'
                ,'Semanal' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA > @semanaAtras
                AND FECHA <= @ayer
                AND Rubro = 'Panaderia' 
                AND IDARTI IN ('00366','00336')
                        
            GROUP BY Negocio, UEN

        )

        UNION ALL

        (--Consulta de rubros adicionales a panadería
            SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                --,sum([MargenBruto]) as 'Margen Total'
                ,'Semanal' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA > @semanaAtras
                AND FECHA <= @ayer
                AND Rubro <> 'Panaderia' 
                        
            GROUP BY Negocio, UEN

        )


        )


        , actual as (
        SELECT
                RTRIM([UEN]) as 'UEN'
                ,Negocio
                ,sum([Cantidad Total]) as 'Ventas'
                --,sum([Importe Total]) as 'Importe Total'
                --,sum([Costo Total]) as 'Costo Total'
                --,sum([Margen Total]) as 'Margen Total'
                ,Orden
            FROM pan_semanal_actual
            GROUP BY UEN, Negocio, Orden
        )


        Select
            act.UEN
            , act.Negocio
            , act.[Ventas]
            --, act.[Importe Total]
            --, act.[Costo Total]
            --, act.[Margen Total]
            , act.Orden
        from actual as act

        order by Orden, Negocio, UEN
        """
        , conexAZMil
    )


    ####################################################


    df_fullPI = pd.read_sql(
        """
        /* Extrae datos de los FULL de la base de datos MILLENIUM */

        DECLARE @ayer date
        set @ayer = GETDATE()-1
        DECLARE @semanaAtras date
        set @semanaAtras = GETDATE()-8
        DECLARE @dosSemanasAtras date
        set @dosSemanasAtras = GETDATE()-15;



        WITH pan_semanal_actual as (

            -- Consulta de panificados por unidad
            (SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                --,sum([MargenBruto]) as 'Margen Total'
                ,'Semanal' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA > @semanaAtras
                AND FECHA <= @ayer
                AND Rubro = 'Panaderia' -- filtrando panificados por unidad
                AND IDARTI NOT IN ('00365','00366','00335','00336')
                        
            GROUP BY Negocio, UEN

        )

        UNION ALL

        (-- Consulta de panificados por docena en unidades
            SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]*12) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                --,sum([MargenBruto]) as 'Margen Total'
                ,'Semanal' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA > @semanaAtras
                AND FECHA <= @ayer
                AND Rubro = 'Panaderia' 
                AND IDARTI IN ('00365','00335')
                        
            GROUP BY Negocio, UEN

        )

        UNION ALL

        (-- Consulta de panificados por media docena en unidades
            SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]*6) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                --,sum([MargenBruto]) as 'Margen Total'
                ,'Semanal' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA > @semanaAtras
                AND FECHA <= @ayer
                AND Rubro = 'Panaderia' 
                AND IDARTI IN ('00366','00336')
                        
            GROUP BY Negocio, UEN

        )

        UNION ALL

        (--Consulta de rubros adicionales a panadería
            SELECT
                [UEN]
                ,filtro.Negocio
                ,sum([Cantidad]) as 'Cantidad Total'
                --,sum([ImporteTotal]) as 'Importe Total'
                --,sum([CostoTotal]) as 'Costo Total'
                --,sum([MargenBruto]) as 'Margen Total'
                ,'Semanal' as 'Orden'
            FROM [MILENIUM].[dbo].[View_Vta_SC_Con_Costo]

            OUTER APPLY

            (SELECT
                CASE
                    WHEN Rubro = 'Comidas Frias' AND Familia = 'Sandwiches' THEN 'SANDWICHES'
                    WHEN Rubro = 'Panaderia' THEN 'PANADERIA'
                    WHEN Rubro = 'Cafeteria' THEN 'CAFETERIA'
                    WHEN Rubro IN ('Accesorios Automotor', 'Lubricantes') THEN 'LUBRIPLAYA'
                    ELSE 'SALON'
                end as Negocio
            ) as filtro

            WHERE FECHA > @semanaAtras
                AND FECHA <= @ayer
                AND Rubro <> 'Panaderia' 
                        
            GROUP BY Negocio, UEN

        )


        )


        , actual as (
        SELECT
                RTRIM([UEN]) as 'UEN'
                ,Negocio
                ,sum([Cantidad Total]) as 'Ventas'
                --,sum([Importe Total]) as 'Importe Total'
                --,sum([Costo Total]) as 'Costo Total'
                --,sum([Margen Total]) as 'Margen Total'
                ,Orden
            FROM pan_semanal_actual
            GROUP BY UEN, Negocio, Orden
        )


        Select
            act.UEN
            , act.Negocio
            , act.[Ventas]
            --, act.[Importe Total]
            --, act.[Costo Total]
            --, act.[Margen Total]
            , act.Orden
        from actual as act

        order by Orden, Negocio, UEN
        """
        , conexPIMil
    )

    ##########################################################
    # Adding "FULL" data to "SGES" data
    ##########################################################

    # Concat, group by UEN (sum) and sort by UEN
    df_SCFull_weekly = pd.concat([df_SC, df_fullAZ, df_fullPI])
    df_SCFull_weekly = df_SCFull_weekly.groupby(
        ["UEN","Negocio","Orden"]
        , as_index=False
    ).sum()
    df_SCFull_weekly = df_SCFull_weekly.sort_values(by=["Orden","Negocio","UEN"])

    # print(df_SCFull)

    return df_SCFull_weekly



##########################################
# STYLING of the dataframe
##########################################


def _estiladorVtaTitulo(
    df:pd.DataFrame
    , list_Col_Num=[]
    , list_Col_Din=[]
    , list_Col_Perc=[]
    , titulo=""
):
    """
This function will return a styled dataframe that must be assign to a variable.
ARGS:
    df: Dataframe that will be styled.
    list_Col_Num: List of numeric columns that will be formatted with
    zero decimals and thousand separator.
    list_Col_Din: List of numeric columns that will be formatted with money 
    symbol, zero decimals and thousand separator.
    list_Col_Perc: List of numeric columns that will be formatted 
    as percentage.
    titulo: String for the table caption.
    """
    resultado = df.style \
        .format("{0:,.0f}", subset=list_Col_Num) \
        .format("$ {0:,.0f}", subset=list_Col_Din) \
        .format("{:,.2%}", subset=list_Col_Perc) \
        .set_caption(
            titulo
            + " Semana Actual "
            + ((pd.to_datetime("today")-pd.to_timedelta(7,"days"))
            .strftime("%d/%m/%y"))
            + " al "
            + ((pd.to_datetime("today")-pd.to_timedelta(1,"days"))
            .strftime("%d/%m/%y"))
        ) \
        .set_properties(subset=list_Col_Num + list_Col_Din #+ list_Col_Perc
        # commented list_Col_Perc due to multiindex selection
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
                    ,("font-size", "14px")
                ]
            }
        ]) \
        .apply(lambda x: ["background-color: black" if x.name == df.index[-1] 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == df.index[-1]
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["font-size: 15px" if x.name == df.index[-1]
            else "" for i in x]
            , axis=1)

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



##########################################
# MERGING images
##########################################

def _append_images(listOfImages, direction='horizontal',
                  bg_color=(255,255,255), alignment='center'):
    """
    Appends images in horizontal/vertical direction.

    Args:
        listOfImages: List of images with complete path
        direction: direction of concatenation, 'horizontal' or 'vertical'
        bg_color: Background color (default: white)
        alignment: alignment mode if images need padding;
           'left', 'right', 'top', 'bottom', or 'center'

    Returns:
        Concatenated image as a new PIL image object.
    """
    images = [Image.open(x) for x in listOfImages]
    widths, heights = zip(*(i.size for i in images))

    if direction=='horizontal':
        new_width = sum(widths)
        new_height = max(heights)
    else:
        new_width = max(widths)
        new_height = sum(heights)

    new_im = Image.new('RGB', (new_width, new_height), color=bg_color)

    offset = 0
    for im in images:
        if direction=='horizontal':
            y = 0
            if alignment == 'center':
                y = int((new_height - im.size[1])/2)
            elif alignment == 'bottom':
                y = new_height - im.size[1]
            new_im.paste(im, (offset, y))
            offset += im.size[0]
        else:
            x = 0
            if alignment == 'center':
                x = int((new_width - im.size[0])/2)
            elif alignment == 'right':
                x = new_width - im.size[0]
            new_im.paste(im, (x, offset))
            offset += im.size[1]

    return new_im



##########################################
# FUNCTION TO RUN MODULE
##########################################

def perifericoSemanal():
    """
    
    """

    # Timer
    tiempoInicio = pd.to_datetime("today")

    # Connection to DBs
    conexCentral = conectorMSSQL(login)
    conexAZMil = conectorMSSQL(loginAZMilenium)
    conexPIMil = conectorMSSQL(loginPIMilenium)

    # Getting DFs
    df_month = _get_SCFull_monthly(conexCentral, conexAZMil, conexPIMil)
    df_week = _get_SCFull_weekly(conexCentral, conexAZMil, conexPIMil)

    # Pivot DF to create a multiindex
    df_pivot_m = df_month.pivot(index=["Negocio", "UEN"], columns=["Orden"])
    df_pivot_w = df_week.pivot(index=["Negocio", "UEN"], columns=["Orden"])

    # Join DFs using their indexes
    df_pivot_join = df_pivot_w.join(df_pivot_m)

    # Get DF for each group
    df_salon = df_pivot_join.loc["SALON"]
    df_pan = df_pivot_join.loc["PANADERIA"]
    df_cafe = df_pivot_join.loc["CAFETERIA"]
    df_sandwich = df_pivot_join.loc["SANDWICHES"]
    df_lubri = df_pivot_join.loc["LUBRIPLAYA"]
    
    
    # Total Row for each group
    df_salon.loc["TOTAL"] = df_salon.sum(numeric_only=True)
    df_pan.loc["TOTAL"] = df_pan.sum(numeric_only=True)
    df_cafe.loc["TOTAL"] = df_cafe.sum(numeric_only=True)
    df_sandwich.loc["TOTAL"] = df_sandwich.sum(numeric_only=True)
    df_lubri.loc["TOTAL"] = df_lubri.sum(numeric_only=True)



    # Use a tuple to create a column and define a new lvl 1 name for each group
    df_salon[("Intermensual Ventas","Var %")]=(
        (df_salon[("Intermensual Ventas","Proyectado")] 
        - df_salon[("Intermensual Ventas","Anterior")])
        / abs(df_salon[("Intermensual Ventas","Anterior")])
    )
    df_salon[("Intermensual Margen","Var %")]=(
        (df_salon[("Intermensual Margen","Proyectado")] 
        - df_salon[("Intermensual Margen","Anterior")])
        / abs(df_salon[("Intermensual Margen","Anterior")])
    )

    df_pan[("Intermensual Ventas","Var %")]=(
        (df_pan[("Intermensual Ventas","Proyectado")] 
        - df_pan[("Intermensual Ventas","Anterior")])
        / abs(df_pan[("Intermensual Ventas","Anterior")])
    )
    df_pan[("Intermensual Margen","Var %")]=(
        (df_pan[("Intermensual Margen","Proyectado")] 
        - df_pan[("Intermensual Margen","Anterior")])
        / abs(df_pan[("Intermensual Margen","Anterior")])
    )

    df_cafe[("Intermensual Ventas","Var %")]=(
        (df_cafe[("Intermensual Ventas","Proyectado")] 
        - df_cafe[("Intermensual Ventas","Anterior")])
        / abs(df_cafe[("Intermensual Ventas","Anterior")])
    )
    df_cafe[("Intermensual Margen","Var %")]=(
        (df_cafe[("Intermensual Margen","Proyectado")] 
        - df_cafe[("Intermensual Margen","Anterior")])
        / abs(df_cafe[("Intermensual Margen","Anterior")])
    )

    df_sandwich[("Intermensual Ventas","Var %")]=(
        (df_sandwich[("Intermensual Ventas","Proyectado")] 
        - df_sandwich[("Intermensual Ventas","Anterior")])
        / abs(df_sandwich[("Intermensual Ventas","Anterior")])
    )
    df_sandwich[("Intermensual Margen","Var %")]=(
        (df_sandwich[("Intermensual Margen","Proyectado")] 
        - df_sandwich[("Intermensual Margen","Anterior")])
        / abs(df_sandwich[("Intermensual Margen","Anterior")])
    )

    df_lubri[("Intermensual Ventas","Var %")]=(
        (df_lubri[("Intermensual Ventas","Proyectado")] 
        - df_lubri[("Intermensual Ventas","Anterior")])
        / abs(df_lubri[("Intermensual Ventas","Anterior")])
    )
    df_lubri[("Intermensual Margen","Var %")]=(
        (df_lubri[("Intermensual Margen","Proyectado")] 
        - df_lubri[("Intermensual Margen","Anterior")])
        / abs(df_lubri[("Intermensual Margen","Anterior")])
    )

    # Sorting columns descending for lvl 0 and ascending for lvl 1 will get the
    # desired order thanks to the naming of columns
    df_salon.sort_index(axis=1, ascending=[False,True], inplace=True)
    df_pan.sort_index(axis=1, ascending=[False,True], inplace=True)
    df_cafe.sort_index(axis=1, ascending=[False,True], inplace=True)
    df_sandwich.sort_index(axis=1, ascending=[False,True], inplace=True)
    df_lubri.sort_index(axis=1, ascending=[False,True], inplace=True)
    
    # We could also sort by using .reindex(columns=["every tuple here"])
    # Example:
    # a1="Intermensual Ventas"
    # a2="Intermensual Margen"
    # b1="Anterior"
    # b2="Proyectado"
    # b3="Var %"
    # # df_salon = df_salon[(a1,b1),(a1,b2),(a1,b3),(a2,b1),(a2,b2)]
    # df_salon=df_salon.reindex(columns=[(a1,b1),(a1,b2),(a1,b3),(a2,b1),(a2,b2)])



    # Styling of DFs
    df_salon_estilo = _estiladorVtaTitulo(
        df_salon
        , list_Col_Num=["Ventas","Intermensual Ventas"]
        , list_Col_Din=["Intermensual Margen"]
        , list_Col_Perc=[
            ("Intermensual Ventas", "Var %")
            , ("Intermensual Margen", "Var %")
        ]
        , titulo="SALÓN"
    )
    df_pan_estilo = _estiladorVtaTitulo(
        df_pan
        , list_Col_Num=["Ventas","Intermensual Ventas"]
        , list_Col_Din=["Intermensual Margen"]
        , list_Col_Perc=[
            ("Intermensual Ventas", "Var %")
            , ("Intermensual Margen", "Var %")
        ]
        , titulo="PANADERÍA"
    )
    df_cafe_estilo = _estiladorVtaTitulo(
        df_cafe
        , list_Col_Num=["Ventas","Intermensual Ventas"]
        , list_Col_Din=["Intermensual Margen"]
        , list_Col_Perc=[
            ("Intermensual Ventas", "Var %")
            , ("Intermensual Margen", "Var %")
        ]
        , titulo="CAFETERIA"
    )
    df_sandwich_estilo = _estiladorVtaTitulo(
        df_sandwich
        , list_Col_Num=["Ventas","Intermensual Ventas"]
        , list_Col_Din=["Intermensual Margen"]
        , list_Col_Perc=[
            ("Intermensual Ventas", "Var %")
            , ("Intermensual Margen", "Var %")
        ]
        , titulo="SANDWICHES"
    )
    df_lubri_estilo = _estiladorVtaTitulo(
        df_lubri
        , list_Col_Num=["Ventas","Intermensual Ventas"]
        , list_Col_Din=["Intermensual Margen"]
        , list_Col_Perc=[
            ("Intermensual Ventas", "Var %")
            , ("Intermensual Margen", "Var %")
        ]
        , titulo="LUBRIPLAYA"
    )



    # Files location
    ubicacion = str(pathlib.Path(__file__).parent)+"\\"

    # # Printing Images
    _df_to_image(df_salon_estilo, ubicacion, "periferico_salon.png")
    _df_to_image(df_pan_estilo, ubicacion, "periferico_pan.png")
    _df_to_image(df_cafe_estilo, ubicacion, "periferico_cafe.png")
    _df_to_image(df_sandwich_estilo, ubicacion, "periferico_sandwich.png")
    _df_to_image(df_lubri_estilo, ubicacion, "periferico_lubri.png")
    


    ######################################################################
    # Group "GRILL" use a try except to be generated if SGES registration is 
    # updated

    try:
        df_grill = df_pivot_join.loc["GRILL"]

        df_grill.loc["TOTAL"] = df_grill.sum(numeric_only=True)

        df_grill[("Intermensual Ventas","Var %")]=(
            (df_grill[("Intermensual Ventas","Proyectado")] 
            - df_grill[("Intermensual Ventas","Anterior")])
            / abs(df_grill[("Intermensual Ventas","Anterior")])
        )
        df_grill[("Intermensual Margen","Var %")]=(
            (df_grill[("Intermensual Margen","Proyectado")] 
            - df_grill[("Intermensual Margen","Anterior")])
            / abs(df_grill[("Intermensual Margen","Anterior")])
        )

        df_lubri.sort_index(axis=1, ascending=[False,True], inplace=True)

        df_grill_estilo = _estiladorVtaTitulo(
            df_grill
            , list_Col_Num=["Intermensual Ventas"]
            , list_Col_Din=["Intermensual Margen"]
            , list_Col_Perc=[
                ("Intermensual Ventas", "Var %")
                , ("Intermensual Margen", "Var %")
            ]
            , titulo="GRILL"
        )

        _df_to_image(df_grill_estilo, ubicacion, "periferico_grill.png")

    except:
        logger.info("NO DATA IN 'SC AGRUP GRILL'")

    ######################################################################



    # Merge images vertically
    listaImg1 = [
        ubicacion + "periferico_salon.png"
        , ubicacion + "periferico_pan.png"
    ]

    listaImg2 = [
        ubicacion + "periferico_cafe.png"
        , ubicacion + "periferico_sandwich.png"
    ]


    fusionImg = _append_images(listaImg1, direction="vertical")
    fusionImg.save(ubicacion + "periferia_salonpan.png")

    fusionImg2 = _append_images(listaImg2, direction="vertical")
    fusionImg2.save(ubicacion + "periferia_cafesandwich.png")



    # Timer
    tiempoFinal = pd.to_datetime("today")
    logger.info(
        "Info Periferia Semanal"
        + "\nTiempo de Ejecucion Total: "
        + str(tiempoFinal-tiempoInicio)
    )



if __name__ == "__main__":
    perifericoSemanal()