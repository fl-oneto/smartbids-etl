"""ETL incremental desde smartbids hacia smartbids_dw.

Carga dimensiones antes de los hechos y usa UPSERT para permitir reejecuciones.
Las credenciales se leen desde variables de entorno o un archivo .env.
"""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Iterable, Sequence
from datetime import date
from typing import Any

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row


BATCH_SIZE = 2_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
LOGGER = logging.getLogger("etl_smartbids")


def connection_kwargs(database: str) -> dict[str, Any]:
    """Construye la configuración común de PostgreSQL."""
    return {
        "host": os.getenv("PG_HOST", "localhost"),
        "port": int(os.getenv("PG_PORT", "5432")),
        "user": required_env("PG_USER"),
        "password": required_env("PG_PASSWORD"),
        "dbname": database,
    }


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Falta la variable de entorno obligatoria {name}")
    return value


def execute_many(
    connection: psycopg.Connection[Any],
    statement: str,
    rows: Sequence[Sequence[Any]],
) -> None:
    """Ejecuta un lote sin hacer commit parcial."""
    if not rows:
        return
    with connection.cursor() as cursor:
        cursor.executemany(statement, rows)


def fetch_all(
    connection: psycopg.Connection[Any], statement: str
) -> list[dict[str, Any]]:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(statement)
        return list(cursor.fetchall())


def chunks(rows: Sequence[Any], size: int = BATCH_SIZE) -> Iterable[Sequence[Any]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def date_key(value: date | None) -> int | None:
    if value is None:
        return None
    return value.year * 10_000 + value.month * 100 + value.day


def load_products(
    source: psycopg.Connection[Any], target: psycopg.Connection[Any]
) -> None:
    LOGGER.info("Extrayendo dim_producto")
    rows = fetch_all(
        source,
        """
        WITH combinaciones AS (
            SELECT
                codigo_producto,
                nombre_producto,
                codigo_categoria,
                categoria,
                rubro_n1,
                rubro_n2,
                rubro_n3,
                ROW_NUMBER() OVER (
                    PARTITION BY codigo_producto
                    ORDER BY COUNT(*) DESC,
                             codigo_categoria NULLS LAST,
                             categoria NULLS LAST
                ) AS posicion
            FROM staging.vw_oc_limpias
            GROUP BY
                codigo_producto,
                nombre_producto,
                codigo_categoria,
                categoria,
                rubro_n1,
                rubro_n2,
                rubro_n3
        )
        SELECT
            BTRIM(p.codigo_producto::text) AS producto_codigo,
            COALESCE(
                NULLIF(BTRIM(p.descripcion), ''),
                NULLIF(BTRIM(c.nombre_producto), ''),
                'Producto sin nombre'
            ) AS producto_nombre,
            c.codigo_categoria AS producto_categoria_codigo,
            c.categoria AS producto_categoria,
            COALESCE(NULLIF(BTRIM(p.glosa_nivel2), ''), c.rubro_n1)
                AS producto_rubro_n1,
            COALESCE(NULLIF(BTRIM(p.glosa_nivel3), ''), c.rubro_n2)
                AS producto_rubro_n2,
            COALESCE(NULLIF(BTRIM(p.glosa_nivel4), ''), c.rubro_n3)
                AS producto_rubro_n3
        FROM combinaciones c
        JOIN catalog.producto p
          ON BTRIM(p.codigo_producto::text) = c.codigo_producto
        WHERE c.posicion = 1
        ORDER BY producto_codigo
        """,
    )

    statement = """
        INSERT INTO dw.dim_producto (
            producto_codigo,
            producto_nombre,
            producto_categoria_codigo,
            producto_categoria,
            producto_rubro_n1,
            producto_rubro_n2,
            producto_rubro_n3
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (producto_codigo) DO UPDATE SET
            producto_nombre = EXCLUDED.producto_nombre,
            producto_categoria_codigo = EXCLUDED.producto_categoria_codigo,
            producto_categoria = EXCLUDED.producto_categoria,
            producto_rubro_n1 = EXCLUDED.producto_rubro_n1,
            producto_rubro_n2 = EXCLUDED.producto_rubro_n2,
            producto_rubro_n3 = EXCLUDED.producto_rubro_n3
    """
    values = [
        (
            row["producto_codigo"],
            row["producto_nombre"],
            row["producto_categoria_codigo"],
            row["producto_categoria"],
            row["producto_rubro_n1"],
            row["producto_rubro_n2"],
            row["producto_rubro_n3"],
        )
        for row in rows
    ]
    for batch in chunks(values):
        execute_many(target, statement, batch)
    LOGGER.info("dim_producto procesada: %s filas", len(rows))


def load_providers(
    source: psycopg.Connection[Any], target: psycopg.Connection[Any]
) -> None:
    LOGGER.info("Extrayendo dim_proveedor")
    rows = fetch_all(
        source,
        """
        WITH combinaciones AS (
            SELECT
                codigo_proveedor,
                proveedor_sucursal_codigo,
                nombre_proveedor,
                comuna_proveedor,
                region_proveedor,
                pais_proveedor,
                ROW_NUMBER() OVER (
                    PARTITION BY codigo_proveedor, proveedor_sucursal_codigo
                    ORDER BY COUNT(*) DESC,
                             nombre_proveedor NULLS LAST,
                             comuna_proveedor NULLS LAST
                ) AS posicion
            FROM staging.vw_oc_limpias
            GROUP BY
                codigo_proveedor,
                proveedor_sucursal_codigo,
                nombre_proveedor,
                comuna_proveedor,
                region_proveedor,
                pais_proveedor
        )
        SELECT
            c.codigo_proveedor AS proveedor_codigo,
            c.proveedor_sucursal_codigo::bigint
                AS proveedor_sucursal_codigo,
            COALESCE(
                NULLIF(BTRIM(p.prov_razon_social), ''),
                NULLIF(BTRIM(c.nombre_proveedor), ''),
                'Proveedor sin nombre'
            ) AS proveedor_nombre,
            c.comuna_proveedor AS proveedor_comuna,
            c.region_proveedor AS proveedor_region,
            c.pais_proveedor AS proveedor_pais
        FROM combinaciones c
        LEFT JOIN catalog.proveedor p
          ON p.prov_codigo_proveedor::text = c.codigo_proveedor
         AND p.prov_codigo_sucursal = c.proveedor_sucursal_codigo::bigint
        WHERE c.posicion = 1
        ORDER BY proveedor_codigo, proveedor_sucursal_codigo
        """,
    )

    statement = """
        INSERT INTO dw.dim_proveedor (
            proveedor_codigo,
            proveedor_sucursal_codigo,
            proveedor_nombre,
            proveedor_comuna,
            proveedor_region,
            proveedor_pais
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (proveedor_codigo, proveedor_sucursal_codigo)
        DO UPDATE SET
            proveedor_nombre = EXCLUDED.proveedor_nombre,
            proveedor_comuna = EXCLUDED.proveedor_comuna,
            proveedor_region = EXCLUDED.proveedor_region,
            proveedor_pais = EXCLUDED.proveedor_pais
    """
    values = [
        (
            row["proveedor_codigo"],
            row["proveedor_sucursal_codigo"],
            row["proveedor_nombre"],
            row["proveedor_comuna"],
            row["proveedor_region"],
            row["proveedor_pais"],
        )
        for row in rows
    ]
    for batch in chunks(values):
        execute_many(target, statement, batch)
    LOGGER.info("dim_proveedor procesada: %s filas", len(rows))


def load_buyers(
    source: psycopg.Connection[Any], target: psycopg.Connection[Any]
) -> None:
    LOGGER.info("Extrayendo dim_comprador")
    rows = fetch_all(
        source,
        """
        WITH combinaciones AS (
            SELECT
                SPLIT_PART(codigo_orden, '-', 1)::bigint
                    AS unidad_codigo_publico,
                unidad_compra,
                codigo_organismo_publico,
                organismo_publico,
                sector,
                ciudad_unidad_compra,
                region_unidad_compra,
                pais_unidad_compra,
                ROW_NUMBER() OVER (
                    PARTITION BY SPLIT_PART(codigo_orden, '-', 1)::bigint
                    ORDER BY COUNT(*) DESC,
                             unidad_compra NULLS LAST,
                             organismo_publico NULLS LAST
                ) AS posicion
            FROM staging.vw_oc_limpias
            WHERE SPLIT_PART(codigo_orden, '-', 1) ~ '^[0-9]+$'
            GROUP BY
                SPLIT_PART(codigo_orden, '-', 1)::bigint,
                unidad_compra,
                codigo_organismo_publico,
                organismo_publico,
                sector,
                ciudad_unidad_compra,
                region_unidad_compra,
                pais_unidad_compra
        )
        SELECT
            c.unidad_codigo_publico AS comprador_unidad_codigo_publico,
            COALESCE(
                NULLIF(BTRIM(uc.ucom_descripcion), ''),
                NULLIF(BTRIM(c.unidad_compra), ''),
                'Unidad de compra sin nombre'
            ) AS comprador_unidad_nombre,
            c.codigo_organismo_publico::bigint AS comprador_organismo_codigo,
            COALESCE(
                NULLIF(BTRIM(o.org_nombre), ''),
                NULLIF(BTRIM(c.organismo_publico), ''),
                'Organismo sin nombre'
            ) AS comprador_organismo_nombre,
            c.sector AS comprador_sector,
            c.ciudad_unidad_compra AS comprador_ciudad,
            c.region_unidad_compra AS comprador_region,
            c.pais_unidad_compra AS comprador_pais
        FROM combinaciones c
        LEFT JOIN procurement.unidad_compra uc
          ON uc.codigo_unidad_compra = c.unidad_codigo_publico
        LEFT JOIN catalog.organismo o
          ON o.codigo_organismo = c.codigo_organismo_publico::bigint
        WHERE c.posicion = 1
        ORDER BY comprador_unidad_codigo_publico
        """,
    )

    statement = """
        INSERT INTO dw.dim_comprador (
            comprador_unidad_codigo_publico,
            comprador_unidad_nombre,
            comprador_organismo_codigo,
            comprador_organismo_nombre,
            comprador_sector,
            comprador_ciudad,
            comprador_region,
            comprador_pais
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (comprador_unidad_codigo_publico) DO UPDATE SET
            comprador_unidad_nombre = EXCLUDED.comprador_unidad_nombre,
            comprador_organismo_codigo = EXCLUDED.comprador_organismo_codigo,
            comprador_organismo_nombre = EXCLUDED.comprador_organismo_nombre,
            comprador_sector = EXCLUDED.comprador_sector,
            comprador_ciudad = EXCLUDED.comprador_ciudad,
            comprador_region = EXCLUDED.comprador_region,
            comprador_pais = EXCLUDED.comprador_pais
    """
    values = [
        (
            row["comprador_unidad_codigo_publico"],
            row["comprador_unidad_nombre"],
            row["comprador_organismo_codigo"],
            row["comprador_organismo_nombre"],
            row["comprador_sector"],
            row["comprador_ciudad"],
            row["comprador_region"],
            row["comprador_pais"],
        )
        for row in rows
    ]
    for batch in chunks(values):
        execute_many(target, statement, batch)
    LOGGER.info("dim_comprador procesada: %s filas", len(rows))


def dimension_maps(
    target: psycopg.Connection[Any],
) -> tuple[dict[str, int], dict[tuple[str, int], int], dict[int, int]]:
    products = {
        row["producto_codigo"]: row["producto_key"]
        for row in fetch_all(
            target,
            "SELECT producto_codigo, producto_key FROM dw.dim_producto",
        )
    }
    providers = {
        (str(row["proveedor_codigo"]), row["proveedor_sucursal_codigo"]): row[
            "proveedor_key"
        ]
        for row in fetch_all(
            target,
            """
            SELECT proveedor_codigo, proveedor_sucursal_codigo, proveedor_key
            FROM dw.dim_proveedor
            """,
        )
    }
    buyers = {
        row["comprador_unidad_codigo_publico"]: row["comprador_key"]
        for row in fetch_all(
            target,
            """
            SELECT comprador_unidad_codigo_publico, comprador_key
            FROM dw.dim_comprador
            """,
        )
    }
    return products, providers, buyers


def load_orders(
    source: psycopg.Connection[Any],
    target: psycopg.Connection[Any],
    providers: dict[tuple[str, int], int],
    buyers: dict[int, int],
) -> None:
    LOGGER.info("Extrayendo fact_orden_compra")
    rows = fetch_all(
        source,
        """
        SELECT
            codigo_orden,
            codigo_licitacion,
            fecha_creacion,
            fecha_envio,
            fecha_aceptacion,
            fecha_ultima_modificacion,
            codigo_proveedor,
            proveedor_sucursal_codigo::bigint AS proveedor_sucursal_codigo,
            SPLIT_PART(codigo_orden, '-', 1)::bigint
                AS unidad_codigo_publico,
            nombre_orden,
            link,
            codigo_estado,
            estado,
            codigo_estado_proveedor,
            estado_proveedor,
            es_compra_confirmada,
            moneda_orden,
            monto_total_orden,
            monto_total_clp,
            total_neto_orden,
            COUNT(*)::integer AS cantidad_items
        FROM staging.vw_oc_limpias
        GROUP BY
            codigo_orden,
            codigo_licitacion,
            fecha_creacion,
            fecha_envio,
            fecha_aceptacion,
            fecha_ultima_modificacion,
            codigo_proveedor,
            proveedor_sucursal_codigo,
            nombre_orden,
            link,
            codigo_estado,
            estado,
            codigo_estado_proveedor,
            estado_proveedor,
            es_compra_confirmada,
            moneda_orden,
            monto_total_orden,
            monto_total_clp,
            total_neto_orden
        ORDER BY codigo_orden
        """,
    )

    statement = """
        INSERT INTO dw.fact_orden_compra (
            orden_codigo,
            licitacion_codigo,
            fecha_creacion_key,
            fecha_envio_key,
            fecha_aceptacion_key,
            fecha_ultima_modificacion_key,
            proveedor_key,
            comprador_key,
            orden_nombre,
            orden_link,
            orden_estado_codigo,
            orden_estado,
            proveedor_estado_codigo,
            proveedor_estado,
            orden_es_confirmada,
            orden_moneda,
            orden_monto_total,
            orden_monto_total_clp,
            orden_total_neto,
            orden_cantidad_items
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (orden_codigo) DO UPDATE SET
            licitacion_codigo = EXCLUDED.licitacion_codigo,
            fecha_creacion_key = EXCLUDED.fecha_creacion_key,
            fecha_envio_key = EXCLUDED.fecha_envio_key,
            fecha_aceptacion_key = EXCLUDED.fecha_aceptacion_key,
            fecha_ultima_modificacion_key = EXCLUDED.fecha_ultima_modificacion_key,
            proveedor_key = EXCLUDED.proveedor_key,
            comprador_key = EXCLUDED.comprador_key,
            orden_nombre = EXCLUDED.orden_nombre,
            orden_link = EXCLUDED.orden_link,
            orden_estado_codigo = EXCLUDED.orden_estado_codigo,
            orden_estado = EXCLUDED.orden_estado,
            proveedor_estado_codigo = EXCLUDED.proveedor_estado_codigo,
            proveedor_estado = EXCLUDED.proveedor_estado,
            orden_es_confirmada = EXCLUDED.orden_es_confirmada,
            orden_moneda = EXCLUDED.orden_moneda,
            orden_monto_total = EXCLUDED.orden_monto_total,
            orden_monto_total_clp = EXCLUDED.orden_monto_total_clp,
            orden_total_neto = EXCLUDED.orden_total_neto,
            orden_cantidad_items = EXCLUDED.orden_cantidad_items,
            orden_fecha_carga = CURRENT_TIMESTAMP
    """

    values: list[tuple[Any, ...]] = []
    for row in rows:
        provider_id = providers.get(
            (row["codigo_proveedor"], row["proveedor_sucursal_codigo"])
        )
        buyer_id = buyers.get(row["unidad_codigo_publico"])
        if provider_id is None or buyer_id is None:
            raise RuntimeError(
                "No se encontró dimensión para la orden "
                f"{row['codigo_orden']}: proveedor={provider_id}, "
                f"comprador={buyer_id}"
            )
        values.append(
            (
                row["codigo_orden"],
                row["codigo_licitacion"],
                date_key(row["fecha_creacion"]),
                date_key(row["fecha_envio"]),
                date_key(row["fecha_aceptacion"]),
                date_key(row["fecha_ultima_modificacion"]),
                provider_id,
                buyer_id,
                row["nombre_orden"],
                row["link"],
                row["codigo_estado"],
                row["estado"],
                row["codigo_estado_proveedor"],
                row["estado_proveedor"],
                row["es_compra_confirmada"],
                row["moneda_orden"],
                row["monto_total_orden"],
                row["monto_total_clp"],
                row["total_neto_orden"],
                row["cantidad_items"],
            )
        )

    for batch in chunks(values):
        execute_many(target, statement, batch)
    LOGGER.info("fact_orden_compra procesada: %s filas", len(rows))


def load_items(
    source: psycopg.Connection[Any],
    target: psycopg.Connection[Any],
    products: dict[str, int],
    providers: dict[tuple[str, int], int],
    buyers: dict[int, int],
) -> None:
    LOGGER.info("Extrayendo fact_item_orden_compra")
    rows = fetch_all(
        source,
        """
        SELECT
            v.id_item,
            v.codigo_orden,
            v.codigo_licitacion,
            v.codigo_producto,
            v.fecha_creacion,
            v.fecha_envio,
            v.fecha_aceptacion,
            v.fecha_ultima_modificacion,
            v.codigo_proveedor,
            v.proveedor_sucursal_codigo::bigint AS proveedor_sucursal_codigo,
            SPLIT_PART(v.codigo_orden, '-', 1)::bigint
                AS unidad_codigo_publico,
            v.es_compra_confirmada,
            v.cantidad,
            v.unidad_medida,
            v.moneda_item,
            v.precio_neto,
            v.cantidad * v.precio_neto
                - CASE
                    WHEN r.total_descuentos IS NULL
                      OR UPPER(TRIM(r.total_descuentos)) IN ('', 'NA', 'N/A')
                    THEN 0::numeric
                    ELSE REPLACE(TRIM(r.total_descuentos), ',', '.')::numeric
                  END
                + CASE
                    WHEN r.total_cargos IS NULL
                      OR UPPER(TRIM(r.total_cargos)) IN ('', 'NA', 'N/A')
                    THEN 0::numeric
                    ELSE REPLACE(TRIM(r.total_cargos), ',', '.')::numeric
                  END AS total_linea_neto
        FROM staging.vw_oc_limpias v
        JOIN staging.ordenes_compra_raw r
          ON TRIM(r.id_item) = v.id_item
        ORDER BY v.id_item
        """,
    )

    statement = """
        INSERT INTO dw.fact_item_orden_compra (
            item_id_mercado_publico,
            orden_codigo,
            licitacion_codigo,
            producto_key,
            fecha_creacion_key,
            fecha_envio_key,
            fecha_aceptacion_key,
            fecha_ultima_modificacion_key,
            proveedor_key,
            comprador_key,
            orden_es_confirmada,
            item_cantidad,
            item_unidad_medida,
            item_moneda,
            item_precio_neto,
            item_total_linea_neto
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (item_id_mercado_publico) DO UPDATE SET
            orden_codigo = EXCLUDED.orden_codigo,
            licitacion_codigo = EXCLUDED.licitacion_codigo,
            producto_key = EXCLUDED.producto_key,
            fecha_creacion_key = EXCLUDED.fecha_creacion_key,
            fecha_envio_key = EXCLUDED.fecha_envio_key,
            fecha_aceptacion_key = EXCLUDED.fecha_aceptacion_key,
            fecha_ultima_modificacion_key = EXCLUDED.fecha_ultima_modificacion_key,
            proveedor_key = EXCLUDED.proveedor_key,
            comprador_key = EXCLUDED.comprador_key,
            orden_es_confirmada = EXCLUDED.orden_es_confirmada,
            item_cantidad = EXCLUDED.item_cantidad,
            item_unidad_medida = EXCLUDED.item_unidad_medida,
            item_moneda = EXCLUDED.item_moneda,
            item_precio_neto = EXCLUDED.item_precio_neto,
            item_total_linea_neto = EXCLUDED.item_total_linea_neto,
            item_fecha_carga = CURRENT_TIMESTAMP
    """

    values: list[tuple[Any, ...]] = []
    for row in rows:
        product_id = products.get(row["codigo_producto"])
        provider_id = providers.get(
            (row["codigo_proveedor"], row["proveedor_sucursal_codigo"])
        )
        buyer_id = buyers.get(row["unidad_codigo_publico"])
        if None in (product_id, provider_id, buyer_id):
            raise RuntimeError(
                "No se encontraron todas las claves para el ítem "
                f"{row['id_item']}"
            )

        base = (
            row["id_item"],
            row["codigo_orden"],
            row["codigo_licitacion"],
            product_id,
            date_key(row["fecha_creacion"]),
            date_key(row["fecha_envio"]),
            date_key(row["fecha_aceptacion"]),
            date_key(row["fecha_ultima_modificacion"]),
            provider_id,
            buyer_id,
            row["es_compra_confirmada"],
            row["cantidad"],
            row["unidad_medida"],
            row["moneda_item"],
            row["precio_neto"],
            row["total_linea_neto"],
        )
        values.append(base)

    for batch in chunks(values):
        execute_many(target, statement, batch)
    LOGGER.info("fact_item_orden_compra procesada: %s filas", len(rows))


def validate(target: psycopg.Connection[Any]) -> None:
    LOGGER.info("Ejecutando validaciones finales")
    result = fetch_all(
        target,
        """
        SELECT
            (SELECT COUNT(*) FROM dw.dim_producto) AS productos,
            (SELECT COUNT(*) FROM dw.dim_proveedor) AS proveedores_sucursales,
            (SELECT COUNT(*) FROM dw.dim_comprador) AS compradores,
            (SELECT COUNT(*) FROM dw.fact_orden_compra) AS ordenes,
            (SELECT COUNT(*) FROM dw.fact_item_orden_compra) AS items
        """,
    )[0]
    LOGGER.info("Conteos finales: %s", result)

    expected = {
        "productos": 5_718,
        "proveedores_sucursales": 9_841,
        "compradores": 2_624,
        "ordenes": 60_386,
        "items": 143_320,
    }
    for name, minimum in expected.items():
        if result[name] < minimum:
            raise RuntimeError(
                f"Validación fallida: {name}={result[name]}, esperado al menos {minimum}"
            )


def main() -> int:
    load_dotenv()
    source_db = os.getenv("DB_ORIGEN", "smartbids")
    target_db = os.getenv("DB_DESTINO", "smartbids_dw")

    LOGGER.info("Iniciando ETL %s -> %s", source_db, target_db)
    try:
        with psycopg.connect(**connection_kwargs(source_db)) as source:
            with psycopg.connect(**connection_kwargs(target_db)) as target:
                load_products(source, target)
                load_providers(source, target)
                load_buyers(source, target)
                products, providers, buyers = dimension_maps(target)
                load_orders(source, target, providers, buyers)
                load_items(source, target, products, providers, buyers)
                validate(target)
                target.commit()
        LOGGER.info("ETL finalizado correctamente")
        return 0
    except Exception:
        LOGGER.exception("El ETL falló; la transacción de destino fue revertida")
        return 1


if __name__ == "__main__":
    sys.exit(main())
