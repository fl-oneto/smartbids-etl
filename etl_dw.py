"""ETL incremental desde smartbids hacia smartbids_dw.

Carga dimensiones antes de los hechos y usa UPSERT para permitir reejecuciones.
Las credenciales se leen desde variables de entorno o un archivo .env.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from collections.abc import Iterable, Sequence
from datetime import date
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row


BATCH_SIZE = 2_000

LOGGER = logging.getLogger("etl_smartbids")


def configure_logging() -> Path:
    """Registra la ejecución en consola y en un archivo con rotación."""
    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "etl_dw.log"

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    LOGGER.setLevel(logging.INFO)
    LOGGER.handlers.clear()
    LOGGER.addHandler(console_handler)
    LOGGER.addHandler(file_handler)
    LOGGER.propagate = False
    return log_file


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
        SELECT
            BTRIM(p.codigo_producto::text) AS producto_codigo,
            NULLIF(BTRIM(p.descripcion), '') AS producto_descripcion,
            NULLIF(BTRIM(gp.nombre_grupo_producto), '') AS producto_grupo,
            NULLIF(BTRIM(n1.descripcion), '') AS producto_nivel1,
            NULLIF(BTRIM(p.glosa_nivel2), '') AS producto_nivel2,
            NULLIF(BTRIM(p.glosa_nivel3), '') AS producto_nivel3,
            NULLIF(BTRIM(p.glosa_nivel4), '') AS producto_nivel4
        FROM catalog.producto p
        LEFT JOIN catalog.nivel1_producto n1
            ON p.nivel1 = n1.codigo_nivel1
        LEFT JOIN catalog.grupo_producto gp
            ON n1.codigo_grupo_producto = gp.codigo_grupo_producto
        WHERE p.activo = TRUE
        ORDER BY producto_codigo
        """,
    )

    statement = """
        INSERT INTO dw.dim_producto (
            producto_codigo,
            producto_descripcion,
            producto_grupo,
            producto_nivel1,
            producto_nivel2,
            producto_nivel3,
            producto_nivel4
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (producto_codigo) DO UPDATE SET
            producto_descripcion = EXCLUDED.producto_descripcion,
            producto_grupo = EXCLUDED.producto_grupo,
            producto_nivel1 = EXCLUDED.producto_nivel1,
            producto_nivel2 = EXCLUDED.producto_nivel2,
            producto_nivel3 = EXCLUDED.producto_nivel3,
            producto_nivel4 = EXCLUDED.producto_nivel4
    """

    values = [
        (
            row["producto_codigo"],
            row["producto_descripcion"],
            row["producto_grupo"],
            row["producto_nivel1"],
            row["producto_nivel2"],
            row["producto_nivel3"],
            row["producto_nivel4"],
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
        SELECT
            p.prov_codigo_proveedor AS proveedor_codigo,

            LOWER(NULLIF(BTRIM(p.prov_nombre), ''))
                AS proveedor_nombre,

            c.nombre_comuna AS proveedor_comuna,

            pr.nombre_provincia AS proveedor_provincia,

            r.nombre_region AS proveedor_region,

            pa.pais_nombre AS proveedor_pais,

            LOWER(NULLIF(BTRIM(ae.nombre_actividad), ''))
                AS proveedor_actividad_econ,

            LOWER(NULLIF(BTRIM(ra.nombre_rubro), ''))
                AS proveedor_rubro,

            LOWER(NULLIF(BTRIM(sa.nombre_subrubro), ''))
                AS proveedor_subrubro

        FROM catalog.proveedor p

        LEFT JOIN catalog.comuna c
            ON p.prov_codigo_comuna = c.codigo_comuna

        LEFT JOIN catalog.provincia pr
            ON c.codigo_provincia = pr.codigo_provincia

        LEFT JOIN catalog.region r
            ON pr.codigo_region = r.codigo_region

        LEFT JOIN catalog.pais pa
            ON p.prov_codigo_pais = pa.pais_codigo

        LEFT JOIN catalog.actividad_economica ae
            ON p.prov_cod_actividad_economica = ae.codigo_actividad

        LEFT JOIN catalog.subrubro_actividad sa
            ON ae.codigo_subrubro = sa.codigo_subrubro

        LEFT JOIN catalog.rubro_actividad ra
            ON sa.codigo_rubro = ra.codigo_rubro

        WHERE p.prov_activo = TRUE

        ORDER BY proveedor_codigo
        """,
    )

    statement = """
        INSERT INTO dw.dim_proveedor (
            proveedor_codigo,
            proveedor_nombre,
            proveedor_comuna,
            proveedor_provincia,
            proveedor_region,
            proveedor_pais,
            proveedor_actividad_econ,
            proveedor_rubro,
            proveedor_subrubro
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (proveedor_codigo) DO UPDATE SET
            proveedor_nombre = EXCLUDED.proveedor_nombre,
            proveedor_comuna = EXCLUDED.proveedor_comuna,
            proveedor_provincia = EXCLUDED.proveedor_provincia,
            proveedor_region = EXCLUDED.proveedor_region,
            proveedor_pais = EXCLUDED.proveedor_pais,
            proveedor_actividad_econ = EXCLUDED.proveedor_actividad_econ,
            proveedor_rubro = EXCLUDED.proveedor_rubro,
            proveedor_subrubro = EXCLUDED.proveedor_subrubro
    """

    values = [
        (
            row["proveedor_codigo"],
            row["proveedor_nombre"],
            row["proveedor_comuna"],
            row["proveedor_provincia"],
            row["proveedor_region"],
            row["proveedor_pais"],
            row["proveedor_actividad_econ"],
            row["proveedor_rubro"],
            row["proveedor_subrubro"],
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
        SELECT
            o.codigo_organismo AS comprador_organismo_codigo,

            NULLIF(BTRIM(o.org_nombre), '')
                AS comprador_organismo_nombre,

            NULLIF(BTRIM(o.org_sigla), '')
                AS comprador_org_sigla,

            NULLIF(BTRIM(s.nombre_sector), '')
                AS comprador_sector,

            NULLIF(BTRIM(c.nombre_comuna), '')
                AS comprador_ciudad,

            NULLIF(BTRIM(p.nombre_provincia), '')
                AS comprador_provincia,

            NULLIF(BTRIM(r.nombre_region), '')
                AS comprador_region,

            'Chile' AS comprador_pais

        FROM catalog.organismo o

        LEFT JOIN catalog.sector s
            ON o.org_codigo_sector = s.codigo_sector

        LEFT JOIN catalog.comuna c
            ON o.org_codigo_comuna = c.codigo_comuna

        LEFT JOIN catalog.provincia p
            ON c.codigo_provincia = p.codigo_provincia

        LEFT JOIN catalog.region r
            ON p.codigo_region = r.codigo_region

        WHERE o.activo = TRUE

        ORDER BY comprador_organismo_codigo
        """,
    )

    statement = """
        INSERT INTO dw.dim_comprador (
            comprador_organismo_codigo,
            comprador_organismo_nombre,
            comprador_org_sigla,
            comprador_sector,
            comprador_ciudad,
            comprador_provincia,
            comprador_region,
            comprador_pais
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (comprador_organismo_codigo) DO UPDATE SET
            comprador_organismo_nombre = EXCLUDED.comprador_organismo_nombre,
            comprador_org_sigla = EXCLUDED.comprador_org_sigla,
            comprador_sector = EXCLUDED.comprador_sector,
            comprador_ciudad = EXCLUDED.comprador_ciudad,
            comprador_provincia = EXCLUDED.comprador_provincia,
            comprador_region = EXCLUDED.comprador_region,
            comprador_pais = EXCLUDED.comprador_pais
    """

    values = [
        (
            row["comprador_organismo_codigo"],
            row["comprador_organismo_nombre"],
            row["comprador_org_sigla"],
            row["comprador_sector"],
            row["comprador_ciudad"],
            row["comprador_provincia"],
            row["comprador_region"],
            row["comprador_pais"],
        )
        for row in rows
    ]

    for batch in chunks(values):
        execute_many(target, statement, batch)

    LOGGER.info("dim_comprador procesada: %s filas", len(rows))
    
def load_tipo_licitacion(
    source: psycopg.Connection[Any], target: psycopg.Connection[Any]
) -> None:
    LOGGER.info("Extrayendo dim_tipo_licitacion")

    rows = fetch_all(
        source,
        """
        SELECT
            tl.codigo_tipo_licitacion,
            NULLIF(BTRIM(cl.nombre_cat_licitacion), '')
                AS categoria_lic,
            NULLIF(BTRIM(tr.nombre_tramo_licitacion), '')
                AS tramo_lic
        FROM catalog.tipo_licitacion tl
        LEFT JOIN catalog.categoria_licitacion cl
            ON tl.codigo_cat_licitacion = cl.codigo_cat_licitacion
        LEFT JOIN catalog.tramo_licitacion tr
            ON tl.codigo_tramo_licitacion = tr.codigo_tramo_licitacion
        WHERE tl.activo = TRUE
        ORDER BY tl.codigo_tipo_licitacion
        """,
    )

    statement = """
        INSERT INTO dw.dim_tipo_licitacion (
            codigo_tipo_licitacion,
            categoria_lic,
            tramo_lic
        )
        VALUES (%s, %s, %s)
        ON CONFLICT (codigo_tipo_licitacion) DO UPDATE SET
            categoria_lic = EXCLUDED.categoria_lic,
            tramo_lic = EXCLUDED.tramo_lic
    """

    values = [
        (
            row["codigo_tipo_licitacion"],
            row["categoria_lic"],
            row["tramo_lic"],
        )
        for row in rows
    ]

    for batch in chunks(values):
        execute_many(target, statement, batch)

    LOGGER.info(
        "dim_tipo_licitacion procesada: %s filas",
        len(rows),
    )





def main() -> int:
    load_dotenv()
    log_file = configure_logging()
    source_db = os.getenv("DB_ORIGEN", "smartbids")
    target_db = os.getenv("DB_DESTINO", "smartbids_dw")
    started_at = time.monotonic()

    LOGGER.info("Iniciando ETL %s -> %s | log=%s", source_db, target_db, log_file)
    try:
        with psycopg.connect(**connection_kwargs(source_db)) as source:
            with psycopg.connect(**connection_kwargs(target_db)) as target:
                load_products(source, target)
                load_providers(source, target)
                load_buyers(source, target)
                load_tipo_licitacion(source, target)
                target.commit()
        LOGGER.info(
            "Dimensiones cargadas | duración=%.2f segundos",
            time.monotonic() - started_at,
        )
        return 0
    except Exception:
        LOGGER.exception(
            "El ETL falló; la transacción de destino fue revertida | "
            "duración=%.2f segundos",
            time.monotonic() - started_at,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
