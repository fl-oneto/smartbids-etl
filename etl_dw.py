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

from datetime import datetime

import psycopg

from dotenv import load_dotenv

from psycopg.rows import dict_row

BATCH_SIZE = 2_000

LOGGER = logging.getLogger("etl_smartbids")

def configure_logging() -> Path:
    """Consola, log general rotativo y warnings por ejecucion."""
    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "etl_dw.log"
    warnings_file = log_dir / (
        f"warnings_dw_{datetime.now():%Y%m%d_%H%M%S_%f}.log"
    )
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    for handler in LOGGER.handlers[:]:
        LOGGER.removeHandler(handler)
        handler.close()
    LOGGER.setLevel(logging.INFO)
    LOGGER.propagate = False

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024,
        backupCount=5, encoding="utf-8",
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    warnings_handler = logging.FileHandler(warnings_file, encoding="utf-8")
    warnings_handler.setLevel(logging.WARNING)
    warnings_handler.setFormatter(formatter)

    LOGGER.addHandler(console_handler)
    LOGGER.addHandler(file_handler)
    LOGGER.addHandler(warnings_handler)
    LOGGER.info("Archivo de warnings y errores: %s", warnings_file.resolve())
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

def load_fecha(

    source: psycopg.Connection[Any], target: psycopg.Connection[Any]

) -> None:

    LOGGER.info("Generando dim_fecha desde 2020 hasta 2030")

    rows = fetch_all(

        source,

        """

        SELECT fecha

        FROM generate_series(

            DATE '2020-01-01',

            DATE '2030-12-31',

            INTERVAL '1 day'

        ) AS fechas(fecha)

        """,

    )

    statement = """

        INSERT INTO dw.dim_fecha (

            fecha,

            dia,

            dia_semana,

            nombre_dia,

            nombre_dia_corto,

            mes,

            nombre_mes,

            nombre_mes_corto,

            trimestre,

            semestre,

            anio,

            anio_mes,

            es_fin_semana

        )

        VALUES (

            %s,

            EXTRACT(DAY FROM %s),

            EXTRACT(ISODOW FROM %s),

            CASE EXTRACT(ISODOW FROM %s)

                WHEN 1 THEN 'Lunes'

                WHEN 2 THEN 'Martes'

                WHEN 3 THEN 'Miércoles'

                WHEN 4 THEN 'Jueves'

                WHEN 5 THEN 'Viernes'

                WHEN 6 THEN 'Sábado'

                WHEN 7 THEN 'Domingo'

            END,

            CASE EXTRACT(ISODOW FROM %s)

                WHEN 1 THEN 'Lun'

                WHEN 2 THEN 'Mar'

                WHEN 3 THEN 'Mié'

                WHEN 4 THEN 'Jue'

                WHEN 5 THEN 'Vie'

                WHEN 6 THEN 'Sáb'

                WHEN 7 THEN 'Dom'

            END,

            EXTRACT(MONTH FROM %s),

            CASE EXTRACT(MONTH FROM %s)

                WHEN 1 THEN 'Enero'

                WHEN 2 THEN 'Febrero'

                WHEN 3 THEN 'Marzo'

                WHEN 4 THEN 'Abril'

                WHEN 5 THEN 'Mayo'

                WHEN 6 THEN 'Junio'

                WHEN 7 THEN 'Julio'

                WHEN 8 THEN 'Agosto'

                WHEN 9 THEN 'Septiembre'

                WHEN 10 THEN 'Octubre'

                WHEN 11 THEN 'Noviembre'

                WHEN 12 THEN 'Diciembre'

            END,

            CASE EXTRACT(MONTH FROM %s)

                WHEN 1 THEN 'Ene'

                WHEN 2 THEN 'Feb'

                WHEN 3 THEN 'Mar'

                WHEN 4 THEN 'Abr'

                WHEN 5 THEN 'May'

                WHEN 6 THEN 'Jun'

                WHEN 7 THEN 'Jul'

                WHEN 8 THEN 'Ago'

                WHEN 9 THEN 'Sep'

                WHEN 10 THEN 'Oct'

                WHEN 11 THEN 'Nov'

                WHEN 12 THEN 'Dic'

            END,

            EXTRACT(QUARTER FROM %s),

            CASE

                WHEN EXTRACT(MONTH FROM %s) <= 6 THEN 1

                ELSE 2

            END,

            EXTRACT(YEAR FROM %s),

            TO_CHAR(%s, 'YYYY-MM'),

            EXTRACT(ISODOW FROM %s) IN (6, 7)

        )

        ON CONFLICT (fecha) DO UPDATE SET

            dia = EXCLUDED.dia,

            dia_semana = EXCLUDED.dia_semana,

            nombre_dia = EXCLUDED.nombre_dia,

            nombre_dia_corto = EXCLUDED.nombre_dia_corto,

            mes = EXCLUDED.mes,

            nombre_mes = EXCLUDED.nombre_mes,

            nombre_mes_corto = EXCLUDED.nombre_mes_corto,

            trimestre = EXCLUDED.trimestre,

            semestre = EXCLUDED.semestre,

            anio = EXCLUDED.anio,

            anio_mes = EXCLUDED.anio_mes,

            es_fin_semana = EXCLUDED.es_fin_semana

    """

    values = []

    for row in rows:

        fecha = row["fecha"]

        values.append(

            (

                fecha,  # fecha

                fecha,  # dia

                fecha,  # dia_semana

                fecha,  # nombre_dia

                fecha,  # nombre_dia_corto

                fecha,  # mes

                fecha,  # nombre_mes

                fecha,  # nombre_mes_corto

                fecha,  # trimestre

                fecha,  # semestre

                fecha,  # anio

                fecha,  # anio_mes

                fecha,  # es_fin_semana

            )

        )

    for batch in chunks(values):

        execute_many(target, statement, batch)

    LOGGER.info("dim_fecha procesada: %s filas", len(rows))

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

                AS comprador_comuna,

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

            comprador_comuna,

            comprador_provincia,

            comprador_region,

            comprador_pais

        )

        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)

        ON CONFLICT (comprador_organismo_codigo) DO UPDATE SET

            comprador_organismo_nombre = EXCLUDED.comprador_organismo_nombre,

            comprador_org_sigla = EXCLUDED.comprador_org_sigla,

            comprador_sector = EXCLUDED.comprador_sector,

            comprador_comuna = EXCLUDED.comprador_comuna,

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

            row["comprador_comuna"],

            row["comprador_provincia"],

            row["comprador_region"],

            row["comprador_pais"],

        )

        for row in rows

    ]

    for batch in chunks(values):

        execute_many(target, statement, batch)

    LOGGER.info("dim_comprador procesada: %s filas", len(rows))

def load_tipo_orden_compra(

    source: psycopg.Connection[Any], target: psycopg.Connection[Any]

) -> None:

    LOGGER.info("Cargando dim_tipo_orden_compra")

    values = [

        (8, "SE", "Sin emisión automatica"),

        (9, "CM", "Convenio marco"),

        (13, "AG", "Compra agil"),

        (14, "CC", "Compra coordinada"),

    ]

    statement = """

        INSERT INTO dw.dim_tipo_orden_compra (

            codigo_tipo,

            codigo_abreviado_tipo_oc,

            descripcion_tipo_oc

        )

        VALUES (%s, %s, %s)

        ON CONFLICT (codigo_tipo) DO UPDATE SET

            codigo_abreviado_tipo_oc = EXCLUDED.codigo_abreviado_tipo_oc,

            descripcion_tipo_oc = EXCLUDED.descripcion_tipo_oc

    """

    for batch in chunks(values):

        execute_many(target, statement, batch)

    LOGGER.info(

        "dim_tipo_orden_compra procesada: %s filas",

        len(values),

    )

def load_fact_orden_compra(

    source: psycopg.Connection[Any],

    target: psycopg.Connection[Any],

) -> None:

    from decimal import Decimal, localcontext

    LOGGER.info("Extrayendo fact_orden_compra: importes en CLP")

    rows = fetch_all(

        source,

        """

        SELECT

            oc.codigo AS codigo_orden,

            oc.fecha_envio AS fecha,

            oc.codigo_producto_onu,

            oc.codigo_proveedor,

            oc.codigo_organismo_publico,

            oc.codigo_tipo,

            SUM(oc.total_linea_neto) AS monto_neto,

            SUM(oc.total_impuestos) AS impuesto,

            SUM(oc.total_cargos) AS costo,

            SUM(oc.total_descuentos) AS descuento,

            SUM(oc.cantidad) AS cantidad_productos,

            COUNT(DISTINCT oc.id_item) AS cantidad_items,

            MAX(oc.monto_total_oc_pesos_chilenos)

                AS monto_orden_compra,

            COUNT(DISTINCT oc.monto_total_oc_pesos_chilenos)

                AS totales_oc_distintos

        FROM staging.ordenes_compra_limpias oc

        WHERE oc.codigo IS NOT NULL

          AND oc.fecha_envio IS NOT NULL

          AND oc.codigo_producto_onu IS NOT NULL

          AND oc.codigo_proveedor IS NOT NULL

          AND oc.codigo_organismo_publico IS NOT NULL

          AND oc.codigo_tipo IS NOT NULL

        GROUP BY

            oc.codigo,

            oc.fecha_envio,

            oc.codigo_producto_onu,

            oc.codigo_proveedor,

            oc.codigo_organismo_publico,

            oc.codigo_tipo

        ORDER BY

            oc.fecha_envio,

            oc.codigo,

            oc.codigo_producto_onu,

            oc.codigo_proveedor,

            oc.codigo_organismo_publico,

            oc.codigo_tipo

        """,

    )

    if not rows:

        raise ValueError(

            "No hay hechos extraídos. Se conserva la tabla destino."

        )

    LOGGER.info("Grupos OC/producto extraídos: %s", len(rows))

    # Si ya existe una transacción, crea un savepoint.

    # El ETL principal deberá confirmar la transacción externa.

    with target.transaction():

        fechas = {

            row["fecha"]: row["fecha_key"]

            for row in fetch_all(

                target,

                "SELECT fecha, fecha_key FROM dw.dim_fecha",

            )

        }

        productos = {

            str(row["producto_codigo"]).strip(): row["producto_key"]

            for row in fetch_all(

                target,

                """

                SELECT producto_codigo, producto_key

                FROM dw.dim_producto

                """,

            )

        }

        proveedores = {

            row["proveedor_codigo"]: row["proveedor_key"]

            for row in fetch_all(

                target,

                """

                SELECT proveedor_codigo, proveedor_key

                FROM dw.dim_proveedor

                """,

            )

        }

        compradores = {

            row["comprador_organismo_codigo"]: row["comprador_key"]

            for row in fetch_all(

                target,

                """

                SELECT comprador_organismo_codigo, comprador_key

                FROM dw.dim_comprador

                """,

            )

        }

        tipos_oc = {

            row["codigo_tipo"]: row["tipo_orden_compra_key"]

            for row in fetch_all(

                target,

                """

                SELECT codigo_tipo, tipo_orden_compra_key

                FROM dw.dim_tipo_orden_compra

                """,

            )

        }

        values = []

        vistos = {}

        conflictos = 0

        dimensiones_faltantes = 0

        for row in rows:

            pk = (

                fechas.get(row["fecha"]),

                proveedores.get(row["codigo_proveedor"]),

                compradores.get(row["codigo_organismo_publico"]),

                tipos_oc.get(row["codigo_tipo"]),

                productos.get(

                    str(row["codigo_producto_onu"]).strip()

                ),

            )

            if any(key is None for key in pk):

                dimensiones_faltantes += 1

                LOGGER.warning(

                    "Dimensión no encontrada: "

                    "OC=%s, fecha=%s, producto=%s, "

                    "proveedor=%s, organismo=%s, tipo=%s, "

                    "claves=%s",

                    row["codigo_orden"],

                    row["fecha"],

                    row["codigo_producto_onu"],

                    row["codigo_proveedor"],

                    row["codigo_organismo_publico"],

                    row["codigo_tipo"],

                    pk,

                )

                continue

            if row["totales_oc_distintos"] > 1:

                raise ValueError(

                    f"OC {row['codigo_orden']}: "

                    "totales de cabecera inconsistentes."

                )

            if row["monto_orden_compra"] is None:

                raise ValueError(

                    f"OC {row['codigo_orden']}: "

                    "falta el monto total de la OC en CLP."

                )

            if pk in vistos:
                conflictos += 1

                LOGGER.warning(
                    "CONFLICTO PK | "
                    "Fecha: fecha_key=%s, fecha=%s | "
                    "Proveedor: proveedor_key=%s, codigo_proveedor=%s | "
                    "Comprador: comprador_key=%s, codigo_organismo=%s | "
                    "Tipo OC: tipo_orden_compra_key=%s, codigo_tipo=%s | "
                    "Producto: producto_key=%s, codigo_producto_onu=%s | "
                    "OC anterior=%s | OC entrante=%s | "
                    "Acción: DO UPDATE reemplaza las medidas anteriores; no suma.",
                    pk[0],
                    row["fecha"],
                    pk[1],
                    row["codigo_proveedor"],
                    pk[2],
                    row["codigo_organismo_publico"],
                    pk[3],
                    row["codigo_tipo"],
                    pk[4],
                    row["codigo_producto_onu"],
                    vistos[pk],
                    row["codigo_orden"],
                )

            vistos[pk] = row["codigo_orden"]

            monto_neto = row["monto_neto"] or Decimal(0)

            impuesto = row["impuesto"] or Decimal(0)

            costo = row["costo"] or Decimal(0)

            descuento = row["descuento"] or Decimal(0)

            cantidad_productos = (

                row["cantidad_productos"] or Decimal(0)

            )

            cantidad_items = row["cantidad_items"] or 0

            with localcontext() as context:

                context.prec = 60

                monto_bruto = (

                    monto_neto

                    + impuesto

                    + costo

                    - descuento

                )

                precio_unitario = (

                    monto_bruto / cantidad_productos

                    if cantidad_productos > 0

                    else Decimal(0)

                )

            values.append(

                (

                    *pk,

                    monto_neto,

                    impuesto,

                    monto_bruto,

                    cantidad_productos,

                    precio_unitario,

                    1,

                    cantidad_items,

                    costo,

                    descuento,

                    row["monto_orden_compra"],

                )

            )

        # Cancela antes de borrar si faltan dimensiones.

        if dimensiones_faltantes:

            raise ValueError(

                f"{dimensiones_faltantes} grupos sin dimensiones. "

                "Se cancela la recarga para evitar una carga parcial."

            )

        statement = """

            INSERT INTO dw.fact_orden_compra (

                fecha_key,

                proveedor_key,

                comprador_key,

                tipo_orden_compra_key,

                producto_key,

                monto_neto,

                impuesto,

                monto_bruto,

                cantidad_productos,

                precio_unitario,

                cantidad_ordenes_compra,

                cantidad_items,

                costo,

                descuento,

                monto_orden_compra

            )

            VALUES (

                %s, %s, %s, %s, %s,

                %s, %s, %s, %s, %s,

                %s, %s, %s, %s, %s

            )

            ON CONFLICT (

                fecha_key,

                proveedor_key,

                comprador_key,

                tipo_orden_compra_key,

                producto_key

            )

            DO UPDATE SET

                monto_neto = EXCLUDED.monto_neto,

                impuesto = EXCLUDED.impuesto,

                monto_bruto = EXCLUDED.monto_bruto,

                cantidad_productos = EXCLUDED.cantidad_productos,

                precio_unitario = EXCLUDED.precio_unitario,

                cantidad_ordenes_compra =

                    EXCLUDED.cantidad_ordenes_compra,

                cantidad_items = EXCLUDED.cantidad_items,

                costo = EXCLUDED.costo,

                descuento = EXCLUDED.descuento,

                monto_orden_compra = EXCLUDED.monto_orden_compra

        """

        with target.cursor() as cursor:

            cursor.execute(

                """

                LOCK TABLE dw.fact_orden_compra

                IN SHARE ROW EXCLUSIVE MODE

                """

            )

            cursor.execute("DELETE FROM dw.fact_orden_compra")

        # executemany no confirma la transacción por su cuenta.

        with target.cursor() as cursor:

            for batch in chunks(values):

                cursor.executemany(statement, batch)

        with target.cursor() as cursor:

            cursor.execute(

                """

                SELECT COUNT(*) AS total

                FROM dw.fact_orden_compra

                """

            )

            resultado = cursor.fetchone()

            filas_finales = (

                resultado["total"]

                if isinstance(resultado, dict)

                else resultado[0]

            )

        LOGGER.info(

            "Recarga ejecutada: grupos=%s, "

            "conflictos PK=%s, filas finales=%s",

            len(values),

            conflictos,

            filas_finales,

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

                load_fecha(source, target)

                load_products(source, target)

                load_providers(source, target)

                load_buyers(source, target)

                load_tipo_orden_compra(source, target)

                load_fact_orden_compra(source, target)

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
