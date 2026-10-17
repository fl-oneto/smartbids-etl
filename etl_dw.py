"""ETL dimensional SmartBids (reconstrucción completa 2024-2026).

Carga dimensiones antes de los hechos y usa UPSERT para permitir reejecuciones.

Las credenciales se leen desde variables de entorno o un archivo .env.

"""

from __future__ import annotations
import argparse
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
    warnings_file = log_dir / (f"warnings_dw_{datetime.now():%Y%m%d_%H%M%S_%f}.log")
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
        log_file,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
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
            NULLIF(BTRIM(p.glosa_nivel4), '') AS producto_nivel4,
            um.nombre_unidad_medida
        FROM catalog.producto p
        LEFT JOIN catalog.unidad_medida um
            ON um.codigo_unidad_medida = p.codigo_unidad_medida
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
            producto_nivel4,
            nombre_unidad_medida
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (producto_codigo) DO UPDATE SET
            producto_descripcion = EXCLUDED.producto_descripcion,
            producto_grupo = EXCLUDED.producto_grupo,
            producto_nivel1 = EXCLUDED.producto_nivel1,
            producto_nivel2 = EXCLUDED.producto_nivel2,
            producto_nivel3 = EXCLUDED.producto_nivel3,
            producto_nivel4 = EXCLUDED.producto_nivel4,
            nombre_unidad_medida = EXCLUDED.nombre_unidad_medida
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
            row["nombre_unidad_medida"],
        )
        for row in rows
    ]
    sin_unidad = sum(not row["nombre_unidad_medida"] for row in rows)
    if sin_unidad:
        LOGGER.warning("Productos sin unidad en el catálogo: %s", sin_unidad)
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
        (10, "TD", "Trato Directo"),
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



# Los ítems provienen de una sola tabla staging conciliada (CSV 2024 + Feather 2025/26).
# La transferencia entre bases es por streaming COPY; ninguna fila se carga en listas Python.
STAGING_COLS = (
    "codigo", "id_item", "fecha_envio", "codigo_producto_onu",
    "codigo_proveedor", "codigo_organismo_publico", "codigo_tipo",
    "cantidad", "neto_item_ajustado_clp", "impuesto_asignado_clp",
    "cargo_oc_asignado_clp", "descuento_oc_asignado_clp",
    "monto_bruto_asignado_clp", "monto_total_oc_pesos_chilenos",
)


def reconstruir_dw(source, target):
    """Reemplaza atomicamente 5 dimensiones y el hecho desde staging consolidado.

    Cualquier error, inclusive en simulacion, produce ROLLBACK de toda la transaccion.
    TRUNCATE no utiliza CASCADE: dependencias inesperadas deben fallar de forma segura.
    """
    from decimal import Decimal
    from psycopg import sql

    tablas = (
        "fact_orden_compra", "dim_fecha", "dim_producto", "dim_proveedor",
        "dim_comprador", "dim_tipo_orden_compra",
    )
    with target.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout = '15s'")
        cur.execute("SET LOCAL statement_timeout = 0")
        cur.execute("ALTER TABLE dw.fact_orden_compra ADD COLUMN IF NOT EXISTS total_oc_procesadas bigint")
        cur.execute("ALTER TABLE dw.fact_orden_compra ADD COLUMN IF NOT EXISTS monto_total_procesado_clp numeric")
        cur.execute("TRUNCATE " + ", ".join("dw." + t for t in tablas) + " RESTART IDENTITY")
        LOGGER.info("Seis tablas DW truncadas dentro de la transacción (pendiente de COMMIT)")

    # Reutiliza las transformaciones existentes de catalogos para las dimensiones.
    load_fecha(source, target)
    load_products(source, target)
    load_providers(source, target)
    load_buyers(source, target)
    load_tipo_orden_compra(source, target)

    with target.cursor() as cur:
        cur.execute("""CREATE TEMP TABLE tmp_oc_items (
            codigo text, id_item text, fecha_envio date,
            codigo_producto_onu text, codigo_proveedor bigint,
            codigo_organismo_publico bigint, codigo_tipo integer,
            cantidad numeric, neto_item_ajustado_clp numeric,
            impuesto_asignado_clp numeric, cargo_oc_asignado_clp numeric,
            descuento_oc_asignado_clp numeric, monto_bruto_asignado_clp numeric,
            monto_total_oc_pesos_chilenos numeric
        ) ON COMMIT DROP""")

    cols = sql.SQL(", ").join(map(sql.Identifier, STAGING_COLS))
    # COPY FROM STDIN con bytes tal como salen de COPY TO STDOUT.
    # El staging es solo lectura: source se revierte al final (no muta la fuente).
    exportar = sql.SQL("COPY (SELECT {} FROM staging.ordenes_compra_limpias) TO STDOUT WITH (FORMAT CSV, NULL '\\N')").format(cols)
    importar = sql.SQL("COPY tmp_oc_items ({}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')").format(cols)
    LOGGER.info("Copiando ítems de staging unificado a tabla temporal en %s", target.info.dbname)
    with source.cursor() as cs, target.cursor() as ct:
        with cs.copy(exportar) as lectura, ct.copy(importar) as escritura:
            for bloque in lectura:
                escritura.write(bloque)
    with target.cursor() as cur:
        cur.execute("SELECT COUNT(*), COUNT(DISTINCT codigo) FROM tmp_oc_items")
        n_items, n_oc = cur.fetchone()
        LOGGER.info("Staging transferido: %s ítems, %s OC únicas", n_items, n_oc)
        if n_items == 0 or n_oc == 0:
            raise ValueError("Staging vacío: se revierte la reconstrucción")
        # Valida presencia de dimensiones antes de cualquier agregación.
        cur.execute("""SELECT COUNT(*) FROM tmp_oc_items s
            LEFT JOIN dw.dim_fecha d ON d.fecha=s.fecha_envio
            LEFT JOIN dw.dim_proveedor p ON p.proveedor_codigo=s.codigo_proveedor
            LEFT JOIN dw.dim_comprador c ON c.comprador_organismo_codigo=s.codigo_organismo_publico
            LEFT JOIN dw.dim_tipo_orden_compra t ON t.codigo_tipo=s.codigo_tipo
            LEFT JOIN dw.dim_producto pr ON pr.producto_codigo=BTRIM(s.codigo_producto_onu)
            WHERE d.fecha_key IS NULL OR p.proveedor_key IS NULL OR c.comprador_key IS NULL
               OR t.tipo_orden_compra_key IS NULL OR pr.producto_key IS NULL""")
        faltantes = cur.fetchone()[0]
        if faltantes:
            raise ValueError(f"{faltantes} ítems del staging sin dimensión: cambios revertidos")
        cur.execute("""SELECT COUNT(*) FROM tmp_oc_items WHERE codigo IS NULL OR id_item IS NULL
            OR fecha_envio IS NULL OR monto_total_oc_pesos_chilenos IS NULL
            OR cantidad IS NULL OR neto_item_ajustado_clp IS NULL OR impuesto_asignado_clp IS NULL
            OR cargo_oc_asignado_clp IS NULL OR descuento_oc_asignado_clp IS NULL
            OR monto_bruto_asignado_clp IS NULL""")
        nulos = cur.fetchone()[0]
        if nulos:
            raise ValueError(f"{nulos} ítems con campos obligatorios nulos")
        cur.execute("""SELECT COUNT(*) FROM (
            SELECT codigo, id_item FROM tmp_oc_items GROUP BY codigo, id_item HAVING COUNT(*)>1
        ) duplicados""")
        duplicados = cur.fetchone()[0]
        if duplicados:
            raise ValueError(f"{duplicados} códigos OC+ítem duplicados; cambios revertidos")
        cur.execute("""CREATE TEMP TABLE tmp_oc_totales ON COMMIT DROP AS
            SELECT codigo, MIN(fecha_envio) fecha_min, MAX(fecha_envio) fecha_max,
                   MIN(monto_total_oc_pesos_chilenos) total_min,
                   MAX(monto_total_oc_pesos_chilenos) total_max,
                   SUM(monto_bruto_asignado_clp) bruto_items,
                   COUNT(*) cantidad_items
            FROM tmp_oc_items GROUP BY codigo""")
        cur.execute("""SELECT COUNT(*) FROM tmp_oc_totales
            WHERE fecha_min<>fecha_max OR ABS(total_max-total_min)>0.01
               OR ABS(bruto_items-total_min)>0.01""")
        descuadres = cur.fetchone()[0]
        if descuadres:
            raise ValueError(f"{descuadres} OC no concilian ítems, cabeceras o fechas")
        cur.execute("""SELECT COUNT(*), SUM(total_min) FROM tmp_oc_totales""")
        ocs, monto = cur.fetchone()
        if ocs != n_oc:
            raise ValueError("Conteo de OC inconsistente")
        # Agregación SQL exacta por la PK de hechos, CON DISTINCT OC por grupo.
        # Las cifras monetarias son NUMERIC, no float; no redondea al agrupar.
        LOGGER.info("Agregando ítems por fecha/proveedor/comprador/tipo/producto en PostgreSQL")
        cur.execute("""INSERT INTO dw.fact_orden_compra (
                fecha_key, proveedor_key, comprador_key, tipo_orden_compra_key, producto_key,
                monto_neto, impuesto, monto_bruto, cantidad_productos, precio_unitario,
                cantidad_ordenes_compra, cantidad_items, costo, descuento
            )
            SELECT d.fecha_key, p.proveedor_key, c.comprador_key,
                   t.tipo_orden_compra_key, pr.producto_key,
                   SUM(s.neto_item_ajustado_clp), SUM(s.impuesto_asignado_clp),
                   SUM(s.monto_bruto_asignado_clp), SUM(s.cantidad),
                   CASE WHEN SUM(s.cantidad)>0 THEN SUM(s.monto_bruto_asignado_clp)/SUM(s.cantidad)
                        ELSE 0 END,
                   COUNT(DISTINCT s.codigo), COUNT(*),
                   SUM(s.cargo_oc_asignado_clp), SUM(s.descuento_oc_asignado_clp)
            FROM tmp_oc_items s
            JOIN dw.dim_fecha d ON d.fecha=s.fecha_envio
            JOIN dw.dim_proveedor p ON p.proveedor_codigo=s.codigo_proveedor
            JOIN dw.dim_comprador c ON c.comprador_organismo_codigo=s.codigo_organismo_publico
            JOIN dw.dim_tipo_orden_compra t ON t.codigo_tipo=s.codigo_tipo
            JOIN dw.dim_producto pr ON pr.producto_codigo=BTRIM(s.codigo_producto_onu)
            GROUP BY d.fecha_key, p.proveedor_key, c.comprador_key,
                     t.tipo_orden_compra_key, pr.producto_key""")
        cur.execute("""SELECT COUNT(*), SUM(cantidad_items), SUM(monto_bruto),
                     COUNT(*) FILTER (WHERE precio_unitario IS NULL),
                     COUNT(*) FILTER (WHERE ABS(monto_bruto-(monto_neto+impuesto+costo-descuento))>0.01)
            FROM dw.fact_orden_compra""")
        grupos, items_dw, bruto_dw, precios_nulos, grupos_incorrectos = cur.fetchone()
        if items_dw != n_items or abs(bruto_dw-monto)>Decimal("0.01") or precios_nulos or grupos_incorrectos:
            raise ValueError(f"Validación DW falló: items={items_dw}/{n_items}, bruto={bruto_dw}/{monto}, "
                             f"precios_nulos={precios_nulos}, grupos_incorrectos={grupos_incorrectos}")
        # KPI global = OC únicas, no suma de cantidad_ordenes_compra por grupos.
        cur.execute("""UPDATE dw.fact_orden_compra
            SET total_oc_procesadas=%s, monto_total_procesado_clp=%s""", (ocs, monto))
        cur.execute("""SELECT EXTRACT(YEAR FROM d.fecha)::int, COUNT(*), SUM(f.cantidad_items),
                   SUM(f.monto_bruto) FROM dw.fact_orden_compra f
                   JOIN dw.dim_fecha d ON d.fecha_key=f.fecha_key
                   GROUP BY 1 ORDER BY 1""")
        for anio, n_grupos, n_items_anio, bruto in cur.fetchall():
            LOGGER.info("DW año %s: %s grupos, %s ítems, bruto CLP=%s", anio, n_grupos, n_items_anio, bruto)
        LOGGER.info("DW validado: %s grupos, %s OC únicas, %s ítems; monto CLP=%s",
                    grupos, ocs, n_items, monto)

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reconstrucción total DW desde staging consolidado; simula salvo --aplicar."
    )
    parser.add_argument(
        "--aplicar", action="store_true",
        help="Confirma los cambios en PostgreSQL; sin esta opción se revierten."
    )
    parser.add_argument(
        "--solo-tipos", action="store_true",
        help="Carga únicamente dim_tipo_orden_compra; no modifica los hechos."
    )
    args = parser.parse_args()
    load_dotenv()
    log_file = configure_logging()
    source_db = os.getenv("DB_ORIGEN", "smartbids")
    target_db = os.getenv("DB_DESTINO", "smartbids_dw")
    started_at = time.monotonic()
    LOGGER.info("Iniciando ETL %s -> %s | modo=%s | log=%s", source_db, target_db, "APLICAR" if args.aplicar else "SIMULACIÓN", log_file)
    try:
        with psycopg.connect(**connection_kwargs(target_db)) as target:
            if args.solo_tipos:
                load_tipo_orden_compra(target, target)
            else:
                with psycopg.connect(**connection_kwargs(source_db)) as source:
                    reconstruir_dw(source, target)
            if args.aplicar:
                target.commit()
                LOGGER.info("MODO APLICAR: transacción confirmada en %s", target_db)
            else:
                target.rollback()
                LOGGER.info("MODO SIMULACIÓN: cambios revertidos en %s", target_db)
        LOGGER.info(
            "ETL %s: dimensiones, hechos agrupados y KPI globales | duración=%.2f segundos",
            "confirmado" if args.aplicar else "simulado sin persistir",
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
