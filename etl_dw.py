"""ETL dimensional SmartBids (reconstrucción anual 2024).

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


def load_fact_orden_compra(source, target):
    """Reconstruye hechos agregados 2024 con importes ya conciliados en staging.

    Conserva filas de otros años. No se agregan totales de cabecera repetidos.
    """
    from decimal import Decimal, localcontext

    campos = (
        "neto_item_ajustado_clp", "impuesto_asignado_clp",
        "cargo_oc_asignado_clp", "descuento_oc_asignado_clp",
        "monto_bruto_asignado_clp", "cantidad",
    )
    LOGGER.info("Extrayendo ítems conciliados de 2024")
    rows = fetch_all(source, """
        SELECT * FROM staging.ordenes_compra_limpias
        WHERE fecha_envio >= DATE '2024-01-01'
          AND fecha_envio < DATE '2025-01-01'
    """)
    if not rows:
        raise ValueError("No hay ítems de 2024: no se modifica el DW.")
    faltantes = set(campos) - set(rows[0])
    if faltantes:
        raise ValueError(f"Staging no contiene las columnas de asignación: {sorted(faltantes)}")

    def mapa(query, codigo, key):
        result = {}
        for r in fetch_all(target, query):
            if r[codigo] in result:
                raise ValueError(f"Dimensión ambigua para {codigo}={r[codigo]}")
            result[r[codigo]] = r[key]
        return result

    fechas = mapa("SELECT fecha, fecha_key FROM dw.dim_fecha", "fecha", "fecha_key")
    proveedores = mapa("SELECT proveedor_codigo, proveedor_key FROM dw.dim_proveedor", "proveedor_codigo", "proveedor_key")
    compradores = mapa("SELECT comprador_organismo_codigo, comprador_key FROM dw.dim_comprador", "comprador_organismo_codigo", "comprador_key")
    tipos = mapa("SELECT codigo_tipo, tipo_orden_compra_key FROM dw.dim_tipo_orden_compra", "codigo_tipo", "tipo_orden_compra_key")
    productos = mapa("SELECT producto_codigo, producto_key FROM dw.dim_producto", "producto_codigo", "producto_key")

    grupos = {}
    vistos = set()
    codigos_oc = set()
    total_por_oc = {}
    with localcontext() as ctx:
        ctx.prec = 80
        for numero_fila, r in enumerate(rows, 1):
            identidad = (r["codigo"], r["id_item"])
            if not all(identidad) or identidad in vistos:
                raise ValueError(f"Ítem duplicado o sin identificación: {identidad}")
            vistos.add(identidad)
            codigos_oc.add(r["codigo"])
            total = r["monto_total_oc_pesos_chilenos"]
            if total is None:
                raise ValueError(f"OC sin total CLP: {r['codigo']}")
            if r["codigo"] in total_por_oc and total_por_oc[r["codigo"]] != total:
                raise ValueError(f"Total de cabecera inconsistente: {r['codigo']}")
            total_por_oc[r["codigo"]] = total
            pk = (
                fechas.get(r["fecha_envio"]),
                proveedores.get(r["codigo_proveedor"]),
                compradores.get(r["codigo_organismo_publico"]),
                tipos.get(r["codigo_tipo"]),
                productos.get(str(r["codigo_producto_onu"]).strip()),
            )
            if any(k is None for k in pk):
                raise ValueError(f"Dimensión faltante: OC={r['codigo']} item={r['id_item']} claves={pk}")
            if any(r[c] is None for c in campos):
                raise ValueError(f"Medidas incompletas: OC={r['codigo']} item={r['id_item']}")
            g = grupos.setdefault(pk, {"medidas": [Decimal(0)] * 6, "ocs": set(), "items": 0})
            for i, campo in enumerate(campos):
                g["medidas"][i] += r[campo]
            g["ocs"].add(r["codigo"])
            g["items"] += 1
            if numero_fila % 50000 == 0:
                LOGGER.info("Agrupados %s/%s ítems; grupos=%s", numero_fila, len(rows), len(grupos))

        # No se redondean los montos por grupo; se conserva la precisión de staging.
        values = []
        for pk, g in grupos.items():
            neto, impuesto, costo, descuento, bruto, cantidad = g["medidas"]
            if abs(bruto - (neto + impuesto + costo - descuento)) > Decimal("0.01"):
                raise ValueError(f"Bruto no concilia en grupo {pk}")
            precio = bruto / cantidad if cantidad > 0 else Decimal(0)
            values.append((*pk, neto, impuesto, bruto, cantidad, precio,
                           len(g["ocs"]), g["items"], costo, descuento))

    # La suma de bruto por ítem debe coincidir con cada total único de OC.
    # Esta validación fue realizada en staging; se repite globalmente antes del reemplazo.
    monto_2024 = sum(total_por_oc.values(), Decimal(0))
    bruto_2024 = sum((r["monto_bruto_asignado_clp"] for r in rows), Decimal(0))
    if abs(monto_2024 - bruto_2024) > Decimal("0.01"):
        raise ValueError(f"Monto 2024 no concilia: cabeceras={monto_2024}, ítems={bruto_2024}")
    LOGGER.info("2024: OC únicas=%s, ítems=%s, grupos=%s, total CLP=%s",
                len(codigos_oc), len(rows), len(values), monto_2024)

    columnas = "fecha_key,proveedor_key,comprador_key,tipo_orden_compra_key,producto_key,monto_neto,impuesto,monto_bruto,cantidad_productos,precio_unitario,cantidad_ordenes_compra,cantidad_items,costo,descuento"
    with target.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout='10s'")
        cur.execute("ALTER TABLE dw.fact_orden_compra ADD COLUMN IF NOT EXISTS total_oc_procesadas bigint")
        cur.execute("ALTER TABLE dw.fact_orden_compra ADD COLUMN IF NOT EXISTS monto_total_procesado_clp numeric")
        cur.execute("LOCK TABLE dw.fact_orden_compra IN SHARE ROW EXCLUSIVE MODE")
        cur.execute("""
            DELETE FROM dw.fact_orden_compra f USING dw.dim_fecha d
            WHERE f.fecha_key = d.fecha_key
              AND d.fecha >= DATE '2024-01-01' AND d.fecha < DATE '2025-01-01'
        """)

    statement = f"INSERT INTO dw.fact_orden_compra ({columnas}) VALUES ({','.join(['%s'] * 14)})"
    for n, batch in enumerate(chunks(values), 1):
        execute_many(target, statement, batch)
        LOGGER.info("Hechos 2024 insertados: lote %s", n)

    with target.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*), COUNT(*) FILTER (WHERE f.precio_unitario IS NULL)
            FROM dw.fact_orden_compra f JOIN dw.dim_fecha d USING (fecha_key)
            WHERE d.fecha >= DATE '2024-01-01' AND d.fecha < DATE '2025-01-01'
        """)
        total, nulos = cur.fetchone()
        if total != len(values) or nulos:
            raise ValueError(f"Verificación fallida: filas={total}, esperadas={len(values)}, precios nulos={nulos}")
    actualizar_kpi_globales(target, len(codigos_oc), monto_2024)


def actualizar_kpi_globales(target, ocs_2024, monto_2024):
    """Escribe KPI histórico SOLO si se conocen también los de otros años.

    Una tabla agregada por producto NO permite reconstruir OC únicas globales
    sumando cantidad_ordenes_compra. Nunca se inventan esos KPI.
    """
    from decimal import Decimal, InvalidOperation
    with target.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM dw.fact_orden_compra f
            JOIN dw.dim_fecha d USING (fecha_key)
            WHERE d.fecha < DATE '2024-01-01' OR d.fecha >= DATE '2025-01-01'
        """)
        filas_otros = cur.fetchone()[0]
        if filas_otros:
            texto_ocs = os.getenv("DW_OTROS_ANIOS_OC_UNICAS")
            texto_monto = os.getenv("DW_OTROS_ANIOS_MONTO_TOTAL_CLP")
            if not texto_ocs or not texto_monto:
                raise ValueError(
                    "Existen hechos de otros años, pero el conteo único de OC y su monto total "
                    "no se pueden reconstruir de la tabla agregada. Configura "
                    "DW_OTROS_ANIOS_OC_UNICAS y DW_OTROS_ANIOS_MONTO_TOTAL_CLP en .env "
                    "con totales oficiales deduplicados de los otros años; no se aplicó la carga."
                )
            try:
                ocs_otros = int(texto_ocs)
                monto_otros = Decimal(texto_monto)
            except (ValueError, InvalidOperation) as exc:
                raise ValueError("KPI de otros años inválidos en .env") from exc
            if ocs_otros < 0 or not monto_otros.is_finite() or monto_otros < 0:
                raise ValueError("KPI de otros años negativos o no finitos")
            LOGGER.warning("KPI otros años provienen del .env: verificar unicidad entre fuentes/años")
        else:
            ocs_otros, monto_otros = 0, Decimal(0)
        total_ocs = ocs_2024 + ocs_otros
        total_monto = monto_2024 + monto_otros
        cur.execute("""
            UPDATE dw.fact_orden_compra
            SET total_oc_procesadas = %s,
                monto_total_procesado_clp = %s
        """, (total_ocs, total_monto))
    LOGGER.info("KPI globales: %s OC únicas; monto CLP=%s", total_ocs, total_monto)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="ETL dimensional SmartBids: simulación por defecto; --aplicar confirma cambios."
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
                # Esta dimensión utiliza un catálogo local; no necesita conexión de origen.
                load_tipo_orden_compra(target, target)
            else:
                with psycopg.connect(**connection_kwargs(source_db)) as source:
                    load_fecha(source, target)
                    load_products(source, target)
                    load_providers(source, target)
                    load_buyers(source, target)
                    load_tipo_orden_compra(source, target)
                    load_fact_orden_compra(source, target)
                if args.aplicar:
                    target.commit()
                    LOGGER.info("MODO APLICAR: transacción confirmada en %s", target_db)
                else:
                    target.rollback()
                    LOGGER.info("MODO SIMULACIÓN: validaciones completadas y cambios revertidos en %s", target_db)
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
