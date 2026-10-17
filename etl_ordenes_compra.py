"""CSV 2024, Feather 2025-2026 y consolidación -> staging único.
Python 3.10+, psycopg 3, python-dotenv. Solo usa DB_ORIGEN.
CSV reemplaza únicamente 2024 en staging. Feather actualiza su staging de
validación y consolida 2025-2026 en staging.ordenes_compra_limpias.
--origen consolidar reutiliza el Feather ya cargado, sin reprocesarlo.
NO carga hechos al DW.
Simula por defecto. --aplicar confirma. CSV mantiene su protección contra
rechazos; Feather excluye órdenes completas no elegibles e informa el motivo.
No modifica catálogos ni crea una segunda tabla de hechos.
"""
import argparse
import csv
import json
import math
import os
import re
import tempfile
from collections import Counter
from datetime import datetime
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
REQUERIDAS = ['Codigo', 'CodigoLicitacion', 'IDItem', 'FechaEnvio',
              'codigoProductoONU', 'CodigoProveedor', 'CodigoOrganismoPublico',
              'CodigoTipo', 'cantidad', 'totalLineaNeto', 'totalImpuestos',
              'totalCargos', 'totalDescuentos', 'monedaItem', 'TipoMonedaOC',
              'MontoTotalOC', 'MontoTotalOC_PesosChilenos', 'TotalNetoOC',
              'Impuestos', 'Cargos', 'Descuentos']
FECHAS = [('FechaCreacion', 'fecha_creacion'), ('FechaEnvio', 'fecha_envio'),
          ('FechaSolicitudCancelacion', 'fecha_solicitud_cancelacion'),
          ('fechaUltimaModificacion', 'fecha_ultima_modificacion'),
          ('FechaAceptacion', 'fecha_aceptacion'), ('FechaCancelacion', 'fecha_cancelacion')]
LIMPIAS = [('fila_csv', 'bigint'), ('codigo', 'text'), ('id_item', 'text'),
           ('codigo_licitacion', 'text')] + [(n, 'date') for _, n in FECHAS] + [
           ('codigo_producto_onu', 'text'), ('codigo_proveedor', 'bigint'),
           ('codigo_organismo_publico', 'bigint'), ('codigo_tipo', 'integer'),
           ('cantidad', 'numeric'), ('precio_neto', 'numeric'),
           ('total_linea_neto', 'numeric'), ('total_impuestos', 'numeric'),
           ('total_cargos', 'numeric'), ('total_descuentos', 'numeric'),
           ('moneda_item', 'text'), ('moneda_oc', 'text'), ('estado', 'text')]
ORIGINALES = [('precio_neto_original','numeric'),('total_linea_neto_original','numeric'),
              ('total_impuestos_original','numeric'),('total_cargos_original','numeric'),
              ('total_descuentos_original','numeric'),('moneda_original','text'),
              ('tasa_clp','numeric'),('fecha_tasa','date'),('fuente_tasa','text')]
LIMPIAS += ORIGINALES
LIMPIAS += [
    ('neto_item_ajustado_original', 'numeric'),
    ('neto_item_ajustado_clp', 'numeric'),
    ('impuesto_oc_original', 'numeric'),
    ('impuesto_oc_clp', 'numeric'),
    ('impuesto_asignado_clp', 'numeric'),
    ('descuento_oc_original', 'numeric'),
    ('descuento_oc_clp', 'numeric'),
    ('descuento_oc_asignado_clp', 'numeric'),
    ('cargo_oc_original', 'numeric'),
    ('cargo_oc_clp', 'numeric'),
    ('cargo_oc_asignado_clp', 'numeric'),
    ('monto_bruto_asignado_clp', 'numeric'),
    ('tipo_impuesto_oc', 'text'),
    ('monto_total_oc_pesos_chilenos', 'numeric'),
            ('monto_total_oc_original', 'numeric'),
            ('monto_total_oc_clp_fuente', 'numeric'),
            ('moneda_item_declarada', 'text'),
            ('moneda_oc_declarada', 'text'),
            ('moneda_ajustada', 'boolean')]
# La transformacion interna conserva su orden; esta proyeccion define
# exclusivamente las columnas persistidas en la tabla limpia.
QUITAR = {'fila_csv', 'codigo_licitacion', 'fecha_creacion',
          'fecha_solicitud_cancelacion', 'fecha_ultima_modificacion',
          'fecha_aceptacion', 'fecha_cancelacion', 'precio_neto',
          'moneda_item', 'moneda_oc', 'estado', 'precio_neto_original',
          'fuente_tasa'}
INDICES_GUARDAR = [i for i,(c,_) in enumerate(LIMPIAS) if c not in QUITAR]
COLUMNAS_GUARDAR = [LIMPIAS[i] for i in INDICES_GUARDAR]
NULOS = {'', 'na', 'n/a', 'null', 'none', 'nan', 's/i'}
def texto(v):
    t = (v or '').strip()
    return None if t.casefold() in NULOS else t
def requerido(v, campo):
    t = texto(v)
    if t is None:
        raise ValueError(f'{campo}: valor obligatorio ausente')
    return t
def entero(v, campo, bits=64):
    t = requerido(v, campo)
    if not re.fullmatch(r'[0-9]+', t):
        raise ValueError(f'{campo}: codigo entero invalido ({t})')
    n = int(t)
    if n > 2**(bits-1)-1:
        raise ValueError(f'{campo}: fuera de rango')
    return n
def numero(v, campo, config, cero_si_vacio=False):
    t = texto(v)
    if t is None:
        if cero_si_vacio:
            return Decimal(0)
        raise ValueError(f'{campo}: numero ausente')
    decimal = config.get('OC_DECIMAL', '.')
    miles = config.get('OC_MILES', '') or ''
    patron_entero = r'[0-9]+'
    if miles:
        patron_entero = r'(?:[0-9]+|[0-9]{1,3}(?:' + re.escape(miles) + r'[0-9]{3})+)'
    patron = r'[+-]?' + patron_entero + '(?:' + re.escape(decimal) + r'[0-9]+)?(?:[eE][+-]?[0-9]+)?'
    if not re.fullmatch(patron, t):
        raise ValueError(f'{campo}: formato numerico invalido ({t}); revisar OC_DECIMAL/OC_MILES')
    try:
        n = Decimal(t.replace(miles, '') .replace(decimal, '.') if miles else t.replace(decimal, '.'))
    except InvalidOperation:
        raise ValueError(f'{campo}: numero invalido') from None
    if not n.is_finite() or abs(n.adjusted())>1000:
        raise ValueError(f'{campo}: fuera del rango numerico permitido')
    return n
def fecha(v, campo, formato=None):
    t = texto(v)
    if t is None:
        return None
    if formato:
        try:
            return datetime.strptime(t, formato).date()
        except ValueError:
            raise ValueError(f'{campo}: no coincide con OC_FECHA_FORMATO ({t})') from None
    try:
        return datetime.fromisoformat(t.replace('Z', '+00:00')).date()
    except ValueError:
        pass
    for f in ['%d/%m/%Y', '%d/%m/%Y %H:%M:%S', '%d/%m/%Y %H:%M', '%d-%m-%Y']:
        try:
            return datetime.strptime(t, f).date()
        except ValueError:
            pass
    raise ValueError(f'{campo}: fecha no reconocida ({t})')
def transformar(f, numero_fila, config):
    codigo = requerido(f['Codigo'], 'Codigo')
    item = requerido(f['IDItem'], 'IDItem')
    licitacion = requerido(f['CodigoLicitacion'], 'CodigoLicitacion')
    fechas = [fecha(f.get(c), c, config.get('OC_FECHA_FORMATO')) for c, _ in FECHAS]
    if fechas[1] is None:
        raise ValueError('FechaEnvio: obligatoria para el DW')
    producto = requerido(f['codigoProductoONU'], 'codigoProductoONU')
    if not re.fullmatch(r'[0-9]+', producto) or int(producto) == 0:
        raise ValueError('codigoProductoONU: debe ser codigo numerico distinto de 0')
    producto = str(int(producto))
    proveedor = entero(f['CodigoProveedor'], 'CodigoProveedor')
    organismo = entero(f['CodigoOrganismoPublico'], 'CodigoOrganismoPublico')
    tipo = entero(f['CodigoTipo'], 'CodigoTipo', 32)
    cantidad = numero(f['cantidad'], 'cantidad', config)
    if cantidad < 0:
        raise ValueError('cantidad: negativa')
    precio = numero(f['precioNeto'], 'precioNeto', config) if texto(f.get('precioNeto')) else None
    neto = numero(f['totalLineaNeto'], 'totalLineaNeto', config)
    # No se asume que un importe vacio equivale a cero.
    importes = [numero(f[c], c, config) for c in ['totalImpuestos', 'totalCargos', 'totalDescuentos']]
    moneda_oc = texto(f.get('TipoMonedaOC'))
    moneda = texto(f.get('monedaItem')) or moneda_oc
    if not moneda:
        raise ValueError('monedaItem/TipoMonedaOC: moneda ausente')
    return (numero_fila, codigo, item, licitacion, *fechas, producto, proveedor,
            organismo, tipo, cantidad, precio, neto, *importes,
            moneda.upper(), moneda_oc.upper() if moneda_oc else None, texto(f.get('Estado')))
def convertir_clp(valores, tasas):
    v=list(valores)
    moneda=v[20]
    if moneda=='CLP':
        factor,fecha_tasa,fuente=Decimal(1),v[5],'identidad'
    else:
        dato=tasas.get((v[5],moneda))
        if dato is None:
            raise ValueError(f'paridad: falta tasa para {v[5]} / {moneda}')
        factor,fecha_tasa,fuente=dato
    originales=v[15:20]
    with localcontext() as ctx:
        ctx.prec=60
        for i in range(15,20):
            v[i]=v[i]*factor if v[i] is not None else None
    v[20]='CLP'
    return tuple(v)+tuple(originales)+(moneda,factor,fecha_tasa,fuente)
def leer_tasas(ruta):
    tasas={}
    if not ruta:
        return tasas
    with Path(ruta).open(encoding='utf-8-sig',newline='') as f:
        for r in csv.DictReader(f):
            fecha_valor=datetime.strptime(r['fecha'],'%Y-%m-%d').date()
            moneda=r['moneda'].strip().upper()
            factor=Decimal(r['tasa_clp'])
            publicada=datetime.strptime(r['fecha_tasa'],'%Y-%m-%d').date()
            if not factor.is_finite() or factor<=0 or publicada>fecha_valor:
                raise ValueError('Paridad invalida')
            clave=(fecha_valor,moneda)
            if clave in tasas:
                raise ValueError(f'Paridad duplicada: {clave}')
            tasas[clave]=(factor,publicada,r['fuente'])
    return tasas
def analizar_oc(archivo, config, delimitador):
    """Primera pasada completa: conciliacion por OC y base de distribucion.

    Importes siempre en moneda de cabecera ORIGINAL. Solo se incluyen filas
    elegibles por CodigoLicitacion, igual que en la segunda pasada.
    """
    grupos = {}
    campos = ('TotalNetoOC', 'Impuestos', 'Cargos', 'Descuentos', 'MontoTotalOC')
    with Path(archivo).open(encoding=config.get('OC_ENCODING', 'utf-8-sig'), newline='') as f:
        for fila in csv.DictReader(f, delimiter=delimitador):
            if not texto(fila.get('CodigoLicitacion')):
                continue
            codigo = texto(fila.get('Codigo'))
            if not codigo:
                continue
            g = grupos.setdefault(codigo, {
                'suma_base': Decimal(0), 'suma_neto': Decimal(0),
                'cabeceras': {k: set() for k in campos},
                'monedas_oc': set(), 'tipos_impuesto': set(),
                'discrepancia': False, 'valido': True, 'filas': 0,
                'error': None,
            })
            g['filas'] += 1
            moneda_oc = texto(fila.get('TipoMonedaOC'))
            moneda_item = texto(fila.get('monedaItem')) or moneda_oc
            g['monedas_oc'].add(moneda_oc.upper() if moneda_oc else None)
            g['tipos_impuesto'].add(texto(fila.get('TipoImpuesto')))
            if moneda_oc and moneda_item and moneda_oc.upper() != moneda_item.upper():
                g['discrepancia'] = True
            try:
                neto = numero(fila.get('totalLineaNeto'), 'totalLineaNeto', config)
                descuento = numero(fila.get('totalDescuentos'), 'totalDescuentos', config)
                cargo = numero(fila.get('totalCargos'), 'totalCargos', config)
                g['suma_neto'] += neto
                base = neto + descuento - cargo
                if base < 0:
                    raise ValueError('base ajustada negativa')
                g['suma_base'] += base
                for campo in campos:
                    g['cabeceras'][campo].add(numero(fila.get(campo), campo, config))
            except ValueError as exc:
                g['valido'] = False
                g['error'] = str(exc)
    for codigo, g in grupos.items():
        if not g['valido'] or len(g['monedas_oc']) != 1 or None in g['monedas_oc']:
            g['valido'] = False
            g['error'] = g['error'] or 'moneda de cabecera inconsistente'
            continue
        if any(len(v) != 1 for v in g['cabeceras'].values()):
            g['valido'] = False
            g['error'] = 'importes de cabecera inconsistentes'
            continue
        g['cabecera'] = {k: next(iter(v)) for k,v in g['cabeceras'].items()}
        c = g['cabecera']
        if abs(g['suma_base'] - c['TotalNetoOC']) > Decimal('0.01'):
            g['valido'] = False
            g['error'] = 'neto ajustado no concilia con cabecera'
        elif abs(c['TotalNetoOC'] + c['Impuestos'] + c['Cargos'] - c['Descuentos'] - c['MontoTotalOC']) > Decimal('0.01'):
            g['valido'] = False
            g['error'] = 'total de cabecera no concilia'
        elif g['suma_base'] == 0 and any(c[x] != 0 for x in ('Impuestos', 'Cargos', 'Descuentos')):
            g['valido'] = False
            g['error'] = 'base cero con ajustes no nulos'
        elif len(g['tipos_impuesto']) != 1:
            g['valido'] = False
            g['error'] = 'tipo impuesto inconsistente dentro de OC'
        g['procesadas'] = 0
        g['acumulados'] = {k: Decimal(0) for k in ('Impuestos', 'Cargos', 'Descuentos')}
    return grupos


def asignar_cabecera(g, base_item):
    """Devuelve asignaciones originales; ultimo item absorbe residuo exacto."""
    g['procesadas'] += 1
    ultimo = g['procesadas'] == g['filas']
    resultado = {}
    with localcontext() as ctx:
        ctx.prec = 60
        for campo in ('Impuestos', 'Cargos', 'Descuentos'):
            importe = g['cabecera'][campo]
            if ultimo:
                asignacion = importe - g['acumulados'][campo]
            elif g['suma_base'] == 0:
                asignacion = Decimal(0)
            else:
                asignacion = importe * base_item / g['suma_base']
            g['acumulados'][campo] += asignacion
            resultado[campo] = asignacion
    return resultado

def ejecutar(config, aplicar=False, permitir_rechazos=False):
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import make_conninfo
    archivo = Path(config['OC_CSV'])
    # Las paridades se leen solo del CSV, nunca de la base de origen.
    tasas=leer_tasas(config.get('OC_PARIDADES_CSV'))
    delimitador = config.get('OC_DELIMITADOR', ',')
    if delimitador == r'\t':
        delimitador = '\t'
    if len(delimitador) != 1:
        raise ValueError('OC_DELIMITADOR debe tener un caracter')
    dec, miles = config.get('OC_DECIMAL', '.'), config.get('OC_MILES', '') or ''
    if dec not in ('.', ',') or (miles and (miles == dec or len(miles) != 1)):
        raise ValueError('Separadores decimal/miles invalidos')
    # Concilia dinamicamente los casos de moneda discrepante antes de transformar.
    grupos_oc = analizar_oc(archivo, config, delimitador)
    resumen = Counter()
    motivos, monedas = Counter(), Counter()
    vistos = set()
    dsn = make_conninfo(host=config['PG_HOST'], port=config.get('PG_PORT', '5432'),
                       dbname=config['DB_ORIGEN'], user=config['PG_USER'],
                       password=config['PG_PASSWORD'], connect_timeout=10)
    with psycopg.connect(dsn) as conn, tempfile.TemporaryDirectory() as carpeta:
        try:
            with conn.cursor() as cur, archivo.open(encoding=config.get('OC_ENCODING', 'utf-8-sig'), newline='') as f:
                lector = csv.DictReader(f, delimiter=delimitador)
                cabecera = lector.fieldnames
                if not cabecera or len(set(cabecera)) != len(cabecera):
                    raise ValueError('CSV sin encabezados o con encabezados duplicados')
                if any(not c or len(c.encode('utf-8')) > 63 for c in cabecera):
                    raise ValueError('Encabezados vacios o demasiado largos')
                faltan = set(REQUERIDAS)-set(cabecera)
                if faltan:
                    raise ValueError('Faltan columnas: ' + ', '.join(sorted(faltan)))
                cur.execute("SET LOCAL lock_timeout = '10s'")
                cur.execute('CREATE SCHEMA IF NOT EXISTS staging')
                # Retira la tabla auxiliar creada por versiones anteriores.
                # Sin CASCADE: dependencias externas provocan rollback.
                cur.execute('DROP TABLE IF EXISTS staging.paridades_moneda')
                definiciones = sql.SQL(', ').join(sql.SQL('{} text').format(sql.Identifier(c)) for c in cabecera)
                cur.execute(sql.SQL('CREATE TABLE IF NOT EXISTS staging.ordenes_compra_raw ({})').format(definiciones))
                cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='staging' AND table_name='ordenes_compra_raw' ORDER BY ordinal_position")
                existentes = cur.fetchall()
                if existentes != [(c, 'text') for c in cabecera]:
                    raise ValueError('La tabla raw existente no coincide con el CSV; no se modifico su estructura')
                definiciones = sql.SQL(', ').join(sql.SQL('{} {}').format(sql.Identifier(c), sql.SQL(t)) for c,t in COLUMNAS_GUARDAR)
                cur.execute(sql.SQL('CREATE TABLE IF NOT EXISTS staging.ordenes_compra_limpias ({})').format(definiciones))
                # Migra versiones anteriores sin recrear la tabla ni usar CASCADE.
                # Si una vista depende de una columna retirada, PostgreSQL
                # cancela y se revierte toda la transaccion.
                for c,t in COLUMNAS_GUARDAR:
                    cur.execute(sql.SQL('ALTER TABLE staging.ordenes_compra_limpias ADD COLUMN IF NOT EXISTS {} {}').format(sql.Identifier(c),sql.SQL(t)))
                for c in sorted(QUITAR):
                    cur.execute(sql.SQL('ALTER TABLE staging.ordenes_compra_limpias DROP COLUMN IF EXISTS {}').format(sql.Identifier(c)))
                cur.execute('''CREATE TABLE IF NOT EXISTS staging.ordenes_compra_rechazos (
                    fila_csv bigint, codigo text, motivo text, datos_raw jsonb)''')
                cur.execute('LOCK TABLE staging.ordenes_compra_raw, staging.ordenes_compra_limpias, staging.ordenes_compra_rechazos IN SHARE ROW EXCLUSIVE MODE')
                cur.execute('DELETE FROM staging.ordenes_compra_raw')
                ruta_limpias = Path(carpeta)/'limpias.csv'
                ruta_rechazos = Path(carpeta)/'rechazos.csv'
                with ruta_limpias.open('w', newline='', encoding='utf-8') as fl, ruta_rechazos.open('w', newline='', encoding='utf-8') as fr:
                    wl, wr = csv.writer(fl), csv.writer(fr)
                    comando = sql.SQL('COPY staging.ordenes_compra_raw ({}) FROM STDIN').format(sql.SQL(', ').join(map(sql.Identifier,cabecera)))
                    with cur.copy(comando) as copia:
                        for n, fila in enumerate(lector, 2):
                            if None in fila or any(v is None for v in fila.values()):
                                raise ValueError(f'Fila CSV {n}: numero de columnas incorrecto')
                            copia.write_row(tuple(fila[c] for c in cabecera))
                            resumen['filas_raw'] += 1
                            if not texto(fila['CodigoLicitacion']):
                                resumen['sin_licitacion'] += 1
                                continue
                            try:
                                moneda_oc = texto(fila.get('TipoMonedaOC'))
                                if not moneda_oc:
                                    raise ValueError('TipoMonedaOC: moneda de cabecera ausente')
                                moneda_oc = moneda_oc.upper()
                                moneda_item = (texto(fila.get('monedaItem')) or moneda_oc).upper()
                                discrepancia = moneda_item != moneda_oc
                                fila_efectiva = fila
                                if discrepancia:
                                    if not grupos_oc.get(texto(fila.get('Codigo')), {}).get('valido', False):
                                        raise ValueError('moneda: discrepancia sin conciliacion de OC')
                                    fila_efectiva = fila.copy()
                                    fila_efectiva['monedaItem'] = moneda_oc
                                g = grupos_oc.get(texto(fila.get('Codigo')))
                                if not g or not g['valido']:
                                    raise ValueError('conciliacion_oc: ' + (g['error'] if g else 'OC inexistente'))
                                valores = convertir_clp(transformar(fila_efectiva, n, config), tasas)
                                # Los originales declarados se conservan para auditoria.
                                total_oc_original = numero(fila['MontoTotalOC'], 'MontoTotalOC', config)
                                total_oc_clp_fuente = numero(
                                    fila['MontoTotalOC_PesosChilenos'],
                                    'MontoTotalOC_PesosChilenos', config
                                )
                                # convertir_clp ya determino y persistio la tasa de cada item.
                                # La cabecera se convierte con esa MISMA tasa, sin redondearla.
                                with localcontext() as ctx:
                                    ctx.prec = 60
                                    total_oc_clp = total_oc_original * valores[29]
                                # Las asignaciones se hacen en moneda original y luego
                                # se convierten con la misma tasa de FechaEnvio.
                                with localcontext() as ctx:
                                    ctx.prec = 60
                                    neto_ajustado_original = (
                                        valores[24] + valores[27] - valores[26]
                                    )
                                    asignados = asignar_cabecera(g, neto_ajustado_original)
                                    tasa = valores[29]
                                    neto_ajustado_clp = neto_ajustado_original * tasa
                                    imp_oc = g['cabecera']['Impuestos']
                                    cargo_oc = g['cabecera']['Cargos']
                                    dto_oc = g['cabecera']['Descuentos']
                                    imp_asig = asignados['Impuestos'] * tasa
                                    cargo_asig = asignados['Cargos'] * tasa
                                    dto_asig = asignados['Descuentos'] * tasa
                                    bruto_asig = neto_ajustado_clp + imp_asig + cargo_asig - dto_asig
                                valores += (
                                    neto_ajustado_original, neto_ajustado_clp,
                                    imp_oc, imp_oc * tasa, imp_asig,
                                    dto_oc, dto_oc * tasa, dto_asig,
                                    cargo_oc, cargo_oc * tasa, cargo_asig,
                                    bruto_asig, next(iter(g['tipos_impuesto'])),
                                    total_oc_clp, total_oc_original, total_oc_clp_fuente,
                                    moneda_item, moneda_oc, discrepancia
                                )
                                clave = (valores[1], valores[2])
                                if clave in vistos:
                                    raise ValueError('Codigo + IDItem repetido: resolver duplicado/version antes de cargar DW')
                                vistos.add(clave)
                                wl.writerow([valores[i] for i in INDICES_GUARDAR])
                                monedas[valores[20]] += 1
                                resumen['filas_limpias'] += 1
                                if discrepancia:
                                    resumen['moneda_corregida'] += 1
                                if valores[14] != valores[14].to_integral_value():
                                    resumen['cantidades_fraccionarias'] += 1
                            except ValueError as error:
                                motivo = str(error)
                                wr.writerow((n, fila['Codigo'], motivo, json.dumps(fila,ensure_ascii=False)))
                                motivos[motivo.split(':')[0]] += 1
                                resumen['rechazos'] += 1
                            if resumen['filas_raw'] % 100000 == 0:
                                print(f"Procesadas {resumen['filas_raw']} filas", flush=True)
                if not resumen['filas_raw']:
                    raise ValueError('CSV vacio; se cancela para conservar la carga anterior')
                cur.execute("""DELETE FROM staging.ordenes_compra_limpias
                    WHERE fecha_envio >= DATE '2024-01-01'
                      AND fecha_envio < DATE '2025-01-01'""")
                cur.execute('DELETE FROM staging.ordenes_compra_rechazos')
                for tabla,ruta in [('ordenes_compra_limpias',ruta_limpias),('ordenes_compra_rechazos',ruta_rechazos)]:
                    columnas = [c for c,_ in COLUMNAS_GUARDAR] if tabla=='ordenes_compra_limpias' else ['fila_csv','codigo','motivo','datos_raw']
                    comando = sql.SQL('COPY staging.{} ({}) FROM STDIN WITH (FORMAT CSV)').format(sql.Identifier(tabla),sql.SQL(', ').join(map(sql.Identifier,columnas)))
                    with cur.copy(comando) as copia, ruta.open('rb') as datos:
                        while bloque := datos.read(1024*1024):
                            copia.write(bloque)
                cur.execute('CREATE INDEX IF NOT EXISTS idx_oc_limpias_codigo ON staging.ordenes_compra_limpias (codigo)')
            # Nunca confirmar un lote parcial: la distribucion presupone
            # que todas las lineas elegibles de cada OC estan presentes.
            cancelado = aplicar and bool(resumen['rechazos'])
            if aplicar and not cancelado:
                conn.commit()
            else:
                conn.rollback()
            return {'modo': 'cancelado_por_rechazos' if cancelado else 'aplicado' if aplicar else 'simulacion',
                    'base': config['DB_ORIGEN'], **resumen, 'motivos_rechazo': dict(motivos),
                    'monedas_items': dict(monedas),
                    'compatible_importes_DW_actual': bool(monedas) and set(monedas)=={'CLP'},
                    'oc_conciliadas': sum(g['valido'] for g in grupos_oc.values()),
                    'oc_no_conciliadas': sum(not g['valido'] for g in grupos_oc.values()),
                    'advertencia': 'Impuestos/cargos/descuentos de CABECERA asignados proporcionalmente al neto ajustado de item, en CLP por FechaEnvio. No usar SUM sobre columnas de cabecera repetidas. No carga el DW.'}
        except Exception:
            conn.rollback()
            raise
# -----------------------------------------------------------------------------
# Feather 2025-2026: etapa independiente; NUNCA reemplaza staging 2024.
# -----------------------------------------------------------------------------
FEATHER_FIELDS = (
    'OC_Codigo', 'OC_Fecha', 'OC_Tipo', 'OC_Estado', 'OC_Total_Pesos',
    'OC_Total_Neto', 'OC_Total_Impuestos', 'OC_Cargos', 'OC_Descuentos',
    'OC_Organismo', 'Proveedor_Codigo', 'Licitacion_Codigo', 'Item_ID',
    'Item_Producto_Codigo', 'Item_Cantidad', 'Item_Moneda',
    'Item_Total_Neto_Pesos', 'Item_Total_Neto', 'Item_Total_Descuentos',
    'Item_Total_Cargos', 'OC_Tipo_Impuesto',
)
TIPOS_FEATHER = {'SE': 8, 'CM': 9, 'AG': 13, 'CC': 14}
# TD requiere codigo real de dw.dim_tipo_orden_compra; no inferirlo.


def _decimal_feather(value, nombre):
    if value is None:
        raise ValueError(f'{nombre}: valor ausente')
    d = Decimal(str(value))
    if not d.is_finite():
        raise ValueError(f'{nombre}: valor no finito')
    return d


def _clave_fecha(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    return str(value)[:10]


def verificar_dimensiones_feather(base, config, tipos_validos, reporte):
    """Consulta dimensiones en smartbids_dw sin escribir en PostgreSQL.

    Evalua OC previamente aceptadas, completas y con importes conciliados.
    Devuelve conteos por dimension y deja un CSV con OC afectadas.
    """
    import psycopg

    destino = (config.get('DB_DESTINO') or '').strip()
    if not destino:
        raise ValueError('Para validar dimensiones se requiere DB_DESTINO en .env')

    definiciones = (
        ('producto', 'SELECT producto_codigo FROM dw.dim_producto', 'TEXT'),
        ('proveedor', 'SELECT proveedor_codigo FROM dw.dim_proveedor', 'INTEGER'),
        ('comprador', 'SELECT comprador_organismo_codigo FROM dw.dim_comprador', 'INTEGER'),
        ('fecha', 'SELECT fecha FROM dw.dim_fecha', 'TEXT'),
        ('tipo', 'SELECT codigo_tipo FROM dw.dim_tipo_orden_compra', 'INTEGER'),
    )
    print(f'Feather: validando cobertura de dimensiones en {destino}...', flush=True)
    with psycopg.connect(host=config['PG_HOST'], port=config.get('PG_PORT', '5432'),
                         user=config['PG_USER'], password=config['PG_PASSWORD'],
                         dbname=destino, connect_timeout=10) as conexion:
        with conexion.cursor() as cursor:
            for nombre, consulta, tipo in definiciones:
                base.execute(f'CREATE TABLE dim_{nombre} (codigo {tipo} PRIMARY KEY)')
                cursor.execute(consulta)
                while registros := cursor.fetchmany(10000):
                    if nombre == 'fecha':
                        valores = ((r[0].isoformat() if hasattr(r[0], 'isoformat') else str(r[0]),)
                                   for r in registros if r[0] is not None)
                    elif nombre == 'producto':
                        valores = ((str(r[0]).strip(),) for r in registros if r[0] is not None)
                    else:
                        valores = ((int(r[0]),) for r in registros if r[0] is not None)
                    base.executemany(f'INSERT OR IGNORE INTO dim_{nombre}(codigo) VALUES (?)', valores)
                print(f'  dim_{nombre}: {base.execute(f"SELECT COUNT(*) FROM dim_{nombre}").fetchone()[0]:,} claves', flush=True)

    # Una fila por OC aceptada; los MAX evitan contar una OC mas de una vez.
    # Normalizamos producto como entero positivo sin ceros iniciales, igual que
    # el codigo de producto normalizado en el CSV 2024.
    tipo_sql = 'CASE i.tipo ' + ' '.join(
        "WHEN '" + sigla.replace("'", "''") + "' THEN " + str(codigo)
        for sigla, codigo in tipos_validos.items()
    ) + ' ELSE NULL END'
    base.execute(f'''CREATE TABLE cobertura_oc AS
        SELECT i.codigo,
               MAX(CASE WHEN dp.codigo IS NULL THEN 1 ELSE 0 END) AS producto,
               MAX(CASE WHEN dv.codigo IS NULL THEN 1 ELSE 0 END) AS proveedor,
               MAX(CASE WHEN dc.codigo IS NULL THEN 1 ELSE 0 END) AS comprador,
               MAX(CASE WHEN df.codigo IS NULL THEN 1 ELSE 0 END) AS fecha,
               MAX(CASE WHEN dt.codigo IS NULL THEN 1 ELSE 0 END) AS tipo
        FROM item i JOIN oc o ON o.codigo=i.codigo AND o.motivo IS NULL
        LEFT JOIN dim_producto dp ON dp.codigo=CAST(CAST(i.producto AS INTEGER) AS TEXT)
        LEFT JOIN dim_proveedor dv ON dv.codigo=i.proveedor
        LEFT JOIN dim_comprador dc ON dc.codigo=i.organismo
        LEFT JOIN dim_fecha df ON df.codigo=i.fecha
        LEFT JOIN dim_tipo dt ON dt.codigo={tipo_sql}
        GROUP BY i.codigo''')
    base.execute('CREATE UNIQUE INDEX idx_cobertura_codigo ON cobertura_oc(codigo)')
    base.commit()
    conteo = base.execute('''SELECT COUNT(*),
        COALESCE(SUM(CASE WHEN producto+proveedor+comprador+fecha+tipo>0 THEN 1 ELSE 0 END),0),
        COALESCE(SUM(producto),0), COALESCE(SUM(proveedor),0),
        COALESCE(SUM(comprador),0), COALESCE(SUM(fecha),0), COALESCE(SUM(tipo),0)
        FROM cobertura_oc''').fetchone()
    nombres = ('producto', 'proveedor', 'comprador', 'fecha', 'tipo')
    resultado = {'oc_evaluadas': conteo[0], 'oc_con_dimensiones_faltantes': conteo[1],
                 'oc_con_todas_dimensiones': conteo[0]-conteo[1],
                 'oc_afectadas_por_dimension': dict(zip(nombres, conteo[2:]))}
    resultado['items_afectados'] = base.execute('''SELECT COALESCE(SUM(o.n_items),0)
        FROM cobertura_oc c JOIN oc o ON o.codigo=c.codigo
        WHERE c.producto+c.proveedor+c.comprador+c.fecha+c.tipo>0''').fetchone()[0]

    # Cuenta CODIGOS distintos faltantes, no items: útil para ampliar catálogos.
    unicos = {}
    ejemplos = {}
    claves = {'producto': ('CAST(CAST(i.producto AS INTEGER) AS TEXT)', 'dim_producto'),
              'proveedor': ('i.proveedor', 'dim_proveedor'),
              'comprador': ('i.organismo', 'dim_comprador'),
              'fecha': ('i.fecha', 'dim_fecha'),
              'tipo': (tipo_sql, 'dim_tipo')}
    for nombre, (expr, tabla) in claves.items():
        consulta = f'''SELECT DISTINCT {expr} AS clave
            FROM item i JOIN cobertura_oc c ON c.codigo=i.codigo
            LEFT JOIN {tabla} d ON d.codigo={expr}
            WHERE c.{nombre}=1 AND d.codigo IS NULL'''
        # Una sola lectura agrupada para evitar traer millones de claves a Python.
        claves_ausentes = base.execute(f'SELECT COUNT(*) FROM ({consulta})').fetchone()[0]
        muestras = [r[0] for r in base.execute(f'SELECT clave FROM ({consulta}) ORDER BY clave LIMIT 15')]
        unicos[nombre] = claves_ausentes
        ejemplos[nombre] = muestras
    resultado['codigos_distintos_faltantes'] = unicos
    resultado['ejemplos_codigos_faltantes'] = ejemplos
    informe = reporte.with_name(reporte.stem + '_dimensiones.csv')
    with informe.open('w', newline='', encoding='utf-8-sig') as salida:
        escritor = csv.writer(salida)
        escritor.writerow(['codigo_oc', 'producto_faltante', 'proveedor_faltante',
                           'comprador_faltante', 'fecha_faltante', 'tipo_faltante'])
        escritor.writerows(base.execute('''SELECT codigo, producto, proveedor, comprador, fecha, tipo
            FROM cobertura_oc WHERE producto+proveedor+comprador+fecha+tipo>0 ORDER BY codigo'''))
    resultado['reporte_oc_dimensiones_faltantes'] = str(informe)
    print('Cobertura dimensiones:', json.dumps(resultado, ensure_ascii=False), flush=True)
    return resultado


def ejecutar_feather(config, aplicar=False, reporte=None):
    """Valida órdenes COMPLETAS CLP y, con --aplicar, crea staging independiente.

    Los encabezados se comprueban por OC. Las monedas extranjeras y los ítems
    ambiguos se excluyen por OC completa. Las asignaciones siguen el neto CLP
    directo, excepto ajuste de línea cuando ES el único que concilia.
    """
    import sqlite3
    import pyarrow as pa
    import pyarrow.ipc as ipc

    ruta = Path(config['OC_FEATHER'])
    if not ruta.is_file():
        raise FileNotFoundError(f'No existe el archivo Feather: {ruta}')
    tolerancia = Decimal('1')  # Inspección de float64, tolerancia absoluta CLP.
    if reporte is None:
        reporte = Path(f'reporte_feather_{datetime.now():%Y%m%d_%H%M%S}.csv')
    reporte = Path(reporte)
    reporte.parent.mkdir(parents=True, exist_ok=True)
    resumen = Counter()
    tipos_validos = dict(TIPOS_FEATHER)
    codigo_td = (config.get('OC_TIPO_TD_CODIGO') or '').strip()
    if codigo_td:
        if not codigo_td.isdecimal() or int(codigo_td) in tipos_validos.values():
            raise ValueError('OC_TIPO_TD_CODIGO debe ser un entero positivo distinto de los tipos existentes')
        tipos_validos['TD'] = int(codigo_td)

    with tempfile.TemporaryDirectory(prefix='smartbids_feather_') as carpeta:
        base = sqlite3.connect(str(Path(carpeta) / 'feather.sqlite'))
        base.execute('PRAGMA journal_mode=OFF')
        base.execute('PRAGMA synchronous=OFF')
        base.execute('PRAGMA cache_size=-128000')
        base.execute('PRAGMA temp_store=FILE')
        base.execute('''CREATE TABLE item (
            codigo TEXT, fecha TEXT, tipo TEXT, estado TEXT, total REAL,
            neto REAL, impuesto REAL, cargo REAL, descuento REAL,
            organismo INTEGER, proveedor INTEGER, licitacion TEXT, id_item TEXT,
            producto TEXT, cantidad REAL, moneda TEXT, neto_item REAL,
            neto_original REAL, dto_item REAL, cargo_item REAL, tipo_impuesto TEXT,
            error TEXT
        )''')
        insert_sql = 'INSERT INTO item VALUES (' + ','.join(['?'] * 22) + ')'
        with pa.memory_map(str(ruta), 'r') as archivo:
            reader = ipc.open_file(archivo)
            indexes = [reader.schema.get_field_index(k) for k in FEATHER_FIELDS]
            faltantes = [k for k, i in zip(FEATHER_FIELDS, indexes) if i < 0]
            if faltantes:
                raise ValueError('Faltan columnas Feather: ' + ', '.join(faltantes))
            for n in range(reader.num_record_batches):
                batch = reader.get_batch(n)
                columnas = [batch.column(i).to_pylist() for i in indexes]
                filas = []
                for z in zip(*columnas):
                    (codigo, fecha_oc, tipo, estado, total, neto, impuesto, cargo,
                     descuento, organismo, proveedor, licitacion, id_item, producto,
                     cantidad, moneda, neto_item, neto_orig, dto_item, cargo_item,
                     tipo_imp) = z
                    fecha_iso = _clave_fecha(fecha_oc)
                    error = None
                    if not codigo or not str(codigo).strip():
                        raise ValueError('Hay ítems sin código de OC; no se puede verificar integridad')
                    if not fecha_iso or fecha_iso[:4] not in ('2025', '2026'):
                        error = 'fecha_fuera_de_periodo'
                    elif not id_item or not str(id_item).strip():
                        error = 'id_item_invalido'
                    elif producto is None or not str(producto).strip().isdigit() or int(producto) == 0:
                        error = 'producto_invalido'
                    elif not organismo or not proveedor:
                        error = 'proveedor_u_organismo_invalido'
                    elif tipo not in tipos_validos:
                        error = 'tipo_td_pendiente_homologacion' if tipo == 'TD' else 'tipo_no_homologado'
                    elif cantidad is None or not math.isfinite(float(cantidad)) or cantidad < 0:
                        error = 'cantidad_invalida'
                    elif any(v is None or not math.isfinite(float(v))
                             for v in (total, neto, impuesto, cargo, descuento, neto_item)):
                        error = 'monto_ausente_o_no_finito'
                    # Licitacion_Codigo es opcional en 2025-2026.
                    filas.append((str(codigo).strip(), fecha_iso, str(tipo or ''), str(estado or ''),
                                  total, neto, impuesto, cargo, descuento,
                                  organismo, proveedor, str(licitacion or ''), str(id_item or ''),
                                  str(producto or ''), cantidad, str(moneda or ''),
                                  neto_item, neto_orig, dto_item, cargo_item,
                                  str(tipo_imp or ''), error))
                base.executemany(insert_sql, filas)
                resumen['items_leidos'] += len(filas)
                if (n+1) % 10 == 0 or n+1 == reader.num_record_batches:
                    print(f'Feather: bloques {n+1}/{reader.num_record_batches}, ítems={resumen["items_leidos"]:,}', flush=True)
        base.commit()
        print('Feather: agrupando y verificando órdenes completas...', flush=True)
        base.execute('CREATE INDEX idx_item_feather_codigo ON item(codigo)')
        base.execute('''CREATE TABLE oc AS SELECT
            codigo, COUNT(*) n_items,
            MIN(fecha) fecha_min, MAX(fecha) fecha_max,
            MIN(tipo) tipo_min, MAX(tipo) tipo_max,
            MIN(estado) estado_min, MAX(estado) estado_max,
            MIN(moneda) moneda_min, MAX(moneda) moneda_max,
            MIN(proveedor) proveedor_min, MAX(proveedor) proveedor_max,
            MIN(organismo) organismo_min, MAX(organismo) organismo_max,
            MIN(neto) neto_min, MAX(neto) neto_max,
            MIN(impuesto) impuesto_min, MAX(impuesto) impuesto_max,
            MIN(cargo) cargo_min, MAX(cargo) cargo_max,
            MIN(descuento) descuento_min, MAX(descuento) descuento_max,
            MIN(total) total_min, MAX(total) total_max,
            SUM(neto_item) suma_neto,
            SUM(CASE WHEN neto_item IS NOT NULL AND dto_item IS NOT NULL
                     AND cargo_item IS NOT NULL
                     THEN neto_item + dto_item - cargo_item END) suma_ajustada,
            SUM(CASE WHEN neto_item IS NULL OR dto_item IS NULL OR cargo_item IS NULL
                     THEN 1 ELSE 0 END) n_ajustes_nulos,
            SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END) errores_items,
            COUNT(DISTINCT id_item) ids_distintos
            FROM item GROUP BY codigo''')
        base.execute('ALTER TABLE oc ADD COLUMN motivo TEXT')
        # Las condiciones de cabecera se evalúan en SQL sobre float64 con
        # tolerancia absoluta <= 1 CLP. El proceso de carga usa Decimal.
        base.execute('''UPDATE oc SET motivo=CASE
            WHEN moneda_min!='CLP' OR moneda_max!='CLP' THEN 'moneda_no_clp'
            WHEN errores_items>0 THEN (SELECT i2.error FROM item i2 WHERE i2.codigo=oc.codigo AND i2.error IS NOT NULL ORDER BY i2.error LIMIT 1)
            WHEN ids_distintos!=n_items THEN 'item_duplicado_en_oc'
            WHEN fecha_min!=fecha_max OR tipo_min!=tipo_max OR estado_min!=estado_max
              OR proveedor_min!=proveedor_max OR organismo_min!=organismo_max
              THEN 'cabecera_dimensional_inconsistente'
            WHEN neto_min IS NULL OR impuesto_min IS NULL OR cargo_min IS NULL
              OR descuento_min IS NULL OR total_min IS NULL THEN 'cabecera_incompleta'
            WHEN ABS(neto_min-neto_max)>1 OR ABS(impuesto_min-impuesto_max)>1
              OR ABS(cargo_min-cargo_max)>1 OR ABS(descuento_min-descuento_max)>1
              OR ABS(total_min-total_max)>1 THEN 'cabecera_monetaria_inconsistente'
            WHEN neto_min<0 OR n_ajustes_nulos>0 THEN 'neto_negativo_o_ajustes_nulos'
            WHEN ABS(suma_neto-neto_min)>1 AND ABS(suma_ajustada-neto_min)>1
              THEN 'neto_items_no_concilia'
            WHEN neto_min=0 AND (impuesto_min!=0 OR cargo_min!=0 OR descuento_min!=0)
              THEN 'base_cero_con_ajustes'
            ELSE NULL END''')
        base.commit()
        # Diagnóstico primero, exclusión definitiva después: siempre por OC completa.
        cobertura = verificar_dimensiones_feather(base, config, tipos_validos, reporte)
        elegibles_antes = base.execute('SELECT COUNT(*) FROM oc WHERE motivo IS NULL').fetchone()[0]
        if cobertura['oc_evaluadas'] != elegibles_antes:
            raise ValueError('Cobertura dimensional no coincide con OC monetariamente elegibles')
        # Conservamos el motivo preciso y el CSV dimensional; no se descarta nada
        # individualmente por item, pues eso alteraría totales de cabecera.
        base.execute("""UPDATE oc SET motivo='dimension_faltante'
            WHERE motivo IS NULL AND codigo IN (
                SELECT codigo FROM cobertura_oc
                WHERE producto+proveedor+comprador+fecha+tipo > 0
            )""")
        base.commit()
        resumen['oc_excluidas_por_dimension'] = cobertura['oc_con_dimensiones_faltantes']
        resumen['items_excluidos_por_dimension'] = cobertura['items_afectados']
        conteos = dict(base.execute('SELECT COALESCE(motivo, "aceptada"), COUNT(*) FROM oc GROUP BY motivo'))
        resumen['oc_aceptadas'] = conteos.get('aceptada', 0)
        resumen['oc_excluidas'] = sum(v for k, v in conteos.items() if k != 'aceptada')
        resumen['items_aceptados'] = base.execute('SELECT COALESCE(SUM(n_items),0) FROM oc WHERE motivo IS NULL').fetchone()[0]
        resumen['items_excluidos'] = resumen['items_leidos'] - resumen['items_aceptados']
        for anio in ('2025', '2026'):
            resumen['oc_aceptadas_'+anio] = base.execute(
                "SELECT COUNT(*) FROM oc WHERE motivo IS NULL AND SUBSTR(fecha_min,1,4)=?", (anio,)
            ).fetchone()[0]
        resumen['monto_bruto_aceptado_clp_aproximado'] = base.execute(
            'SELECT SUM(neto_min+impuesto_min+cargo_min-descuento_min) FROM oc WHERE motivo IS NULL'
        ).fetchone()[0] or 0
        resumen['oc_reconstruidas_por_total_fuente'] = base.execute("SELECT COUNT(*) FROM oc WHERE motivo IS NULL AND ABS(neto_min+impuesto_min+cargo_min-descuento_min-total_min)>1").fetchone()[0]
        resumen['oc_aceptadas_sin_licitacion'] = base.execute("SELECT COUNT(*) FROM oc o WHERE o.motivo IS NULL AND NOT EXISTS (SELECT 1 FROM item i WHERE i.codigo=o.codigo AND NULLIF(TRIM(i.licitacion),'') IS NOT NULL)").fetchone()[0]
        if resumen['oc_aceptadas'] != cobertura['oc_con_todas_dimensiones']:
            raise ValueError('Las OC finalmente aceptadas difieren de la cobertura dimensional')
        # Regenerar reporte general incluyendo faltantes dimensionales.
        with reporte.open('w', newline='', encoding='utf-8-sig') as destino:
            writer = csv.writer(destino)
            writer.writerow(['codigo_oc', 'anio', 'items', 'motivo', 'moneda', 'estado', 'neto_oc', 'total_clp_fuente'])
            writer.writerows(base.execute("SELECT codigo, SUBSTR(fecha_min,1,4), n_items, motivo, moneda_min, estado_min, neto_min, total_min FROM oc WHERE motivo IS NOT NULL ORDER BY motivo, codigo"))
        print('Exclusiones definitivas por motivo:', json.dumps(
            {k: v for k, v in conteos.items() if k != 'aceptada'}, ensure_ascii=False), flush=True)
        print('Informe:', reporte.resolve(), flush=True)
        if aplicar and not resumen['oc_aceptadas']:
            raise ValueError('No hay OC elegibles; staging anterior intacto')
        if aplicar:
            _guardar_feather_en_staging(config, base, resumen, tolerancia, tipos_validos)
        resultado = {
            'modo': 'aplicado' if aplicar else 'simulacion',
            'feather': str(ruta),
            'tabla_destino': 'staging.ordenes_compra_limpias',
            'tabla_feather_intermedia': 'staging.ordenes_compra_feather_limpias',
            'tipos_homologados': tipos_validos,
            'validacion_dimensiones': cobertura,
            **resumen,
            'exclusiones_por_motivo': {k: v for k, v in conteos.items() if k != 'aceptada'},
            'reporte_excluidas': str(reporte),
            'nota': 'Solo OC completas CLP con dimensiones existentes; consolidación al staging único. Sin carga de hechos al DW. CSV 2024 intacto.'
        }
        base.close()
        return resultado


def _guardar_feather_en_staging(config, db, resumen, tolerancia, tipos_validos):
    import psycopg
    from psycopg import sql
    # La tabla Feather existente sirve de respaldo y fuente de la consolidación.
    ddl = '''CREATE TABLE IF NOT EXISTS staging.ordenes_compra_feather_limpias (
        codigo text NOT NULL,
        id_item text NOT NULL,
        codigo_licitacion text,
        fecha_envio date NOT NULL,
        codigo_producto_onu text NOT NULL,
        codigo_proveedor bigint NOT NULL,
        codigo_organismo_publico bigint NOT NULL,
        codigo_tipo integer NOT NULL,
        cantidad numeric NOT NULL,
        neto_item_ajustado_clp numeric NOT NULL,
        impuesto_asignado_clp numeric NOT NULL,
        cargo_oc_asignado_clp numeric NOT NULL,
        descuento_oc_asignado_clp numeric NOT NULL,
        monto_bruto_asignado_clp numeric NOT NULL,
        monto_total_oc_pesos_chilenos numeric NOT NULL,
        monto_total_oc_clp_fuente numeric,
        impuesto_oc_clp numeric NOT NULL,
        cargo_oc_clp numeric NOT NULL,
        descuento_oc_clp numeric NOT NULL,
        estado text,
        moneda_original text NOT NULL,
        PRIMARY KEY (codigo, id_item)
    )'''
    nombres = ('codigo','id_item','codigo_licitacion','fecha_envio','codigo_producto_onu',
               'codigo_proveedor','codigo_organismo_publico','codigo_tipo','cantidad',
               'neto_item_ajustado_clp','impuesto_asignado_clp','cargo_oc_asignado_clp',
               'descuento_oc_asignado_clp','monto_bruto_asignado_clp',
               'monto_total_oc_pesos_chilenos','monto_total_oc_clp_fuente',
               'impuesto_oc_clp','cargo_oc_clp','descuento_oc_clp','estado','moneda_original')
    with psycopg.connect(host=config['PG_HOST'], port=config.get('PG_PORT', '5432'),
                         user=config['PG_USER'], password=config['PG_PASSWORD'],
                         dbname=config['DB_ORIGEN'], connect_timeout=10) as con:
        with con.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout='10s'")
            cur.execute('CREATE SCHEMA IF NOT EXISTS staging')
            cur.execute(ddl)
            cur.execute('LOCK TABLE staging.ordenes_compra_feather_limpias IN SHARE ROW EXCLUSIVE MODE')
            cur.execute('DELETE FROM staging.ordenes_compra_feather_limpias')
            consulta = '''SELECT i.codigo, i.id_item, i.licitacion, i.fecha, i.producto,
                i.proveedor, i.organismo, i.tipo, i.cantidad, i.neto_item, i.neto_original,
                i.dto_item, i.cargo_item, i.estado, o.neto_min, o.impuesto_min,
                o.cargo_min, o.descuento_min, o.total_min, i.moneda, o.n_items,
                o.suma_neto, o.suma_ajustada
                FROM item i JOIN oc o ON i.codigo=o.codigo
                WHERE o.motivo IS NULL ORDER BY i.codigo, i.id_item'''
            query = db.execute(consulta)
            command = sql.SQL('COPY staging.ordenes_compra_feather_limpias ({}) FROM STDIN').format(
                sql.SQL(',').join(map(sql.Identifier, nombres)))
            cargados = 0
            with cur.copy(command) as copia:
                codigo_anterior = None
                acumulado = [Decimal(0), Decimal(0), Decimal(0)]
                items_oc = 0
                neto_acumulado = Decimal(0)
                for r in query:
                    (codigo, id_item, licitacion, fecha_oc, producto, proveedor, organismo,
                     tipo, cantidad, neto_item, neto_original, dto_item, cargo_item,
                     estado, neto_oc, impuesto_oc, cargo_oc, descuento_oc, total_fuente,
                     moneda, n_items, suma_neto, suma_ajustada) = r
                    if codigo != codigo_anterior:
                        codigo_anterior = codigo
                        acumulado = [Decimal(0), Decimal(0), Decimal(0)]
                        items_oc = 0
                        neto_acumulado = Decimal(0)
                    items_oc += 1
                    neto = _decimal_feather(neto_item, 'neto_item')
                    # Prefiere directo; usa ajuste solo si es el que concilia.
                    if abs(_decimal_feather(suma_neto, 'suma_neto') - _decimal_feather(neto_oc, 'neto_oc')) > tolerancia:
                        neto += _decimal_feather(dto_item, 'dto_item') - _decimal_feather(cargo_item, 'cargo_item')
                    base_neto = _decimal_feather(neto_oc, 'neto_oc')
                    # Asigna el residuo final del neto debido a la precisión float64.
                    if items_oc == n_items:
                        neto = base_neto - neto_acumulado
                    neto_acumulado += neto
                    if neto < 0:
                        raise ValueError(f'OC {codigo}: neto de ítem negativo; se revierte carga')
                    imp = _decimal_feather(impuesto_oc, 'impuesto_oc')
                    cargo = _decimal_feather(cargo_oc, 'cargo_oc')
                    dto = _decimal_feather(descuento_oc, 'descuento_oc')
                    totales = (imp, cargo, dto)
                    importes = []
                    with localcontext() as ctx:
                        ctx.prec=60
                        for j, total in enumerate(totales):
                            asignado = (total - acumulado[j]) if items_oc == n_items else (
                                total * neto / base_neto if base_neto else Decimal(0))
                            acumulado[j] += asignado
                            importes.append(asignado)
                        bruto = neto + importes[0] + importes[1] - importes[2]
                        total_calculado = base_neto + imp + cargo - dto
                    copia.write_row((codigo, id_item, licitacion, fecha_oc, producto,
                                     proveedor, organismo, tipos_validos[tipo],
                                     Decimal(str(cantidad)), neto, importes[0],
                                     importes[1], importes[2], bruto,
                                     total_calculado, _decimal_feather(total_fuente, 'total_fuente'),
                                     imp, cargo, dto, estado, moneda))
                    cargados += 1
                    if cargados % 200000 == 0:
                        print(f'Staging Feather: escritos {cargados:,} ítems...', flush=True)
            if cargados != resumen['items_aceptados']:
                raise ValueError(f'Cantidad cargada difiere: {cargados} vs {resumen["items_aceptados"]}')
            # No duplicar órdenes de 2024 si el mismo código aparece en ambas fuentes.
            cur.execute("""SELECT f.codigo FROM staging.ordenes_compra_feather_limpias f
                JOIN staging.ordenes_compra_limpias c ON c.codigo=f.codigo LIMIT 5""")
            superpuestos = cur.fetchall()
            if superpuestos:
                raise ValueError(f'OC ya existentes en staging 2024: {superpuestos}')
            cur.execute('SELECT COUNT(*) FROM staging.ordenes_compra_feather_limpias')
            if cur.fetchone()[0] != cargados:
                raise ValueError('Conteo staging Feather difiere')
        # Inserta en staging consolidado en la MISMA transacción. Si falla,
        # se revierten tanto los cambios de Feather como la consolidación.
        consolidado = consolidar_staging(config, aplicar=True, conexion=con, esperados=cargados)
        con.commit()
    print(f'Staging Feather y consolidado confirmados: {cargados:,} ítems', flush=True)
    print('Resumen de consolidación:', json.dumps(consolidado, ensure_ascii=False, default=str), flush=True)



def consolidar_staging(config, aplicar=False, conexion=None, esperados=None):
    """Consolida OC 2025-26 en staging.ordenes_compra_limpias.

    Origen: staging.ordenes_compra_feather_limpias ya validada.
    Idempotente: sustituye únicamente años 2025-26. No elimina 2024.
    Todas las operaciones de escritura se ejecutan dentro de una transacción.
    """
    import psycopg
    from psycopg import sql

    if conexion is None:
        raise ValueError('Se requiere una conexión transaccional explícita')
    con = conexion
    with con.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout = '15s'")
        cur.execute("SELECT to_regclass('staging.ordenes_compra_limpias'), to_regclass('staging.ordenes_compra_feather_limpias')")
        existente, fuente = cur.fetchone()
        if existente is None or fuente is None:
            raise RuntimeError('Se requieren ambas tablas de staging: 2024 y Feather ya aplicado')

        cur.execute("""SELECT COUNT(*), COUNT(DISTINCT codigo),
                   COALESCE(SUM(monto_bruto_asignado_clp),0)
                   FROM staging.ordenes_compra_feather_limpias""")
        n_fuente, oc_fuente, bruto_fuente = cur.fetchone()
        if n_fuente <= 0:
            raise ValueError('Staging Feather vacío: no se modifica staging consolidado')
        if esperados is not None and n_fuente != esperados:
            raise ValueError(f'Ítems Feather: {n_fuente}; esperados: {esperados}')
        cur.execute("""SELECT COUNT(*) FROM staging.ordenes_compra_feather_limpias
            WHERE fecha_envio < DATE '2025-01-01' OR fecha_envio >= DATE '2027-01-01'
               OR fecha_envio IS NULL""")
        if cur.fetchone()[0]:
            raise ValueError('Feather contiene registros fuera de 2025-2026')

        cur.execute("""SELECT COUNT(*), COALESCE(SUM(monto_bruto_asignado_clp),0)
            FROM staging.ordenes_compra_limpias
            WHERE fecha_envio >= DATE '2024-01-01' AND fecha_envio < DATE '2025-01-01'""")
        cuenta_2024, bruto_2024 = cur.fetchone()
        if cuenta_2024 != 143337:
            raise ValueError(f'Protección 2024: se esperaban 143337 ítems, encontrados {cuenta_2024}')
        cur.execute("""SELECT f.codigo FROM staging.ordenes_compra_feather_limpias f
            WHERE EXISTS (
                SELECT 1 FROM staging.ordenes_compra_limpias c
                WHERE c.codigo=f.codigo
                  AND (c.fecha_envio < DATE '2025-01-01'
                       OR c.fecha_envio >= DATE '2027-01-01')
            ) LIMIT 1""")
        conflicto = cur.fetchone()
        if conflicto:
            raise ValueError(f'OC Feather solapada con otro período: {conflicto[0]}')

        salida = {
            'modo': 'aplicado' if aplicar else 'simulacion',
            'tabla_destino': 'staging.ordenes_compra_limpias',
            'items_2024_preservados': cuenta_2024,
            'items_feather_a_consolidar': n_fuente,
            'oc_feather_a_consolidar': oc_fuente,
            'monto_feather_clp': str(bruto_fuente),
        }
        if not aplicar:
            return salida

        # Bloquea la tabla común para impedir escrituras simultáneas.
        cur.execute('LOCK TABLE staging.ordenes_compra_limpias IN SHARE ROW EXCLUSIVE MODE')
        # Añade metadatos, sin cambiar tipos ni eliminar campos existentes.
        cur.execute("""ALTER TABLE staging.ordenes_compra_limpias
                       ADD COLUMN IF NOT EXISTS origen_datos text""")
        cur.execute("""ALTER TABLE staging.ordenes_compra_limpias
                       ADD COLUMN IF NOT EXISTS anio_origen integer""")
        # Actualiza las etiquetas de 2024 sin alterar sus montos.
        cur.execute("""UPDATE staging.ordenes_compra_limpias
            SET origen_datos='csv_2024', anio_origen=2024
            WHERE fecha_envio >= DATE '2024-01-01' AND fecha_envio < DATE '2025-01-01'
            AND (origen_datos IS DISTINCT FROM 'csv_2024' OR anio_origen IS DISTINCT FROM 2024)""")
        # En reejecución sólo reemplaza 2025/26, nunca 2024.
        cur.execute("""DELETE FROM staging.ordenes_compra_limpias
            WHERE fecha_envio >= DATE '2025-01-01' AND fecha_envio < DATE '2027-01-01'""")
        cur.execute("""INSERT INTO staging.ordenes_compra_limpias (
            codigo, id_item, fecha_envio, codigo_producto_onu,
            codigo_proveedor, codigo_organismo_publico, codigo_tipo,
            cantidad, neto_item_ajustado_original, neto_item_ajustado_clp,
            impuesto_oc_clp, impuesto_asignado_clp,
            cargo_oc_clp, cargo_oc_asignado_clp,
            descuento_oc_clp, descuento_oc_asignado_clp,
            monto_bruto_asignado_clp, monto_total_oc_pesos_chilenos,
            monto_total_oc_clp_fuente, moneda_original, tasa_clp,
            fecha_tasa, moneda_item_declarada, moneda_oc_declarada,
            moneda_ajustada, origen_datos, anio_origen
        )
        SELECT
            codigo, id_item, fecha_envio, codigo_producto_onu,
            codigo_proveedor, codigo_organismo_publico, codigo_tipo,
            cantidad, neto_item_ajustado_clp, neto_item_ajustado_clp,
            impuesto_oc_clp, impuesto_asignado_clp,
            cargo_oc_clp, cargo_oc_asignado_clp,
            descuento_oc_clp, descuento_oc_asignado_clp,
            monto_bruto_asignado_clp, monto_total_oc_pesos_chilenos,
            monto_total_oc_clp_fuente, moneda_original, 1,
            fecha_envio, moneda_original, moneda_original,
            FALSE, 'feather', EXTRACT(YEAR FROM fecha_envio)::integer
        FROM staging.ordenes_compra_feather_limpias""")
        if cur.rowcount != n_fuente:
            raise ValueError('INSERT Feather incompleto; se revierte la transacción')

        cur.execute("""SELECT COUNT(*), COUNT(DISTINCT codigo),
                        COALESCE(SUM(monto_bruto_asignado_clp),0)
            FROM staging.ordenes_compra_limpias
            WHERE fecha_envio >= DATE '2025-01-01' AND fecha_envio < DATE '2027-01-01'""")
        n_nuevo, oc_nuevo, bruto_nuevo = cur.fetchone()
        if (n_nuevo != n_fuente or oc_nuevo != oc_fuente
                or abs(bruto_nuevo - bruto_fuente) > Decimal('0.01')):
            raise ValueError('Conciliación Feather consolidado falló: transacción revertida')
        cur.execute("""SELECT COUNT(*), COALESCE(SUM(monto_bruto_asignado_clp),0)
            FROM staging.ordenes_compra_limpias
            WHERE fecha_envio >= DATE '2024-01-01' AND fecha_envio < DATE '2025-01-01'""")
        if cur.fetchone() != (cuenta_2024, bruto_2024):
            raise ValueError('Protección de 2024 falló; transacción revertida')
        salida['items_total_consolidado'] = n_nuevo + cuenta_2024
        salida['importe_feather_conciliado'] = True
        return salida

def main():
    from dotenv import dotenv_values
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--env', type=Path, default=Path('.env'))
    p.add_argument('--aplicar', action='store_true')
    p.add_argument('--origen', choices=('csv', 'feather', 'consolidar'), default='csv')
    p.add_argument('--reporte', type=Path, help='Ruta CSV de OC excluidas, para origen feather')
    p.add_argument('--permitir-rechazos', action='store_true', help='Compatibilidad: no habilita cargas parciales con importes asignados')
    args = p.parse_args()
    if not args.env.is_file():
        p.error('No se encontro el .env')
    config = {**dotenv_values(args.env), **os.environ}
    obligatorias = ['PG_HOST','PG_USER','PG_PASSWORD','DB_ORIGEN']
    if args.origen == 'csv':
        obligatorias += ['DB_DESTINO','OC_CSV']
    elif args.origen == 'feather':
        obligatorias += ['OC_FEATHER', 'DB_DESTINO']
    for clave in obligatorias:
        if not config.get(clave):
            p.error(f'Falta {clave} en .env')
    if config['PG_PASSWORD']=='reemplazar_con_tu_contrasena':
        p.error('Configura tu contraseña real')
    if args.origen == 'consolidar':
        import psycopg
        with psycopg.connect(host=config['PG_HOST'], port=config.get('PG_PORT','5432'),
                             user=config['PG_USER'], password=config['PG_PASSWORD'],
                             dbname=config['DB_ORIGEN']) as conexion:
            resultado = consolidar_staging(config, aplicar=args.aplicar, conexion=conexion)
            if not args.aplicar:
                conexion.rollback()
        print(json.dumps(resultado, ensure_ascii=False, indent=2))
        return
    # CSV relativo se resuelve respecto de la ubicacion del .env.
    if args.origen == 'feather':
        ruta = Path(config['OC_FEATHER'])
        if not ruta.is_absolute():
            config['OC_FEATHER'] = str(args.env.resolve().parent/ruta)
        resultado = ejecutar_feather(config, args.aplicar, args.reporte)
        print(json.dumps(resultado, ensure_ascii=False, indent=2))
        return
    ruta = Path(config['OC_CSV'])
    if not ruta.is_absolute():
        config['OC_CSV'] = str(args.env.resolve().parent/ruta)
    if config.get('OC_PARIDADES_CSV'):
        ruta=Path(config['OC_PARIDADES_CSV'])
        if not ruta.is_absolute():
            config['OC_PARIDADES_CSV']=str(args.env.resolve().parent/ruta)
    resultado = ejecutar(config,args.aplicar,args.permitir_rechazos)
    print(json.dumps(resultado,ensure_ascii=False,indent=2))
    # No crea una segunda tabla de hechos; staging solamente.
    if resultado['modo']=='cancelado_por_rechazos':
        raise SystemExit(2)
if __name__=='__main__':
    main()
