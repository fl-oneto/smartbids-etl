"""CSV -> staging.ordenes_compra_raw -> staging.ordenes_compra_limpias.

Python 3.10+, psycopg 3, python-dotenv. Solo usa DB_ORIGEN.
Cada ejecucion reemplaza el lote completo; no acumula archivos historicos.
Simula por defecto. --aplicar confirma. Si hay rechazos no confirma salvo
--permitir-rechazos. No modifica catalogos ni carga el DW.
"""
import argparse
import csv
import json
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
              'MontoTotalOC_PesosChilenos']
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
LIMPIAS += [('monto_total_oc_pesos_chilenos', 'numeric')]
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
                                valores = convertir_clp(transformar(fila, n, config),tasas)
                                # El total de cabecera ya esta expresado en CLP.
                                # Se conserva por OC y se repite en cada item.
                                total_oc = numero(fila['MontoTotalOC_PesosChilenos'],
                                                  'MontoTotalOC_PesosChilenos',config)
                                valores += (total_oc,)
                                clave = (valores[1], valores[2])
                                if clave in vistos:
                                    raise ValueError('Codigo + IDItem repetido: resolver duplicado/version antes de cargar DW')
                                vistos.add(clave)
                                wl.writerow([valores[i] for i in INDICES_GUARDAR])
                                monedas[valores[20]] += 1
                                resumen['filas_limpias'] += 1
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
                cur.execute('DELETE FROM staging.ordenes_compra_limpias')
                cur.execute('DELETE FROM staging.ordenes_compra_rechazos')
                for tabla,ruta in [('ordenes_compra_limpias',ruta_limpias),('ordenes_compra_rechazos',ruta_rechazos)]:
                    columnas = [c for c,_ in COLUMNAS_GUARDAR] if tabla=='ordenes_compra_limpias' else ['fila_csv','codigo','motivo','datos_raw']
                    comando = sql.SQL('COPY staging.{} ({}) FROM STDIN WITH (FORMAT CSV)').format(sql.Identifier(tabla),sql.SQL(', ').join(map(sql.Identifier,columnas)))
                    with cur.copy(comando) as copia, ruta.open('rb') as datos:
                        while bloque := datos.read(1024*1024):
                            copia.write(bloque)
                cur.execute('CREATE INDEX IF NOT EXISTS idx_oc_limpias_codigo ON staging.ordenes_compra_limpias (codigo)')
            cancelado = aplicar and bool(resumen['rechazos']) and not permitir_rechazos
            if aplicar and not cancelado:
                conn.commit()
            else:
                conn.rollback()
            return {'modo': 'cancelado_por_rechazos' if cancelado else 'aplicado' if aplicar else 'simulacion',
                    'base': config['DB_ORIGEN'], **resumen, 'motivos_rechazo': dict(motivos),
                    'monedas_items': dict(monedas),
                    'compatible_importes_DW_actual': bool(monedas) and set(monedas)=={'CLP'},
                    'advertencia': 'Importes convertidos a CLP por fecha_envio. Revisar cantidades fraccionarias: el DW actual las convierte a integer.'}
        except Exception:
            conn.rollback()
            raise


def main():
    from dotenv import dotenv_values
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--env', type=Path, default=Path('.env'))
    p.add_argument('--aplicar', action='store_true')
    p.add_argument('--permitir-rechazos', action='store_true', help='Guarda raw completa, limpias validas y rechazos')
    args = p.parse_args()
    if not args.env.is_file():
        p.error('No se encontro el .env')
    config = {**dotenv_values(args.env), **os.environ}
    for clave in ['PG_HOST','PG_USER','PG_PASSWORD','DB_ORIGEN','OC_CSV']:
        if not config.get(clave):
            p.error(f'Falta {clave} en .env')
    if config['PG_PASSWORD']=='reemplazar_con_tu_contrasena':
        p.error('Configura tu contraseña real')
    # CSV relativo se resuelve respecto de la ubicacion del .env.
    ruta = Path(config['OC_CSV'])
    if not ruta.is_absolute():
        config['OC_CSV'] = str(args.env.resolve().parent/ruta)
    if config.get('OC_PARIDADES_CSV'):
        ruta=Path(config['OC_PARIDADES_CSV'])
        if not ruta.is_absolute():
            config['OC_PARIDADES_CSV']=str(args.env.resolve().parent/ruta)
    resultado = ejecutar(config,args.aplicar,args.permitir_rechazos)
    print(json.dumps(resultado,ensure_ascii=False,indent=2))
    if resultado['modo']=='cancelado_por_rechazos':
        raise SystemExit(2)


if __name__=='__main__':
    main()
