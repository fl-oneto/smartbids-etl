"""API Mercado Público -> staging consolidado. Solo INSERT de OC nuevas.
Python 3.10+, requests, psycopg[binary], python-dotenv.
No ejecuta ni modifica el DW. Simula salvo --aplicar.
"""
import argparse
import csv
import sqlite3
import json
import logging
import os
import time
from collections import Counter
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo

LOG = logging.getLogger('oc_api')
URL = 'https://api.mercadopublico.cl/servicios/v1/publico/ordenesdecompra.json'


class NoElegible(ValueError):
    pass


def num(value, campo):
    if value is None or isinstance(value, bool):
        raise NoElegible(f'{campo}: numero ausente o invalido')
    try:
        n = Decimal(str(value))
    except InvalidOperation:
        raise NoElegible(f'{campo}: numero invalido') from None
    if not n.is_finite() or abs(n.adjusted()) > 100:
        raise NoElegible(f'{campo}: fuera de rango')
    return n


def texto(value, campo):
    s = '' if value is None else str(value).strip()
    if not s or s.lower() in {'none', 'null', 'nan'}:
        raise NoElegible(f'{campo}: ausente')
    return s


def entero(value, campo):
    s = texto(value, campo)
    if not s.isascii() or not s.isdigit() or not 0 < int(s) < 2**63:
        raise NoElegible(f'{campo}: entero positivo invalido')
    return int(s)


def paridades(ruta):
    resultado = {}
    if not ruta:
        return resultado
    with Path(ruta).open(encoding='utf-8-sig', newline='') as f:
        for r in csv.DictReader(f):
            d = date.fromisoformat(r['fecha'])
            publicada = date.fromisoformat(r['fecha_tasa'])
            moneda = r['moneda'].strip().upper()
            factor = num(r['tasa_clp'], 'tasa_clp')
            if factor <= 0 or publicada > d or (d, moneda) in resultado:
                raise ValueError('Paridad no valida o duplicada')
            resultado[d, moneda] = (factor, publicada)
    return resultado


def transformar(oc, anio, tasas, dimensiones):
    """Valida una OC completa. No infiere monedas ni impuestos ausentes.

    Base neta = cantidad * precio. Exige conciliar neto y ajustes de items
    con cabecera; reparte ajustes de cabecera por base neta. Ante un esquema
    distinto, rechaza la OC para revisar su respuesta original.
    """
    with localcontext() as ctx:
        ctx.prec = 60
        codigo = texto(oc.get('Codigo'), 'Codigo')
        licitacion = str(oc.get('CodigoLicitacion') or '').strip()
        if not licitacion or licitacion.lower() in {'null', 'none', 'nan'}:
            raise NoElegible('sin_licitacion')
        try:
            envio = datetime.fromisoformat(oc['Fechas']['FechaEnvio'].replace('Z', '+00:00'))
            if envio.tzinfo:
                envio = envio.astimezone(ZoneInfo('America/Santiago'))
            envio = envio.date()
        except (KeyError, TypeError, ValueError, AttributeError):
            raise NoElegible('fecha_envio_invalida') from None
        if envio.year != anio:
            raise NoElegible('fecha_envio_fuera_del_anio')
        moneda = texto(oc.get('TipoMoneda'), 'TipoMoneda').upper()
        if moneda == 'CLP':
            factor, publicada = Decimal(1), envio
        elif (envio, moneda) in tasas:
            factor, publicada = tasas[envio, moneda]
        else:
            raise NoElegible(f'paridad_faltante:{envio}:{moneda}')
        proveedor = entero(oc.get('Proveedor', {}).get('Codigo'), 'proveedor')
        comprador = entero(oc.get('Comprador', {}).get('CodigoOrganismo'), 'organismo')
        tipo = entero(oc.get('CodigoTipo'), 'CodigoTipo')
        for nombre, valor in [('proveedor', proveedor), ('comprador', comprador), ('tipo', tipo)]:
            if valor not in dimensiones[nombre]:
                raise NoElegible(f'dimension_faltante:{nombre}:{valor}')
        cab = {k: num(oc.get(k), k) for k in ('TotalNeto', 'Impuestos', 'Cargos', 'Descuentos', 'Total')}
        if any(n < 0 for n in cab.values()):
            raise NoElegible('cabecera_negativa')
        # Tolerancia maxima de 0.01 CLP, tambien al convertir otra moneda.
        tolerancia = Decimal('0.01') / max(Decimal(1), factor)
        if abs(cab['TotalNeto'] + cab['Impuestos'] + cab['Cargos'] - cab['Descuentos'] - cab['Total']) > tolerancia:
            raise NoElegible('cabecera_no_concilia')
        items = oc.get('Items', {}).get('Listado')
        if not isinstance(items, list) or not items:
            raise NoElegible('sin_items')
        if entero(oc.get('Items', {}).get('Cantidad'), 'Items.Cantidad') != len(items):
            raise NoElegible('detalle_incompleto')
        preparados, vistos = [], set()
        ajustes_items = {k: Decimal(0) for k in ('Impuestos', 'Cargos', 'Descuentos')}
        for item in items:
            correlativo = entero(item.get('Correlativo'), 'Correlativo')
            if correlativo in vistos:
                raise NoElegible('correlativo_duplicado')
            vistos.add(correlativo)
            producto = str(entero(item.get('CodigoProducto'), 'CodigoProducto'))
            if producto not in dimensiones['producto']:
                raise NoElegible(f'dimension_faltante:producto:{producto}')
            # Monedas diferentes requieren una conciliacion especifica; no se fuerzan.
            mi = texto(item.get('Moneda'), 'Items.Moneda').upper()
            if mi != moneda:
                raise NoElegible(f'monedas_distintas:{moneda}:{mi}')
            cantidad = num(item.get('Cantidad'), 'Cantidad')
            precio = num(item.get('PrecioNeto'), 'PrecioNeto')
            if cantidad < 0 or precio < 0:
                raise NoElegible('cantidad_o_precio_negativo')
            base = cantidad * precio
            ajustes = {k: num(item.get('Total' + k), 'Total' + k) for k in ajustes_items}
            if any(n < 0 for n in ajustes.values()):
                raise NoElegible('ajuste_item_negativo')
            total_item = num(item.get('Total'), 'Items.Total')
            esperado = base + ajustes['Impuestos'] + ajustes['Cargos'] - ajustes['Descuentos']
            if abs(total_item - esperado) > tolerancia:
                raise NoElegible('total_item_no_concilia')
            for k, valor in ajustes.items():
                ajustes_items[k] += valor
            preparados.append((correlativo, producto, cantidad, precio, base, ajustes))
        base_total = sum((p[4] for p in preparados), Decimal(0))
        if abs(base_total - cab['TotalNeto']) > tolerancia:
            raise NoElegible('neto_items_no_concilia')
        # Si el ajuste esta informado en detalle debe coincidir con la cabecera.
        # Cero en detalle permite ajustes exclusivos de la cabecera.
        for k, valor in ajustes_items.items():
            if valor != 0 and abs(valor - cab[k]) > tolerancia:
                raise NoElegible(f'ajustes_items_no_concilian:{k}')
        if base_total == 0 and any(cab[k] != 0 for k in ajustes_items):
            raise NoElegible('base_cero_con_ajustes')
        acumulados = {k: Decimal(0) for k in ajustes_items}
        filas = []
        for index, (corr, prod, cant, precio, base, ajustes) in enumerate(preparados):
            asignado = {}
            for k in ajustes_items:
                if index == len(preparados) - 1:
                    asignado[k] = cab[k] - acumulados[k]
                else:
                    asignado[k] = cab[k] * base / base_total if base_total else Decimal(0)
                acumulados[k] += asignado[k]
            bruto = base + asignado['Impuestos'] + asignado['Cargos'] - asignado['Descuentos']
            fila = dict(codigo=codigo, id_item=f'api:{corr}', fecha_envio=envio,
                        codigo_producto_onu=prod, codigo_proveedor=proveedor,
                        codigo_organismo_publico=comprador, codigo_tipo=tipo, cantidad=cant,
                        neto_item_ajustado_original=base, neto_item_ajustado_clp=base*factor,
                        monto_bruto_asignado_clp=bruto*factor,
                        monto_total_oc_pesos_chilenos=cab['Total']*factor,
                        monto_total_oc_original=cab['Total'], monto_total_oc_clp_fuente=None,
                        moneda_original=moneda, tasa_clp=factor, fecha_tasa=publicada,
                        moneda_item_declarada=moneda, moneda_oc_declarada=moneda,
                        moneda_ajustada=False, origen_datos='api', anio_origen=anio,
                        total_linea_neto=base*factor, total_linea_neto_original=base,
                        total_impuestos=ajustes['Impuestos']*factor,
                        total_cargos=ajustes['Cargos']*factor,
                        total_descuentos=ajustes['Descuentos']*factor)
            for k, prefijo, destino in [('Impuestos','impuesto','impuesto_asignado_clp'),
                                       ('Cargos','cargo','cargo_oc_asignado_clp'),
                                       ('Descuentos','descuento','descuento_oc_asignado_clp')]:
                fila[prefijo+'_oc_original'] = cab[k]
                fila[prefijo+'_oc_clp'] = cab[k]*factor
                fila[destino] = asignado[k]*factor
            filas.append(fila)
        if abs(sum(f['monto_bruto_asignado_clp'] for f in filas) - cab['Total']*factor) > Decimal('0.01'):
            raise NoElegible('asignacion_clp_no_concilia')
        return filas


def segundos_retry_after(valor, ahora=None):
    if not valor:
        return 0.0
    try:
        return max(0.0, float(valor))
    except ValueError:
        try:
            momento = parsedate_to_datetime(valor).timestamp()
            return max(0.0, momento - (time.time() if ahora is None else ahora))
        except (ValueError, TypeError, OverflowError):
            return 0.0


class CacheDetalles:
    """SQLite local: detalles completos, sin ticket ni URLs autenticadas."""
    def __init__(self, ruta, horas=168):
        Path(ruta).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(ruta))
        self.horas = horas
        self.db.execute('CREATE TABLE IF NOT EXISTS detalles (codigo TEXT PRIMARY KEY, fecha REAL NOT NULL, contenido TEXT NOT NULL)')
        self.db.commit()

    def obtener(self, codigo):
        fila = self.db.execute('SELECT fecha,contenido FROM detalles WHERE codigo=?',(codigo,)).fetchone()
        if not fila or time.time()-fila[0] > self.horas*3600:
            return None
        try:
            oc = json.loads(fila[1], parse_float=Decimal)
        except ValueError:
            return None
        return oc if isinstance(oc,dict) and oc.get('Codigo') == codigo and isinstance(oc.get('Items'),dict) else None

    def guardar(self, codigo, oc):
        self.db.execute('INSERT OR REPLACE INTO detalles VALUES (?,?,?)',
                        (codigo,time.time(),json.dumps(oc,default=str,ensure_ascii=False)))
        self.db.commit()

    def close(self):
        self.db.close()


class Cliente:
    def __init__(self, ticket, pausa, intentos, maximo, cache=None,
                 minimo=2.0, refrescar=False):
        import requests
        self.requests = requests
        self.session = requests.Session()
        self.ticket, self.pausa, self.intentos = ticket, max(pausa,minimo), intentos
        self.minimo, self.cache, self.refrescar = minimo, cache, refrescar
        self.maximo, self.llamadas, self.ultima = maximo, 0, 0.0
        self.cache_hits, self.exitos = 0, 0

    def consultar(self, **params):
        codigo = params.get('codigo')
        if codigo and self.cache and not self.refrescar:
            dato = self.cache.obtener(codigo)
            if dato is not None:
                self.cache_hits += 1
                return [dato]
        for intento in range(self.intentos):
            if self.llamadas >= self.maximo:
                raise RuntimeError('Presupuesto de llamadas agotado; los detalles descargados quedan en cache')
            time.sleep(max(0, self.pausa - (time.monotonic() - self.ultima)))
            self.llamadas += 1
            self.ultima = time.monotonic()
            espera = min(60, 2**(intento+1))
            try:
                with self.session.get(URL, params={**params, 'ticket':self.ticket},
                                      timeout=(10, 60), allow_redirects=False) as respuesta:
                    estado = respuesta.status_code
                    if estado == 429 or 500 <= estado < 600:
                        fallo = f'HTTP {estado}'
                        espera = max(espera, segundos_retry_after(respuesta.headers.get('Retry-After')))
                        if estado == 429:
                            self.pausa = min(60, max(self.minimo,self.pausa*1.5))
                            self.exitos = 0
                            espera = max(espera,self.pausa)
                    elif estado != 200:
                        raise RuntimeError(f'API HTTP {estado}; revisar acceso (ticket omitido)')
                    else:
                        try:
                            datos = json.loads(respuesta.text, parse_float=Decimal)
                        except ValueError:
                            fallo = 'respuesta JSON invalida'
                        else:
                            if not isinstance(datos, dict) or not isinstance(datos.get('Listado'), list):
                                fallo = 'respuesta sin Listado; revisar acceso o servicio'
                            elif num(datos.get('Cantidad'), 'Cantidad API') != len(datos['Listado']):
                                fallo = 'listado incompleto'
                            else:
                                listado = datos['Listado']
                                if codigo and (len(listado)!=1 or listado[0].get('Codigo')!=codigo):
                                    raise RuntimeError('Detalle ausente o codigo inesperado')
                                if codigo and self.cache and isinstance(listado[0].get('Items'),dict):
                                    self.cache.guardar(codigo,listado[0])
                                self.exitos += 1
                                if self.exitos >= 20:
                                    self.pausa = max(self.minimo,self.pausa*0.9)
                                    self.exitos = 0
                                return listado
            except self.requests.RequestException:
                fallo = 'error de red o timeout'
            LOG.warning('Consulta fallida (%s), intento %s/%s; pausa=%.1fs; espera=%.1fs',
                        fallo,intento+1,self.intentos,self.pausa,espera)
            # Incluso en el ultimo intento respetamos Retry-After antes de finalizar.
            fin = time.monotonic()+espera
            while time.monotonic()<fin:
                time.sleep(min(30,max(0,fin-time.monotonic())))
        raise RuntimeError(f'No se pudo consultar la API: {fallo}')


def preparar(cur):
    cur.execute("""ALTER TABLE staging.ordenes_compra_limpias
                   ADD COLUMN IF NOT EXISTS origen_datos text,
                   ADD COLUMN IF NOT EXISTS anio_origen integer""")
    cur.execute('''CREATE TABLE IF NOT EXISTS staging.oc_api_respuestas (
        codigo text PRIMARY KEY, consultado_en timestamptz NOT NULL DEFAULT now(),
        fecha_consulta date NOT NULL, estado text NOT NULL, motivo text,
        respuesta jsonb NOT NULL)''')


def cargar_nueva(con, filas, oc, dia):
    from psycopg import sql
    with con.transaction():
        with con.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout='15s'")
            # Mismo bloqueo que el consolidado anterior; revalida tras bloquear.
            cur.execute('LOCK TABLE staging.ordenes_compra_limpias IN SHARE ROW EXCLUSIVE MODE')
            cur.execute('SELECT 1 FROM staging.ordenes_compra_limpias WHERE codigo=%s LIMIT 1', (oc['Codigo'],))
            if cur.fetchone():
                return False
            columnas = tuple(filas[0])
            stmt = sql.SQL('INSERT INTO staging.ordenes_compra_limpias ({}) VALUES ({})').format(
                sql.SQL(',').join(map(sql.Identifier, columnas)),
                sql.SQL(',').join(sql.Placeholder() for _ in columnas))
            cur.executemany(stmt, [tuple(f[c] for c in columnas) for f in filas])
            guardar_respuesta(cur, oc, dia, 'cargada', None)
    return True


def guardar_respuesta(cur, oc, dia, estado, motivo):
    from psycopg.types.json import Jsonb
    # Decimal se serializa como texto, sin convertir a float ni perder precision.
    cur.execute('''INSERT INTO staging.oc_api_respuestas(codigo,fecha_consulta,estado,motivo,respuesta)
        VALUES (%s,%s,%s,%s,%s) ON CONFLICT (codigo) DO UPDATE SET
        consultado_en=now(), fecha_consulta=excluded.fecha_consulta, estado=excluded.estado,
        motivo=excluded.motivo, respuesta=excluded.respuesta''',
        (str(oc['Codigo']), dia, estado, motivo,
         Jsonb(oc, dumps=lambda x: json.dumps(x, default=str))))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--env', type=Path, default=Path('.env'))
    p.add_argument('--anio', type=int)
    p.add_argument('--desde', type=date.fromisoformat)
    p.add_argument('--hasta', type=date.fromisoformat)
    p.add_argument('--aplicar', action='store_true')
    p.add_argument('--refrescar-api', action='store_true', help='Ignora detalles en cache; consulta otra vez')
    args = p.parse_args()
    from dotenv import dotenv_values
    import psycopg
    config = {**dotenv_values(args.env), **os.environ}
    hoy = datetime.now(ZoneInfo('America/Santiago')).date()
    anio = args.anio or int(config.get('OC_API_ANIO', hoy.year))
    if not 2000 <= anio <= 9998:
        p.error('Año fuera de rango')
    desde = args.desde or date(anio,1,1)
    hasta = args.hasta or min(date(anio,12,31), hoy)
    if desde.year != anio or hasta.year != anio or desde > hasta or hasta > hoy:
        p.error('Rango invalido: dentro del año elegido y sin fechas futuras')
    for nombre in ['PG_HOST','PG_USER','PG_PASSWORD','DB_ORIGEN','MP_API_TICKET']:
        if not config.get(nombre):
            p.error(f'Falta {nombre} en .env')
    log_dir = Path(config.get('LOG_DIR', 'logs'))
    log_dir.mkdir(parents=True, exist_ok=True)
    sello = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s',
                        handlers=[logging.StreamHandler(),logging.FileHandler(log_dir/f'oc_api_{sello}.log', encoding='utf-8')])
    ruta = config.get('OC_PARIDADES_CSV')
    if ruta and not Path(ruta).is_absolute():
        ruta = args.env.resolve().parent / ruta
    tasas = paridades(ruta)
    ruta_cache = Path(config.get('OC_API_CACHE', 'cache/oc_api.sqlite3'))
    if not ruta_cache.is_absolute():
        ruta_cache = args.env.resolve().parent / ruta_cache
    horas = float(config.get('OC_API_CACHE_HORAS','168'))
    if horas <= 0:
        p.error('OC_API_CACHE_HORAS debe ser positivo')
    cache = CacheDetalles(ruta_cache, horas)
    cliente = Cliente(config['MP_API_TICKET'], float(config.get('OC_API_PAUSA_SEG','1')),
                      int(config.get('OC_API_INTENTOS','4')), int(config.get('OC_API_MAX_LLAMADAS','10000')),
                      cache=cache, minimo=float(config.get('OC_API_PAUSA_MIN_SEG','2')),
                      refrescar=args.refrescar_api)
    if cliente.minimo <= 0 or cliente.pausa < 0 or cliente.intentos < 1 or cliente.maximo < 1:
        p.error('Pausa, intentos o presupuesto invalidos')
    conteos, motivos = Counter(), Counter()
    reporte = log_dir / f'oc_api_rechazos_{sello}.csv'
    with psycopg.connect(host=config['PG_HOST'], port=config.get('PG_PORT','5432'),
                         user=config['PG_USER'], password=config['PG_PASSWORD'],
                         dbname=config['DB_ORIGEN'], autocommit=True) as con, \
            reporte.open('w', newline='', encoding='utf-8-sig') as archivo:
        writer = csv.writer(archivo)
        writer.writerow(['codigo_oc', 'fecha_consulta', 'motivo'])
        with con.cursor() as cur:
            cur.execute("SELECT to_regclass('staging.ordenes_compra_limpias')")
            if cur.fetchone()[0] is None:
                raise RuntimeError('Falta staging.ordenes_compra_limpias; este ETL complementa el histórico')
            cur.execute('SELECT DISTINCT codigo FROM staging.ordenes_compra_limpias')
            existentes = {r[0] for r in cur}
            # Solo lectura de los catalogos usados por tu ETL dimensional.
            dimensiones = {}
            for nombre, consulta in {
                'proveedor':'SELECT prov_codigo_proveedor FROM catalog.proveedor',
                'comprador':'SELECT codigo_organismo FROM catalog.organismo',
                'producto':'SELECT codigo_producto FROM catalog.producto WHERE activo=TRUE',
            }.items():
                cur.execute(consulta)
                dimensiones[nombre] = {str(r[0]).strip() if nombre=='producto' else int(r[0]) for r in cur if r[0] is not None}
            # Codigos exactos usados por load_tipo_orden_compra de etl_dw.py.
            dimensiones['tipo'] = {8, 9, 10, 13, 14}
            if args.aplicar:
                with con.transaction():
                    preparar(cur)
        LOG.info('Modo=%s año=%s rango=%s..%s; OC existentes=%s',
                 'APLICAR' if args.aplicar else 'SIMULACION',anio,desde,hasta,len(existentes))
        dia, vistos = desde, set()
        while dia <= hasta:
            listado = cliente.consultar(fecha=dia.strftime('%d%m%Y'), estado='todos')
            conteos['dias_consultados'] += 1
            LOG.info('Día %s: %s OC listadas',dia,len(listado))
            for resumen in listado:
                codigo = texto(resumen.get('Codigo'), 'Codigo listado')
                if codigo in vistos:
                    continue
                vistos.add(codigo)
                conteos['ocs_listadas_unicas'] += 1
                if codigo in existentes:
                    conteos['ocs_existentes_omitidas'] += 1
                    continue
                detalle = cliente.consultar(codigo=codigo)
                if len(detalle) != 1 or detalle[0].get('Codigo') != codigo:
                    raise RuntimeError('Detalle ausente o código inesperado; reejecutar para continuar')
                oc = detalle[0]
                if (conteos['ocs_listadas_unicas'] % 10 == 0):
                    LOG.info('Revisadas=%s/%s; llamadas=%s; cache=%s; validas=%s; excluidas=%s; pausa=%.1fs',
                             conteos['ocs_listadas_unicas'],len(listado),cliente.llamadas,cliente.cache_hits,
                             conteos['ocs_nuevas_validas'],conteos['ocs_excluidas'],cliente.pausa)
                try:
                    filas = transformar(oc, anio, tasas, dimensiones)
                except NoElegible as exc:
                    motivo = str(exc)
                    motivos[motivo.split(':')[0]] += 1
                    conteos['ocs_excluidas'] += 1
                    writer.writerow([codigo, dia, motivo])
                    archivo.flush()
                    if args.aplicar:
                        with con.transaction():
                            with con.cursor() as cur:
                                guardar_respuesta(cur,oc,dia,'excluida',motivo)
                    continue
                if not args.aplicar or cargar_nueva(con,filas,oc,dia):
                    conteos['ocs_nuevas_validas'] += 1
                    conteos['items_nuevos_validos'] += len(filas)
                    existentes.add(codigo)
                else:
                    conteos['ocs_existentes_omitidas'] += 1
                if conteos['ocs_nuevas_validas'] % 100 == 0:
                    LOG.info('Progreso: %s',dict(conteos))
            LOG.info('Día %s terminado; %s; detalles reutilizados=%s',dia,dict(conteos),cliente.cache_hits)
            dia += timedelta(days=1)
    cache.close()
    print(json.dumps({'modo':'aplicado' if args.aplicar else 'simulacion','anio':anio,
                      **conteos,'motivos_exclusion':dict(motivos),'llamadas_api':cliente.llamadas,
                      'detalles_reutilizados_cache':cliente.cache_hits,
                      'reporte':str(reporte),'dw_modificado':False},ensure_ascii=False,indent=2))
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        LOG.error('Interrumpido: las OC confirmadas quedan guardadas. Reejecutar omite las existentes.')
        raise SystemExit(130)
    except Exception as exc:
        # No mostrar mensajes arbitrarios que puedan incluir credenciales de conexion.
        detalle = str(exc) if isinstance(exc, (RuntimeError, NoElegible)) else 'Revisa configuración/conexión y columnas de staging.'
        LOG.error('ETL detenido (%s): %s. Las OC anteriores confirmadas se conservan.',type(exc).__name__,detalle)
        raise SystemExit(1)
