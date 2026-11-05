"""API de licitaciones SmartBids. Python 3.10+.
Dependencias: python -m pip install requests "psycopg[binary]" python-dotenv tzdata

Usa .env: PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, DB_ORIGEN, MP_API_TICKET.
Opcionales: LIC_API_DESDE=2026-07-26, LIC_API_PAUSA_SEG=5,
LIC_API_CACHE=cache/licitaciones_api.sqlite3, LIC_API_CACHE_HORAS=24,
LIC_API_MAX_LLAMADAS=10000, LIC_API_ESTADOS_MAP={} (JSON codigo API -> codigo local).
El mapeo explicito de estados debe verificarse contra catalog.estado_licitacion.
Sin mapeo, busca coincidencia exacta por nombre normalizado en ese catalogo.

PRIMERA PRUEBA (14 codigos especiales mas las existentes sin cierre):
python etl_licitaciones_api.py --solo-sin-cierre
SIMULACION DEL RANGO:
python etl_licitaciones_api.py --desde 2026-07-26
CONFIRMAR:
python etl_licitaciones_api.py --desde 2026-07-26 --aplicar
Se puede limitar --hasta AAAA-MM-DD. --refrescar-api ignora cache.

Simulacion: no escribe en PostgreSQL; si guarda cache y reporte locales.
Aplicacion: nuevas solo en staging.licitaciones_raw; existentes se actualizan
por codigo, sin recrearlas. SOLO actualiza estado y fechas. Conserva siempre lic_codigo_ucom, lic_texto, lic_diccionario_palabras,
lic_codigos_producto, lic_items_cantidad y relaciones licitaciones_productos.
No ejecuta normalizacion, no carga el DW ni envia notificaciones.
No elimina valores con NULL/vacios de la API. lic_plazo lo calcula PostgreSQL.
El reporte incluye antes/despues y advertencias por campo. Nombre, descripcion, presupuesto, moneda, tipo, UCOM y toma de razon no se actualizan.
Raw conserva cada observacion completa con su resultado, incluso para existentes.
No modifica otras tablas. Ejecutar inicialmente en la copia LOCAL de smartbids.
"""
import argparse
import csv
import json
import logging
import os
import re
import sqlite3
import time
import unicodedata
from collections import Counter
from datetime import date, datetime, timedelta
from decimal import Decimal
from email.utils import parsedate_to_datetime
from pathlib import Path
from zoneinfo import ZoneInfo

LOG = logging.getLogger('licitaciones_api')
URL = 'https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json'
ESPECIALES = '''3948-34-LS26 1431291-7-LS25 3948-33-LS26 2215-6-LS26
2189-2-LS26 5482-126-LS25 1488587-16-LS26 2378-71-LS26 4610-87-LS25
3704-7-LS26 425-5-LS26 2775-93-LS26 1488587-22-LS26 3704-22-LS26'''.split()


def normalizar(s):
    return ' '.join(''.join(c for c in unicodedata.normalize('NFD',str(s).casefold())
                           if not unicodedata.combining(c)).split())


def fecha_api(v):
    d = datetime.fromisoformat(str(v).replace('Z','+00:00'))
    if d.year < 2000:
        raise ValueError('fecha vacia/centinela')
    if d.tzinfo:
        d = d.astimezone(ZoneInfo('America/Santiago')).replace(tzinfo=None)
    return d.replace(microsecond=0)


def texto_comparable(valor):
    """Ignora espaciado, CRLF y LF; conserva diferencias de contenido y acentos."""
    return re.sub(r'\s+', ' ', str(valor or '')).strip()


def transformar(api, actual, refs, mapa):
    """Devuelve solo diferencias validas, sin tocar campos derivados/productos."""
    cambios, avisos = {}, []
    def poner(campo, valor):
        if valor is not None and actual.get(campo) != valor:
            cambios[campo] = valor
    fechas = api.get('Fechas') or {}
    if not isinstance(fechas,dict):
        fechas = {}
    for origen,destino,solo_fecha in [
        ('FechaPublicacion','lic_fecha_public',True),
        ('FechaFinal','lic_fecha_preguntas',False),
        ('FechaPubRespuestas','lic_fecha_respuestas',False)]:
        v = fechas.get(origen)
        if v:
            try:
                d = fecha_api(v)
                poner(destino,d.date() if solo_fecha else d)
            except (ValueError,TypeError):
                avisos.append(f'{origen}: fecha invalida; conservada')
    cierre = fechas.get('FechaCierre') or api.get('FechaCierre')
    if cierre:
        try:
            d = fecha_api(cierre)
            poner('lic_fecha_cierre',d.date())
            poner('lic_hora_cierre',d.time())
        except (ValueError,TypeError):
            avisos.append('FechaCierre: invalida; conservada')
    pub = cambios.get('lic_fecha_public',actual.get('lic_fecha_public'))
    fin = cambios.get('lic_fecha_cierre',actual.get('lic_fecha_cierre'))
    if pub and fin and fin < pub:
        for k in ('lic_fecha_public','lic_fecha_cierre','lic_hora_cierre'):
            cambios.pop(k,None)
        avisos.append('cierre anterior a publicacion; cambios de fechas descartados')
    estado_api = str(api.get('CodigoEstado'))
    estado_local = mapa.get(estado_api)
    if estado_local is None:
        etiqueta = normalizar(api.get('Estado') or '')
        candidatos = {r['codigo_estado_licitacion'] for r in refs['estados']
                      if etiqueta and any(normalizar(v)==etiqueta for v in r.values() if isinstance(v,str))}
        if len(candidatos)==1:
            estado_local = candidatos.pop()
    if estado_local in {r['codigo_estado_licitacion'] for r in refs['estados']}:
        poner('lic_estado',estado_local)
    else:
        avisos.append(f'estado API {estado_api} sin equivalencia local inequivoca; conservado')
    return cambios,avisos


class Api:
    def __init__(self,config,refrescar,base):
        import requests
        self.requests=requests
        self.session=requests.Session()
        self.ticket=config['MP_API_TICKET']
        self.pausa=float(config.get('LIC_API_PAUSA_SEG','5'))
        self.maximo=int(config.get('LIC_API_MAX_LLAMADAS','10000'))
        self.horas=float(config.get('LIC_API_CACHE_HORAS','24'))
        if self.pausa<=0 or self.maximo<1 or self.horas<=0:
            raise ValueError('Configuracion de API fuera de rango')
        self.refrescar=refrescar
        ruta=Path(config.get('LIC_API_CACHE','cache/licitaciones_api.sqlite3'))
        if not ruta.is_absolute(): ruta=base/ruta
        ruta.parent.mkdir(parents=True,exist_ok=True)
        self.cache=sqlite3.connect(ruta)
        self.cache.execute('CREATE TABLE IF NOT EXISTS detalle(codigo TEXT PRIMARY KEY,fecha REAL,contenido TEXT)')
        self.cache.commit()
        self.llamadas,self.hits,self.ultima=0,0,0.0

    def consultar(self,**params):
        codigo=params.get('codigo')
        if codigo and not self.refrescar:
            r=self.cache.execute('SELECT fecha,contenido FROM detalle WHERE codigo=?',(codigo,)).fetchone()
            if r and time.time()-r[0]<=self.horas*3600:
                datos=json.loads(r[1],parse_float=Decimal)
                self.hits+=1
                return [datos]
        for intento in range(6):
            if self.llamadas>=self.maximo:
                raise RuntimeError('Presupuesto API agotado; reejecutar reutiliza cache vigente')
            time.sleep(max(0,self.pausa-(time.monotonic()-self.ultima)))
            self.ultima=time.monotonic();self.llamadas+=1
            espera=min(60,2**(intento+1))
            try:
                with self.session.get(URL,params={**params,'ticket':self.ticket},timeout=(10,60),allow_redirects=False) as res:
                    if res.status_code in {429,500,502,503,504}:
                        fallo=f'HTTP {res.status_code}'
                        cab=res.headers.get('Retry-After')
                        if cab:
                            try: espera=max(espera,float(cab))
                            except ValueError:
                                try: espera=max(espera,parsedate_to_datetime(cab).timestamp()-time.time())
                                except (TypeError,ValueError,OverflowError): pass
                        if res.status_code==429:
                            self.pausa=min(60,self.pausa*1.5)
                            espera=max(espera,self.pausa)
                    elif res.status_code!=200:
                        raise RuntimeError(f'API HTTP {res.status_code}; credenciales omitidas')
                    else:
                        try: datos=json.loads(res.text,parse_float=Decimal)
                        except ValueError: datos={}
                        listado=datos.get('Listado') if isinstance(datos,dict) else None
                        if not isinstance(listado,list) or str(datos.get('Cantidad'))!=str(len(listado)):
                            fallo='respuesta incompleta/sin Listado'
                        elif codigo and (len(listado)!=1 or listado[0].get('CodigoExterno')!=codigo):
                            raise ValueError('codigo no encontrado o detalle inesperado')
                        else:
                            if codigo:
                                if not isinstance(listado[0].get('Fechas'),dict) or 'Descripcion' not in listado[0]:
                                    raise ValueError('respuesta no contiene detalle completo')
                                self.cache.execute('INSERT OR REPLACE INTO detalle VALUES (?,?,?)',
                                    (codigo,time.time(),json.dumps(listado[0],default=str,ensure_ascii=False)))
                                self.cache.commit()
                            return listado
            except self.requests.RequestException:
                fallo='timeout/error de red'
            LOG.warning('%s; intento %s/6; espera %.1fs',fallo,intento+1,espera)
            fin=time.monotonic()+espera
            while time.monotonic()<fin: time.sleep(min(30,max(0,fin-time.monotonic())))
        raise RuntimeError(f'API no disponible: {fallo}')


def preparar_raw(cur):
    # Agrega columnas a la tabla vacia suministrada; nunca la elimina.
    cur.execute('CREATE SCHEMA IF NOT EXISTS staging')
    cur.execute('CREATE TABLE IF NOT EXISTS staging.licitaciones_raw ()')
    cur.execute('''ALTER TABLE staging.licitaciones_raw
        ADD COLUMN IF NOT EXISTS api_codigo text,
        ADD COLUMN IF NOT EXISTS api_consultado_en timestamptz DEFAULT now(),
        ADD COLUMN IF NOT EXISTS api_detalle jsonb,
        ADD COLUMN IF NOT EXISTS api_resultado text,
        ADD COLUMN IF NOT EXISTS api_cambios jsonb,
        ADD COLUMN IF NOT EXISTS api_observaciones jsonb''')


def main():
    p=argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--env',type=Path,default=Path('.env'))
    p.add_argument('--desde',type=date.fromisoformat)
    p.add_argument('--hasta',type=date.fromisoformat)
    p.add_argument('--solo-sin-cierre',action='store_true')
    p.add_argument('--aplicar',action='store_true')
    p.add_argument('--refrescar-api',action='store_true')
    args=p.parse_args()
    from dotenv import dotenv_values
    import psycopg
    from psycopg.rows import dict_row
    from psycopg import sql
    from psycopg.types.json import Jsonb
    config={**dotenv_values(args.env),**os.environ}
    for k in ('PG_HOST','PG_USER','PG_PASSWORD','DB_ORIGEN','MP_API_TICKET'):
        if not config.get(k):p.error(f'Falta {k} en .env')
    hoy=datetime.now(ZoneInfo('America/Santiago')).date()
    desde=args.desde or date.fromisoformat(config.get('LIC_API_DESDE','2026-07-26'))
    hasta=args.hasta or hoy
    if desde>hasta or hasta>hoy:p.error('Rango invalido/futuro')
    mapa={str(k):int(v) for k,v in json.loads(config.get('LIC_API_ESTADOS_MAP','{}')).items()}
    logs=Path(config.get('LOG_DIR','logs'));logs.mkdir(parents=True,exist_ok=True)
    sello=datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    logging.basicConfig(level=logging.INFO,format='%(asctime)s | %(levelname)s | %(message)s',
                        handlers=[logging.StreamHandler(),logging.FileHandler(logs/f'licitaciones_api_{sello}.log',encoding='utf-8')])
    api=Api(config,args.refrescar_api,args.env.resolve().parent)
    conteos=Counter()
    reporte=logs/f'licitaciones_cambios_{sello}.csv'
    dumps=lambda x:json.dumps(x,default=str,ensure_ascii=False)
    with psycopg.connect(host=config['PG_HOST'],port=config.get('PG_PORT','5432'),user=config['PG_USER'],
                         password=config['PG_PASSWORD'],dbname=config['DB_ORIGEN'],
                         autocommit=True,row_factory=dict_row) as con, reporte.open('w',encoding='utf-8-sig',newline='') as f:
        w=csv.writer(f);w.writerow(['codigo','resultado','cambios_antes_despues','observaciones'])
        with con.cursor() as cur:
            cur.execute('SELECT * FROM catalog.estado_licitacion'); estados=cur.fetchall()
            cur.execute('SELECT codigo_tipo_licitacion FROM catalog.tipo_licitacion');tipos={r['codigo_tipo_licitacion'] for r in cur}
            cur.execute('SELECT codigo_unidad_compra FROM procurement.unidad_compra');unidades={r['codigo_unidad_compra'] for r in cur}
            cur.execute('SELECT lic_codigo FROM procurement.licitacion WHERE lic_fecha_cierre IS NULL')
            codigos=dict.fromkeys(ESPECIALES+[r['lic_codigo'] for r in cur])
            if not args.solo_sin_cierre:
                cur.execute('SELECT lic_codigo FROM procurement.licitacion WHERE lic_fecha_public BETWEEN %s AND %s ORDER BY lic_fecha_public,lic_codigo',(desde,hasta))
                codigos.update(dict.fromkeys(r['lic_codigo'] for r in cur))
            if args.aplicar:
                with con.transaction():preparar_raw(cur)
        refs=dict(estados=estados,tipos=tipos,unidades=unidades)
        LOG.info('Base=%s modo=%s rango=%s..%s codigos prioritarios=%s',config['DB_ORIGEN'],
                 'APLICAR' if args.aplicar else 'SIMULACION',desde,hasta,len(codigos))
        # Primero las sin cierre; no se depende de que aparezcan en listado diario.
        def procesar(codigo):
            try:
                detalle=api.consultar(codigo=codigo)[0]
            except ValueError as exc:
                w.writerow([codigo,'no_disponible','{}',str(exc)]);f.flush()
                conteos['no_disponibles']+=1
                return
            with con.transaction():
                with con.cursor() as cur:
                    if args.aplicar:
                        cur.execute("SET LOCAL lock_timeout='15s'")
                        # Protege existencia y cambios frente a otras instancias del script.
                        cur.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',(codigo,))
                    cur.execute('SELECT * FROM procurement.licitacion WHERE lic_codigo=%s'+(' FOR UPDATE' if args.aplicar else ''),(codigo,))
                    actual=cur.fetchone()
                    if actual is None:
                        cambios,avisos={},[]
                        resultado='nueva_solo_raw'
                    else:
                        cambios,avisos=transformar(detalle,actual,refs,mapa)
                        resultado='actualizada' if cambios else 'sin_cambios'
                    diferencias={k:{'antes':actual.get(k),'despues':v} for k,v in cambios.items()}
                    if args.aplicar:
                        if cambios:
                            cur.execute(sql.SQL('UPDATE procurement.licitacion SET {} WHERE lic_codigo=%s').format(
                                sql.SQL(',').join(sql.SQL('{}=%s').format(sql.Identifier(k)) for k in cambios)),
                                (*cambios.values(),codigo))
                        # JSON de fuente y delta quedan confirmados junto con la actualizacion.
                        cur.execute('''INSERT INTO staging.licitaciones_raw
                            (api_codigo,api_detalle,api_resultado,api_cambios,api_observaciones)
                            VALUES (%s,%s,%s,%s,%s)''',
                            (codigo,Jsonb(detalle,dumps=dumps),resultado,Jsonb(diferencias,dumps=dumps),Jsonb(avisos)))
            conteos[resultado]+=1
            if avisos:conteos['con_advertencias']+=1
            w.writerow([codigo,resultado,dumps(diferencias),dumps(avisos)]);f.flush()
            n=sum(conteos[k] for k in ('actualizada','sin_cambios','nueva_solo_raw','no_disponibles'))
            if n%10==0:LOG.info('Procesadas=%s llamadas=%s cache=%s %s',n,api.llamadas,api.hits,dict(conteos))
        vistos=set()
        for codigo in codigos:
            procesar(codigo);vistos.add(codigo)
        if not args.solo_sin_cierre:
            dia=desde
            while dia<=hasta:
                listado=api.consultar(fecha=dia.strftime('%d%m%Y'),estado='todos')
                LOG.info('Publicadas %s: %s codigos',dia,len(listado))
                for lic in listado:
                    codigo=lic.get('CodigoExterno')
                    if not isinstance(codigo,str) or not codigo.strip():raise RuntimeError('Listado con codigo ausente')
                    if codigo not in vistos:
                        procesar(codigo);vistos.add(codigo)
                dia+=timedelta(days=1)
    api.cache.close()
    print(dumps({'modo':'aplicado' if args.aplicar else 'simulacion',**conteos,
                 'llamadas_api':api.llamadas,'detalles_cache':api.hits,'reporte':str(reporte),
                 'nota':'En simulacion actualizada significa propuesta. Nuevas nunca insertadas en procurement. Textos y productos conservados.'}))


if __name__=='__main__':
    try:main()
    except KeyboardInterrupt:
        LOG.error('Interrumpido. Cache y transacciones anteriores confirmadas se conservan.')
        raise SystemExit(130)
    except Exception as exc:
        # No imprimir excepciones de conexion/requests: pueden revelar credenciales.
        LOG.error('Detenido (%s). Revisar configuracion, tablas y conexion. Ultima transaccion revertida; anteriores conservadas.',type(exc).__name__)
        raise SystemExit(1)
