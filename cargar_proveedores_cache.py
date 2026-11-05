"""Completa catalog.proveedor desde el reporte de rechazos y la cache local.
No consulta la API. Simula sin escribir salvo --aplicar. No carga staging ni DW.
Usar junto al .env del ETL. Requiere psycopg 3 y python-dotenv ya instalados.
"""
import argparse
import csv
import json
import os
import sqlite3
from collections import defaultdict
from pathlib import Path


def codigo(v, campo):
    s = str(v).strip() if v is not None else ''
    if not s.isascii() or not s.isdigit() or not 0 <= int(s) < 2**63:
        raise ValueError(f'{campo}: codigo ausente o invalido')
    return int(s)


def cadena(v, campo, limite):
    s = str(v).strip() if v is not None else ''
    if not s or s.lower() in {'null', 'none', 'nan'}:
        return None
    if len(s) > limite:
        raise ValueError(f'{campo}: supera {limite} caracteres; revisar antes de cargar')
    return s


def extraer(reporte, cache):
    grupos, errores = defaultdict(list), []
    # Abre cache existente en modo solo lectura, nunca crea una cache vacia.
    with sqlite3.connect(cache.resolve().as_uri()+'?mode=ro', uri=True) as db:
        with reporte.open(encoding='utf-8-sig', newline='') as f:
            for r in csv.DictReader(f):
                motivo = r.get('motivo', '')
                if not motivo.startswith('dimension_faltante:proveedor:'):
                    continue
                oc = r['codigo_oc'].strip()
                esperado = None
                try:
                    esperado = codigo(motivo.rsplit(':',1)[1], 'proveedor del reporte')
                    registro = db.execute('SELECT contenido FROM detalles WHERE codigo=?',(oc,)).fetchone()
                    if not registro:
                        raise ValueError('OC no encontrada en cache')
                    dato = json.loads(registro[0])
                    if dato.get('Codigo') != oc:
                        raise ValueError('codigo OC no coincide con cache')
                    prov = dato.get('Proveedor') or {}
                    cod = codigo(prov.get('Codigo'), 'Codigo')
                    if cod != esperado:
                        raise ValueError('proveedor no coincide con reporte')
                    suc = codigo(prov.get('CodigoSucursal'), 'CodigoSucursal')
                    nombre = cadena(prov.get('Nombre'), 'Nombre', 250)
                    if not nombre:
                        raise ValueError('Nombre del proveedor ausente')
                    grupos[cod].append((cod,suc,cadena(prov.get('RutSucursal'),'RutSucursal',25),
                                        nombre,cadena(prov.get('Direccion'),'Direccion',200)))
                except (ValueError, TypeError, KeyError) as exc:
                    errores.append({'codigo_oc':oc,'motivo':str(exc)})
                    # No cargar un proveedor a medias si una respuesta suya falla.
                    if esperado is not None:
                        grupos[esperado].append(None)
    filas = []
    for cod, registros in grupos.items():
        distintos = set(registros)
        if None in distintos or len(distintos)!=1:
            errores.append({'proveedor':cod,'motivo':'datos/sucursales inconsistentes entre OCs; revisar'})
        else:
            filas.append(next(iter(distintos)))
    return filas, errores


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--env',type=Path,default=Path('.env'))
    p.add_argument('--reporte',type=Path,required=True)
    p.add_argument('--cache',type=Path)
    p.add_argument('--aplicar',action='store_true')
    args = p.parse_args()
    from dotenv import dotenv_values
    import psycopg
    config = {**dotenv_values(args.env),**os.environ}
    cache = args.cache or Path(config.get('OC_API_CACHE','cache/oc_api.sqlite3'))
    if not cache.is_absolute():
        cache = args.env.resolve().parent/cache
    if not args.reporte.is_file() or not cache.is_file():
        p.error('No existe el reporte o la cache indicada')
    filas, errores = extraer(args.reporte,cache)
    for k in ('PG_HOST','PG_USER','PG_PASSWORD','DB_ORIGEN'):
        if not config.get(k):
            p.error(f'Falta {k} en .env')
    nuevos, existentes = [], []
    with psycopg.connect(host=config['PG_HOST'],port=config.get('PG_PORT','5432'),
                         user=config['PG_USER'],password=config['PG_PASSWORD'],
                         dbname=config['DB_ORIGEN']) as con:
        with con.cursor() as cur:
            if args.aplicar:
                cur.execute("SET LOCAL lock_timeout='15s'")
                cur.execute('LOCK TABLE catalog.proveedor IN SHARE ROW EXCLUSIVE MODE')
            for fila in filas:
                # El DW actual usa proveedor_codigo unico, aunque catalog permite sucursales.
                # No añadimos sucursales de un proveedor que ya existe.
                cur.execute('SELECT 1 FROM catalog.proveedor WHERE prov_codigo_proveedor=%s LIMIT 1',(fila[0],))
                if cur.fetchone():
                    existentes.append(fila[0])
                    continue
                nuevos.append({'codigo':fila[0],'sucursal':fila[1],'nombre':fila[3]})
                if args.aplicar:
                    cur.execute('''INSERT INTO catalog.proveedor
                        (prov_codigo_proveedor,prov_codigo_sucursal,prov_rut,prov_nombre,prov_direccion)
                        VALUES (%s,%s,%s,%s,%s)''',fila)
            if not args.aplicar:
                con.rollback()
    print(json.dumps({'modo':'aplicado' if args.aplicar else 'simulacion',
                      'proveedores_nuevos':nuevos,'existentes_omitidos':existentes,
                      'casos_para_revisar':errores,
                      'nota':'No modifica proveedores existentes, staging ni DW. FK no resueltas y razon social quedan NULL.'},
                     ensure_ascii=False,indent=2))


if __name__=='__main__':
    main()
