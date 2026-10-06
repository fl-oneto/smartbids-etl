"""ETL staging.organismos_raw -> catalog.organismo (Python 3.10+, psycopg 3).

Conexion: .env con PG_HOST, PG_PORT, PG_USER, PG_PASSWORD y DB_ORIGEN.
DB_ORIGEN se usa para staging y catalogo; DB_DESTINO no se utiliza.
Por defecto simula; --aplicar confirma.
Sin --csv utiliza staging existente. --csv reemplaza staging en la misma
transaccion (tambien se revierte en simulacion o ante un error).
Inserta organismos nuevos y resuelve comuna/sector contra los catalogos.
Si hay nombres no vacios sin match unico, reporta y revierte todo.
"""
import argparse
import csv
import json
import os
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

COLUMNAS = [
    'Activo', 'Codigo_Organismo', 'Organismo', 'SIGLA', 'Sector', 'RUT',
    'Dirección', 'Comuna', 'Provincia', 'Región', 'RegiónAbrev',
    'CHILEAVANZA', 'CIS', 'SOLEM', 'TESLA', 'BLUECOMPANY', 'INTRATEC',
    'GDS', 'VALORA', 'EMPRESA3', 'EMPRESA4', 'Descripción', 'Reclamos',
]

CREAR_STAGING = '''CREATE TABLE IF NOT EXISTS staging.organismos_raw (
    "Activo" varchar(10),
    "Codigo_Organismo" varchar(30),
    "Organismo" varchar(300),
    "SIGLA" varchar(50),
    "Sector" varchar(150),
    "RUT" varchar(20),
    "Dirección" varchar(500),
    "Comuna" varchar(150),
    "Provincia" varchar(150),
    "Región" varchar(150),
    "RegiónAbrev" varchar(20),
    "CHILEAVANZA" varchar(10),
    "CIS" varchar(10),
    "SOLEM" varchar(10),
    "TESLA" varchar(10),
    "BLUECOMPANY" varchar(10),
    "INTRATEC" varchar(10),
    "GDS" varchar(10),
    "VALORA" varchar(10),
    "EMPRESA3" varchar(10),
    "EMPRESA4" varchar(10),
    "Descripción" text,
    "Reclamos" varchar(30)
)'''


def limpiar(valor):
    return str(valor).strip() or None if valor is not None else None


def normalizar(valor):
    texto = unicodedata.normalize('NFKD', limpiar(valor) or '')
    texto = ''.join(c for c in texto if not unicodedata.combining(c))
    return ' '.join(texto.casefold().split())


def indice_catalogo(filas):
    indice = defaultdict(set)
    for codigo, nombre in filas:
        indice[normalizar(nombre)].add(codigo)
    return indice


# Equivalencias explicitas a nombres del catalogo proporcionado.
# El codigo siempre se obtiene de catalog.comuna, no se inventa ni se crea.
ALIAS_COMUNAS = {
    'puerto natales': 'natales',
    'san vicente de tagua tagua': 'san vicente',
    'la calera': 'calera',
    'puerto saavedra': 'saavedra',
    'trehuaco': 'treguaco',
}


def resolver_relaciones(filas, comunas, sectores):
    relaciones, incidencias = [], []
    for fila in filas:
        codigos = []
        for campo, indice in [('Comuna', comunas), ('Sector', sectores)]:
            nombre = limpiar(fila.get(campo))
            clave = normalizar(nombre)
            if campo == 'Comuna':
                if clave == 's/i':
                    nombre = None
                    clave = ''
                clave = ALIAS_COMUNAS.get(clave, clave)
            candidatos = sorted(indice.get(clave, set())) if nombre else []
            if nombre and len(candidatos) != 1:
                incidencias.append({'codigo_organismo': int(fila['Codigo_Organismo']),
                    'campo': campo, 'valor': nombre,
                    'problema': 'ambiguo' if candidatos else 'sin_coincidencia',
                    'codigos_candidatos': candidatos})
            codigos.append(candidatos[0] if len(candidatos) == 1 else None)
        relaciones.append(tuple(codigos))
    return relaciones, incidencias


def transformar(filas):
    """Valida el lote completo; no descarta ni trunca registros invalidos."""
    salida, vistos = [], set()
    for numero, fila in enumerate(filas, 1):
        f = {k: limpiar(v) for k, v in fila.items()}
        codigo = f.get('Codigo_Organismo')
        if not codigo or not re.fullmatch(r'[0-9]+', codigo):
            raise ValueError(f'Registro {numero}: codigo invalido')
        codigo = int(codigo)
        if codigo > 9223372036854775807:
            raise ValueError(f'Registro {numero}: codigo excede bigint')
        if codigo in vistos:
            raise ValueError(f'Codigo duplicado: {codigo}')
        vistos.add(codigo)
        activo = (f.get('Activo') or '').upper()
        if activo not in ('SI', 'NO'):
            raise ValueError(f'{codigo}: Activo debe ser SI o NO')
        if not f.get('Organismo'):
            raise ValueError(f'{codigo}: nombre obligatorio')
        for campo, limite in [('Organismo', 200), ('SIGLA', 30),
                              ('RUT', 15), ('Dirección', 300)]:
            if len(f.get(campo) or '') > limite:
                raise ValueError(f'{codigo}: {campo} excede {limite} caracteres')
        reclamos = f.get('Reclamos')
        if reclamos is not None:
            if not re.fullmatch(r'[0-9]+', reclamos):
                raise ValueError(f'{codigo}: Reclamos debe ser entero no negativo')
            reclamos = int(reclamos)
            if reclamos > 2147483647:
                raise ValueError(f'{codigo}: Reclamos excede integer')
        salida.append((codigo, activo == 'SI', f['Organismo'],
                       f.get('SIGLA'), f.get('RUT'), reclamos, f.get('Dirección')))
    if not salida:
        raise ValueError('El origen esta vacio; se cancela el ETL')
    return salida


def leer_csv(ruta):
    with Path(ruta).open(encoding='utf-8-sig', newline='') as archivo:
        lector = csv.DictReader(archivo)
        if lector.fieldnames != COLUMNAS:
            raise ValueError('Encabezados u orden distintos del CSV Organismo original')
        filas = list(lector)
    if any(None in f or any(v is None for v in f.values()) for f in filas):
        raise ValueError('CSV con filas incompletas o columnas adicionales')
    transformar(filas)  # Validar antes de reemplazar staging.
    return filas


def ejecutar_etl(dsn='', *, csv_path=None, aplicar=False):
    """Abre su propia conexion y administra una unica transaccion completa."""
    import psycopg
    from psycopg import sql
    filas_csv = leer_csv(csv_path) if csv_path else None
    with psycopg.connect(dsn) as conexion:
        try:
            with conexion.cursor() as cur:
                cur.execute("SET LOCAL lock_timeout = '10s'")
                cur.execute('CREATE SCHEMA IF NOT EXISTS staging')
                cur.execute(CREAR_STAGING)
                cur.execute('LOCK TABLE staging.organismos_raw IN SHARE ROW EXCLUSIVE MODE')
                if filas_csv is not None:
                    # DELETE evita restricciones de TRUNCATE por claves foraneas.
                    cur.execute('DELETE FROM staging.organismos_raw')
                    comando = sql.SQL('COPY staging.organismos_raw ({}) FROM STDIN').format(
                        sql.SQL(', ').join(map(sql.Identifier, COLUMNAS)))
                    with cur.copy(comando) as copia:
                        for fila in filas_csv:
                            copia.write_row(tuple(fila[k] or None for k in COLUMNAS))
                cur.execute('SELECT "Codigo_Organismo", "Activo", "Organismo", '
                            '"SIGLA", "RUT", "Reclamos", "Dirección", "Comuna", "Sector" '
                            'FROM staging.organismos_raw')
                campos = [c.name for c in cur.description]
                filas = [dict(zip(campos, fila)) for fila in cur.fetchall()]
                datos = transformar(filas)
                cur.execute('LOCK TABLE catalog.comuna, catalog.sector IN SHARE MODE')
                cur.execute('SELECT codigo_comuna, nombre_comuna FROM catalog.comuna')
                comunas = indice_catalogo(cur.fetchall())
                cur.execute('SELECT codigo_sector, nombre_sector FROM catalog.sector')
                sectores = indice_catalogo(cur.fetchall())
                relaciones, incidencias = resolver_relaciones(filas, comunas, sectores)
                if incidencias:
                    conexion.rollback()
                    return {'modo': 'cancelado', 'filas_origen': len(datos),
                            'motivo': 'Resolver nombres sin match unico y volver a ejecutar',
                            'incidencias': incidencias, 'insertados': 0, 'actualizados': 0}
                cur.execute('''CREATE TEMP TABLE etl_org_src (
                    codigo_organismo bigint PRIMARY KEY, activo boolean,
                    org_nombre varchar(200), org_sigla varchar(30),
                    org_rut varchar(15), org_reclamos integer,
                    org_direccion varchar(300), org_codigo_comuna varchar(5),
                    org_codigo_sector varchar(3)) ON COMMIT DROP''')
                with cur.copy('COPY etl_org_src FROM STDIN') as copia:
                    for fila, relacion in zip(datos, relaciones):
                        copia.write_row(fila + relacion)
                cur.execute('LOCK TABLE catalog.organismo IN SHARE ROW EXCLUSIVE MODE')
                cur.execute('''SELECT s.codigo_organismo FROM etl_org_src s
                    LEFT JOIN catalog.organismo o USING (codigo_organismo)
                    WHERE o.codigo_organismo IS NULL ORDER BY s.codigo_organismo''')
                faltantes = [f[0] for f in cur.fetchall()]
                cur.execute('''CREATE TEMP TABLE etl_org_cambios ON COMMIT DROP AS
                    SELECT o.codigo_organismo, s.activo, s.org_nombre,
                      COALESCE(s.org_sigla,o.org_sigla) AS org_sigla,
                      COALESCE(s.org_rut,o.org_rut) AS org_rut,
                      COALESCE(s.org_reclamos,o.org_reclamos) AS org_reclamos,
                      COALESCE(s.org_direccion,o.org_direccion) AS org_direccion,
                      COALESCE(s.org_codigo_comuna,o.org_codigo_comuna) AS org_codigo_comuna,
                      COALESCE(s.org_codigo_sector,o.org_codigo_sector) AS org_codigo_sector
                    FROM catalog.organismo o JOIN etl_org_src s USING (codigo_organismo)
                    WHERE ROW(o.activo,o.org_nombre,o.org_sigla,o.org_rut,
                              o.org_reclamos,o.org_direccion,o.org_codigo_comuna,o.org_codigo_sector)
                      IS DISTINCT FROM ROW(s.activo,s.org_nombre,
                        COALESCE(s.org_sigla,o.org_sigla),COALESCE(s.org_rut,o.org_rut),
                        COALESCE(s.org_reclamos,o.org_reclamos),
                        COALESCE(s.org_direccion,o.org_direccion),
                        COALESCE(s.org_codigo_comuna,o.org_codigo_comuna),
                        COALESCE(s.org_codigo_sector,o.org_codigo_sector))''')
                cur.execute('SELECT codigo_organismo FROM etl_org_cambios ORDER BY codigo_organismo')
                cambios = [f[0] for f in cur.fetchall()]
                actualizados = 0
                insertados = 0
                if aplicar:
                    cur.execute('''UPDATE catalog.organismo o SET
                        activo=c.activo, org_nombre=c.org_nombre,
                        org_sigla=c.org_sigla, org_rut=c.org_rut,
                        org_reclamos=c.org_reclamos, org_direccion=c.org_direccion,
                        org_codigo_comuna=c.org_codigo_comuna, org_codigo_sector=c.org_codigo_sector
                        FROM etl_org_cambios c WHERE o.codigo_organismo=c.codigo_organismo''')
                    actualizados = cur.rowcount
                    cur.execute('''INSERT INTO catalog.organismo (
                        codigo_organismo, activo, org_nombre, org_sigla, org_rut,
                        org_reclamos, org_direccion, org_codigo_comuna, org_codigo_sector)
                        SELECT s.codigo_organismo, s.activo, s.org_nombre, s.org_sigla,
                        s.org_rut, s.org_reclamos, s.org_direccion,
                        s.org_codigo_comuna, s.org_codigo_sector FROM etl_org_src s
                        WHERE NOT EXISTS (SELECT 1 FROM catalog.organismo o
                            WHERE o.codigo_organismo=s.codigo_organismo)''')
                    insertados = cur.rowcount
            if aplicar:
                conexion.commit()
            else:
                conexion.rollback()
            return {
                'modo': 'aplicado' if aplicar else 'simulacion',
                'origen': 'csv' if csv_path else 'staging',
                'filas_origen': len(datos), 'existentes': len(datos)-len(faltantes),
                'sin_cambios': len(datos)-len(faltantes)-len(cambios),
                'a_actualizar': len(cambios), 'actualizados': actualizados,
                'a_insertar': len(faltantes), 'insertados': insertados,
                'codigos_nuevos': faltantes, 'codigos_con_cambios': cambios,
            }
        except Exception:
            conexion.rollback()
            raise


def main():
    from dotenv import dotenv_values
    from psycopg.conninfo import make_conninfo

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env', type=Path, default=Path('.env'),
                        help='Ruta al .env (por defecto .env del directorio actual)')
    parser.add_argument('--csv', type=Path, help='Reemplaza staging con este CSV')
    parser.add_argument('--aplicar', action='store_true', help='Confirma los cambios')
    args = parser.parse_args()
    if not args.env.is_file():
        parser.error(f'No se encontro el archivo {args.env}')
    config = dotenv_values(args.env)
    # Variables del proceso tienen prioridad sobre las del archivo .env.
    claves = ['PG_HOST', 'PG_PORT', 'PG_USER', 'PG_PASSWORD', 'DB_ORIGEN']
    valores = {k: os.getenv(k, config.get(k)) for k in claves}
    faltantes = [k for k, v in valores.items() if not v]
    if faltantes:
        parser.error('Faltan variables: ' + ', '.join(faltantes))
    if valores['PG_PASSWORD'] == 'reemplazar_con_tu_contrasena':
        parser.error('Reemplaza PG_PASSWORD por tu contraseña real en el .env')
    dsn = make_conninfo(host=valores['PG_HOST'], port=valores['PG_PORT'],
                       user=valores['PG_USER'], password=valores['PG_PASSWORD'],
                       dbname=valores['DB_ORIGEN'], connect_timeout=10)
    print(f"Base utilizada para staging y catalogo: {valores['DB_ORIGEN']}")
    resultado = ejecutar_etl(dsn,
                            csv_path=args.csv, aplicar=args.aplicar)
    print(json.dumps(resultado, ensure_ascii=False, indent=2))
    if resultado['modo'] == 'cancelado':
        raise SystemExit(2)


if __name__ == '__main__':
    main()
