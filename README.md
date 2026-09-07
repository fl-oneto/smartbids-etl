# ETL SmartBids

Carga incremental desde la base operacional `smartbids` hacia
`smartbids_dw`.

## Requisitos

- Python 3.11 o superior.
- PostgreSQL accesible desde el equipo local.
- `dw.dim_fecha` ya poblada para cubrir las fechas de las órdenes.
- La vista `staging.vw_oc_limpias` debe incluir las columnas
  `proveedor_sucursal_codigo`, `proveedor_sucursal_rut` y
  `proveedor_sucursal_nombre`.

## Preparación en PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Edita `.env` e ingresa la contraseña real de PostgreSQL. El archivo `.env`
está excluido de Git.

## Ejecución

```powershell
python etl_dw.py
```

## Registro de ejecución

Cada ejecución muestra el avance en la consola y también lo guarda en:

```text
logs/etl_dw.log
```

El registro incluye el inicio y término, duración, filas procesadas por tabla,
conteos de validación y el detalle del error si la carga falla. El archivo rota
al alcanzar 5 MB y conserva hasta cinco archivos anteriores. La carpeta
`logs/` está excluida de Git.

Para guardar los registros en otra ubicación, define opcionalmente `LOG_DIR`
en `.env`, por ejemplo:

```env
LOG_DIR=H:\\Logs\\smartbids-etl
```

La carga se ejecuta en este orden:

1. `dw.dim_producto`
2. `dw.dim_proveedor`
3. `dw.dim_comprador`
4. `dw.fact_orden_compra`
5. `dw.fact_item_orden_compra`

Todas las tablas usan `UPSERT`, por lo que el proceso puede ejecutarse otra
vez después de actualizar staging. Si falla alguna etapa, se revierte toda la
transacción de la base de destino.

Para `dim_comprador`, el código público de la unidad se extrae desde el prefijo
de `codigo_orden`. El campo `codigo_unidad_compra` del archivo de órdenes no se
usa para cruzar con el catálogo porque pertenece a otra codificación.

`fact_orden_compra` y `fact_item_orden_compra` se cargan como hechos
independientes que comparten las dimensiones de fecha, proveedor y comprador.
El hecho de ítems conserva `orden_codigo` como dimensión degenerada, pero no
depende de una relación directa con el hecho de órdenes.

En `fact_item_orden_compra`, `item_id_mercado_publico` conserva el identificador
estable que entrega el sistema de origen y permite que el `UPSERT` evite
duplicados. `item_unidad_medida` puede contener unidades físicas (`Unidad`,
`Caja`, `Saco`) o unidades de contratación de servicios (`Mes`, `Global`). La
validación de coincidencia entre monedas permanece en staging y no se almacena
en la tabla de hechos.

El valor `item_total_linea_neto` no se copia desde `total_linea_neto` del CSV,
porque el archivo vuelve a aplicar algunos descuentos y cargos. El ETL lo
reconstruye mediante `cantidad * precio_neto - total_descuentos + total_cargos`.
Esta fórmula fue conciliada contra `total_neto_oc` para las 60.386 órdenes, sin
diferencias monetarias materiales.
