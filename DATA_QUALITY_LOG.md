# Bitácora de calidad de datos de SmartBids

Este documento registra las inconsistencias detectadas durante la preparación de
la base operacional y del Data Warehouse de SmartBids, junto con su evidencia,
tratamiento y estado. Su objetivo es mantener trazabilidad de las decisiones del
ETL sin modificar los datos originales almacenados en `staging`.

## Estados

- **Resuelta:** la corrección fue aplicada y validada.
- **Aceptada:** el dato es inusual, pero corresponde al comportamiento real del origen.
- **En progreso:** la causa fue identificada y la reparación aún no termina.
- **Pendiente:** requiere investigación adicional.

## Resumen

| ID | Incidencia | Estado |
|---|---|---|
| DQ-001 | Código de unidad de compra de las órdenes no compatible con el catálogo | Resuelta |
| DQ-002 | Total neto de línea con descuentos y cargos aplicados dos veces | Resuelta |
| DQ-003 | Órdenes sin fecha de aceptación | Aceptada |
| DQ-004 | Monedas diferentes entre orden e ítem | Aceptada con exclusión controlada |
| DQ-005 | Productos de licitación almacenados como conjunto textual | Aceptada con limitación documentada |
| DQ-006 | Valor `set()` interpretado como código de producto | Pendiente de aplicar |
| DQ-007 | Catálogos incompletos que excluyeron licitaciones y productos | En progreso |

## DQ-001 — Código de unidad de compra en órdenes

**Detección:** el campo `codigo_unidad_compra` del archivo de órdenes de compra
no coincidía con `procurement.unidad_compra.codigo_unidad_compra`.

**Causa:** ambos valores pertenecen a codificaciones diferentes. El prefijo de
`codigo_orden`, ubicado antes del primer guion, sí corresponde al código público
de la unidad de compra.

**Decisión:** obtener la unidad mediante:

```sql
SPLIT_PART(codigo_orden, '-', 1)::bigint
```

**Corrección:** el ETL utiliza el prefijo de `codigo_orden` para resolver la
dimensión comprador. El campo incompatible del CSV no se usa para ese cruce.

**Estado:** resuelta.

## DQ-002 — Total neto de los ítems de órdenes de compra

**Detección:** algunos valores de `total_linea_neto` eran negativos o no
conciliaban con `total_neto_oc`. Se verificaron ejemplos directamente contra el
detalle publicado en Mercado Público.

**Causa:** el archivo exportado vuelve a aplicar ciertos descuentos y cargos al
calcular `total_linea_neto`.

**Ejemplos investigados:**

- Ítems completamente descontados aparecían con totales negativos.
- La orden `1233616-13-SE24` descontaba nuevamente $1.402.784.
- La orden `2677-1-SE24` agregaba nuevamente $56.000 en cargos.

**Decisión:** no copiar `total_linea_neto` directamente. Reconstruirlo mediante:

```text
cantidad × precio_neto − total_descuentos + total_cargos
```

**Validación:**

- 60.386 órdenes revisadas.
- 60.386 órdenes conciliadas.
- 0 órdenes con diferencias monetarias materiales.
- Diferencia máxima: `0.00003142`, atribuible a precisión decimal.
- 0 ítems negativos después de la corrección.

**Estado:** resuelta e incorporada en `etl_dw.py`.

## DQ-003 — Órdenes sin fecha de aceptación

**Detección:** 900 órdenes no tenían fecha de aceptación.

**Análisis:** correspondían a órdenes aún no confirmadas:

- 850 enviadas al proveedor.
- 42 en proceso.
- 8 con cancelación solicitada.

Todas tenían `orden_es_confirmada = false`.

**Decisión:** conservar `fecha_aceptacion = NULL`. Para análisis de compras
efectivas, filtrar por:

```sql
WHERE orden_es_confirmada = true
```

**Estado:** aceptada; no requiere corrección.

## DQ-004 — Diferencia de moneda entre orden e ítem

**Detección:** se encontraron 6 órdenes, con 17 ítems, donde la moneda de la
orden no coincidía con la moneda del ítem.

**Decisión:** excluir estas órdenes del alcance inicial de conciliación y
mantener la evidencia en staging. No realizar conversiones sin una regla y una
fecha de tipo de cambio verificables.

**Estado:** aceptada con exclusión controlada. Puede revisarse en una futura
ampliación multimoneda.

## DQ-005 — Productos y cantidad de las licitaciones

**Detección:** `staging.licitaciones_raw.codigo_producto` es texto con formato
de conjunto, por ejemplo `{codigo1,codigo2,codigo3}`. `items_cantidad` contiene
la cantidad total de unidades, no la cantidad asignada a cada producto.

**Validación:** no se encontraron códigos repetidos dentro de una misma fila
del conjunto original.

**Decisión:**

- Mantener `procurement.licitaciones_productos` como relación entre licitación
  y producto, con PK `(lic_codigo, codigo_producto)`.
- Mantener `items_cantidad` a nivel de licitación.
- No repartir la cantidad total entre productos, porque el origen no entrega
  esa distribución.
- Usar los ítems de órdenes de compra para analizar cantidades adjudicadas por
  producto.

**Estado:** aceptada con limitación documentada.

## DQ-006 — Código de producto `set()`

**Detección:** dos licitaciones presentaron `set()` como si fuera un código de
producto:

- `638-41-O125`
- `638-43-O125`

**Causa:** `set()` representa un conjunto vacío generado durante una
transformación; no es un producto real.

**Decisión:** conservar el valor original en staging y excluir los marcadores
vacíos únicamente durante la transformación hacia
`procurement.licitaciones_productos`.

**Corrección definida:** la carga debe separar el conjunto textual, aceptar
solo códigos reales y verificar que la licitación y el producto existan antes
de insertar la asociación:

```sql
INSERT INTO procurement.licitaciones_productos (
    lic_codigo,
    codigo_producto
)
SELECT DISTINCT
    r.licitacion_codigo,
    TRIM(p.codigo_producto)
FROM staging.licitaciones_raw r
CROSS JOIN LATERAL regexp_split_to_table(
    TRIM(BOTH '{}' FROM r.codigo_producto),
    '\s*,\s*'
) AS p(codigo_producto)
JOIN procurement.licitacion l
  ON l.lic_codigo = r.licitacion_codigo
JOIN catalog.producto pr
  ON pr.codigo_producto::text = TRIM(p.codigo_producto)
WHERE r.licitacion_codigo IS NOT NULL
  AND r.codigo_producto IS NOT NULL
  AND TRIM(r.codigo_producto) NOT IN ('', '{}')
  AND LOWER(TRIM(p.codigo_producto))
      NOT IN ('', 'set()', 'null', 'none')
ON CONFLICT (lic_codigo, codigo_producto) DO NOTHING;
```

La presencia de los `JOIN` impide crear asociaciones con licitaciones o
productos inexistentes. `ON CONFLICT` permite reejecutar la carga sin generar
duplicados. La consulta debe volver a ejecutarse después de recuperar las
licitaciones faltantes.

**Validación posterior:**

```sql
SELECT COUNT(*) AS asociaciones_invalidas
FROM procurement.licitaciones_productos
WHERE LOWER(TRIM(codigo_producto))
      IN ('', 'set()', 'null', 'none');
```

El resultado esperado es `0`.

**Estado:** corrección definida, pendiente de incorporar o ejecutar en el
proceso de carga. No se debe crear un producto `set()` en el catálogo.

## DQ-007 — Catálogos incompletos y carga en cascada

**Detección inicial:**

| Control | Origen | Destino | Diferencia |
|---|---:|---:|---:|
| Licitaciones | 66.366 | 62.650 | 3.716 |
| Asociaciones licitación-producto | 134.619 | 127.206 | 7.413 |

De las 7.413 asociaciones faltantes, 7.411 correspondían a licitaciones no
cargadas y 2 al marcador inválido `set()`.

**Causa identificada:** las 3.716 licitaciones dependían de 222 unidades de
compra ausentes en `procurement.unidad_compra`:

- 156 unidades estaban disponibles en `staging.unidad_compra_raw`.
- 66 unidades solo aparecían en `staging.licitaciones_raw`.
- Las unidades dependían de organismos mediante una FK.
- 45 unidades referenciaban 35 organismos existentes.
- 177 unidades referenciaban 89 organismos faltantes.

De los 89 organismos faltantes:

- 74 estaban disponibles en `staging.organismos_raw`.
- 15 solo aparecían en `staging.licitaciones_raw`.
- Los 74 organismos del catálogo raw tenían sector válido.
- 8 tenían una coincidencia segura de comuna y provincia.
- Para los demás no se asignó una comuna dudosa; se conservó `NULL`.

**Corrección aplicada hasta ahora:** se cargaron los 74 organismos respaldados
por `staging.organismos_raw`. La validación posterior dejó 15 organismos
pendientes.

**Regla de integridad:** no se desactivan las FK. Las entidades deben cargarse
en este orden:

```text
catálogos geográficos y sector
→ organismos
→ unidades de compra
→ licitaciones
→ asociaciones licitación-producto
```

**Pendientes:**

1. Cargar de forma mínima y trazable los 15 organismos ausentes del catálogo raw.
2. Cargar las 222 unidades de compra faltantes.
3. Recargar las 3.716 licitaciones.
4. Recargar las 7.411 asociaciones válidas.
5. Confirmar los resultados esperados:
   - 66.366 licitaciones.
   - 134.617 asociaciones válidas.
   - 0 asociaciones faltantes.

**Estado:** en progreso.

## Criterios generales aplicados

- Staging conserva el dato original para trazabilidad.
- Una corrección debe contar con evidencia y una validación reproducible.
- No se inventan valores cuando el origen no entrega suficiente detalle.
- Los valores desconocidos se mantienen como `NULL` cuando el modelo lo permite.
- No se desactivan claves foráneas para forzar cargas incompletas.
- Los procesos incrementales deben usar `UPSERT` y poder reejecutarse.
- Las validaciones deben comparar conteos, claves faltantes y conciliaciones
  monetarias después de cada carga.

## Plantilla para nuevas incidencias

```markdown
## DQ-XXX — Nombre de la incidencia

**Fecha:** AAAA-MM-DD  
**Capa:** staging / operacional / DW  
**Detección:**  
**Evidencia:**  
**Causa:**  
**Decisión:**  
**Corrección:**  
**Validación:**  
**Estado:** pendiente / en progreso / resuelta / aceptada
```
