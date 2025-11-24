# Procesos a desarrollar

## Fase 1: Cimientos de Datos y "Zona Cero"

1. Ingesta y Limpieza (ETL):

- Descarga: Obtén el dataset histórico (2015-2025) desde Datos Abiertos o la fuente interna del IGAC.
- Stack Tecnológico: Recomiendo usar Python con librerías eficientes para grandes volúmenes como Polars o Dask (más rápidos que Pandas para 34M de filas) y SQL para almacenamiento estructurado.
- Estandarización Básica (OE1):
  - Normalizar nombres de municipios (ej: "Bogota D.C." vs "Bogotá").
  - Convertir todas las monedas a pesos corrientes del año actual (ajustando por inflación/IPC) para que los precios de 2015 sean comparables con 2025.
  - Limpiar columnas críticas: Valor de Transacción, Área del Terreno, Área Construida.

2. Definición de la "Normalidad":

- Antes de buscar anomalías, debes definir qué es lo normal.
- Calcula estadísticas descriptivas por municipio: Promedio del valor del $m^2$, desviación estándar, y volumen promedio de transacciones por mes.

## Fase 2: Detección Basada en Reglas y Estadística

Aquí atacarás las anomalías "obvias" (OE2 y OE3). No necesitas Inteligencia Artificial compleja todavía, solo lógica de negocio sólida.

1. Implementación de Reglas de Negocio (Hard Rules):

- Regla de Rango: Marcar transacciones donde el valor sea < 10% del avalúo catastral (posible evasión fiscal o error de dedo).
- Regla de Tiempo: Identificar propiedades vendidas más de 2 veces en menos de 6 meses (posible "flipping" fraudulento o lavado de activos).
- Regla de Integridad: Transacciones con valor > 0 pero área = 0.

2. Detección Estadística (Z-Score):

- Para cada municipio, calcula el Z-Score del precio por $m^2$.
- Cualquier transacción con un Z-Score > 3 (es decir, 3 desviaciones estándar por encima de la media) o < -3 es una anomalía estadística y debe ser marcada para revisión.

## Fase 3: Machine Learning y Patrones Complejos

Una vez limpias los errores básicos, usas ML para encontrar lo que el ojo humano no ve (OE2).

1. Modelado No Supervisado:

- Al no tener (probablemente) una etiqueta de "fraude confirmado", usarás aprendizaje no supervisado.
- Algoritmo Recomendado: Isolation Forest. Este algoritmo es excelente para aislar observaciones que son "raras" en un espacio multidimensional (precio, área, ubicación, fecha).
- Variables a usar: Latitud, Longitud, Área, Valor, Tipo de Predio, Estrato.

2. Análisis Geoespacial (Clústeres):

- Usa librerías como Geopandas o PySAL.
- Detecta Outliers Espaciales: Una casa que cuesta 500 millones en un barrio donde el promedio es 100 millones (anomalía local), aunque 500 millones sea normal en otra parte de la ciudad.
