# Procesos a desarrollar

Ingesta y Limpieza (ETL):

Descarga: Obtén el dataset histórico (2015-2025) desde Datos Abiertos o la fuente interna del IGAC.

Stack Tecnológico: Recomiendo usar Python con librerías eficientes para grandes volúmenes como Polars o Dask (más rápidos que Pandas para 34M de filas) y SQL para almacenamiento estructurado.

Estandarización Básica (OE1):

Normalizar nombres de municipios (ej: "Bogota D.C." vs "Bogotá").

Convertir todas las monedas a pesos corrientes del año actual (ajustando por inflación/IPC) para que los precios de 2015 sean comparables con 2025.

Limpiar columnas críticas: Valor de Transacción, Área del Terreno, Área Construida.

Definición de la "Normalidad":

Antes de buscar anomalías, debes definir qué es lo normal.

Calcula estadísticas descriptivas por municipio: Promedio del valor del $m^2$, desviación estándar, y volumen promedio de transacciones por mes.
