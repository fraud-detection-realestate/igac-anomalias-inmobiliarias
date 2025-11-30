# Procesos Desarrollados

## Fase 1: Cimientos de Datos (ETL y Estandarización)

1. **Ingesta y Limpieza:**
   - Procesamiento de >30M de registros históricos (2015-2025).
   - **Stack:** Python + Polars (para alto rendimiento).
   - **Limpieza:** Eliminación de duplicados, manejo de nulos en columnas críticas (`VALOR`, `FECHA`).
2. **Estandarización Avanzada (OE1):**
   - Normalización de nombres de municipios (Mapeo "BOGOTA DC" -> "BOGOTÁ").
   - **Ajuste por Inflación:** Cálculo del `VALOR_CONSTANTE_2024` usando datos históricos del IPC para hacer comparables los precios de 2015 con los de hoy.

## Fase 2: Detección de Anomalías con Machine Learning (OE2)

1. **Modelado No Supervisado (Isolation Forest):**
   - Al carecer de etiquetas de fraude confirmadas, se implementó el algoritmo **Isolation Forest**.
   - **Features:** Precio constante, dinámica inmobiliaria, número de anotaciones, ubicación y naturaleza jurídica.
   - **Resultado:** Detección automática del top 1% de transacciones más divergentes ("raras") del patrón nacional.

## Fase 3: Clasificación de Riesgos y NLP (OE3)

1. **Reglas de Negocio Post-Detección:**
   - Clasificación automática de las anomalías detectadas en tipologías:
     - **Valor Extremo:** Transacciones > $10.000 Millones (Posible Lavado).
     - **Valor Ínfimo:** Transacciones < $1 Millón (Posible Evasión/Error).
     - **Tráfico Inusual:** Predios con > 50 anotaciones (Posible Volteo de Tierras).
2. **Minería de Texto (NLP):**
   - Análisis de la columna `DOCUMENTO_JUSTIFICATIVO` en las anomalías.
   - Uso de **TF-IDF** para identificar palabras clave sospechosas ("Única", "Remate", fechas erróneas) que distinguen a las transacciones fraudulentas de las normales.
