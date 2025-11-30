# Instituto de Monitoreo y Detección de Anomalías en Dinámica Inmobiliaria en Colombia

[![Python](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/)

## 📋 Tabla de Contenidos

- [Descripción del Proyecto](#-descripción-del-proyecto)
- [Objetivos](#-objetivos)
- [Stack Tecnológico](#-stack-tecnológico)
- [Estructura del Proyecto](#-estructura-del-proyecto)
- [Flujo de Datos (ETL)](#-flujo-de-datos-etl)
- [Estructura del Dataset](#-estructura-del-dataset)
- [Estado por Fases](#-estado-por-fases)
- [Instalación](#-instalación)
- [Uso](#-uso)
- [Notas Importantes](#-notas-importantes)
- [Autores](#autores)

---

## 📋 Descripción del Proyecto

Sistema de análisis y detección de anomalías en transacciones inmobiliarias en Colombia, utilizando datos históricos del IGAC (Instituto Geográfico Agustín Codazzi) del período 2015-2025. El proyecto tiene como objetivo identificar patrones irregulares en el mercado inmobiliario que puedan indicar fraude, lavado de activos o manipulación de precios.

## 🎯 Objetivos

- **Monitoreo continuo**: Análisis de ~34 millones de registros de transacciones inmobiliarias
- **Detección de anomalías**: Identificación de patrones irregulares en precios, volúmenes y comportamientos
- **Normalización de datos**: Estandarización de información para análisis comparativo entre regiones y períodos
- **Análisis temporal**: Seguimiento de la evolución del mercado inmobiliario colombiano

Este repositorio implementa de forma incremental las **fases definidas en `docs/task.md`**, apoyándose en la descripción general de objetivos en `docs/documentation.md`.

## 🛠️ Stack Tecnológico

### Procesamiento de Datos

- **Python 3.8+**: Lenguaje principal para análisis y procesamiento
- **Polars**: Manejo eficiente de grandes volúmenes de datos (34M+ filas)
- **Pandas**: Para operaciones que requieran compatibilidad con bibliotecas específicas
- **SQLAlchemy**: Para consultas y operaciones con bases de datos

### Análisis y Visualización

- **Matplotlib/Seaborn**: Visualización de datos
- **Plotly**: Visualizaciones interactivas
- **Scikit-learn**: Modelos de machine learning y detección de anomalías
- **NLTK/Spacy**: Procesamiento de lenguaje natural para análisis de texto

### Herramientas de Desarrollo

- **Jupyter Notebooks**: Para análisis exploratorios y documentación ejecutable
- **Poetry**: Gestión de dependencias
- **Git**: Control de versiones

## 🔄 Flujo de Datos (ETL)

Resumen del recorrido de los datos desde la fuente cruda hasta el dataset estandarizado listo para análisis:

1. **Datos crudos (`data/raw/`)**
   - Descarga del histórico de transacciones inmobiliarias 2015–2025 (~34M registros) desde la fuente del IGAC.
   - Formato original (CSV u otro) sin limpieza.

2. **Dataset limpio (`data/processed/igac_cleaned.parquet`)**
   - Generado en el notebook `02_limpieza_datos.ipynb` usando `etl.data_cleaner.apply_all_cleaning`.
   - Operaciones clave:
     - Limpieza de columnas string y normalización de municipios/departamentos.
     - Estandarización de fechas a formato `dd/mm/YYYY`.
     - Limpieza de valores numéricos (incluyendo `VALOR`).
     - Manejo de nulos e imputaciones específicas (folios, números catastrales).
     - Eliminación de duplicados por `PK`.
   - Formato **Parquet** optimizado para lectura con Polars (aprox. decenas de GB, según compresión).

3. **Dataset estandarizado (`data/processed/igac_standardized.parquet`)**
   - Generado en el notebook `03_estandarizacion.ipynb` usando `etl.normalizer.apply_all_standardization`.
   - Operaciones clave:
     - Cálculo de `VALOR_AJUSTADO` por inflación usando `IPC_DATA` y `BASE_YEAR`.
     - Creación de campos temporales derivados (`MES_RADICA`, `TRIMESTRE_RADICA`, etc.).
     - Generación de clave geográfica `GEO_KEY = DEPARTAMENTO_MUNICIPIO`.
   - Este dataset es la base para:
     - Reglas de negocio y Z-Score (Fase 2).
     - Modelos de ML y análisis geoespacial (fases posteriores).

## 📊 Estructura del Dataset

### Campos Principales

| Campo | Descripción |
|-------|-------------|
| `PK` | Identificador único |
| `MATRICULA` | Número de matrícula inmobiliaria |
| `FECHA_RADICA_TEXTO` | Fecha de radicación |
| `FECHA_APERTURA_TEXTO` | Fecha de apertura |
| `YEAR_RADICA` | Año de radicación |
| `ORIP` | Oficina de Registro de Instrumentos Públicos |
| `DIVIPOLA` | Código de división político-administrativa |
| `DEPARTAMENTO` | Departamento |
| `MUNICIPIO` | Municipio |
| `TIPO_PREDIO_ZONA` | Tipo de predio y zona |
| `CATEGORIA_RURALIDAD` | Categoría rural/urbana |
| `NUM_ANOTACION` | Número de anotación |
| `ESTADO_FOLIO` | Estado del folio |
| `FOLIOS_DERIVADOS` | Folios derivados |
| `Dinámica_Inmobiliaria` | Tipo de dinámica |
| `COD_NATUJUR` | Código naturaleza jurídica |
| `NOMBRE_NATUJUR` | Nombre naturaleza jurídica |
| `NUMERO_CATASTRAL` | Número catastral actual |
| `NUMERO_CATASTRAL_ANTIGUO` | Número catastral anterior |
| `DOCUMENTO_JUSTIFICATIVO` | Documento justificativo |
| `COUNT_A` | Contador A |
| `COUNT_DE` | Contador DE |
| `PREDIOS_NUEVOS` | Indicador de predios nuevos |
| `TIENE_VALOR` | Indicador de valor presente |
| `TIENE_MAS_DE_UN_VALOR` | Indicador de múltiples valores |
| `VALOR` | Valor de la transacción (campo numérico principal para análisis) |

---

## 🚦 Estado por Fases (según `docs/task.md`)

### ✅ Fase 1: Cimientos de Datos y "Zona Cero"

**Estado**: ✅ Implementada en los notebooks `02_limpieza_datos.ipynb` y `03_estandarizacion.ipynb` (parte de OE1).

#### Actividades principales

- [x] **Ingesta y carga eficiente**:
  - Uso de `Polars` en modo *lazy* para cargar ~30M de registros (`load_full_dataset_lazy`).
  - Exploración de esquema y conteo de nulos por columna sin cargar todo en memoria.
- [x] **Limpieza inicial de datos** (notebook `02_limpieza_datos.ipynb` + `etl/data_cleaner.apply_all_cleaning`):
  - Eliminación de comillas dobles en columnas de texto.
  - Manejo de columnas con alta proporción de nulos.
  - Depuración de columnas no relevantes como `FECHA_APERTURA_TEXTO`.
  - Generación y guardado de un dataset limpio en Parquet (`data/processed/igac_cleaned.parquet`).
- [x] **Estandarización básica de valores monetarios** (parte de OE1, notebook `03_estandarizacion.ipynb`):
  - Cálculo de `VALOR_AJUSTADO` usando IPC anual (`IPC_DATA`) con año base definido en `BASE_YEAR`.
- [x] **Campos temporales y claves derivadas**:
  - Derivación de campos como `YEAR_RADICA`, `MES_RADICA`, `TRIMESTRE_RADICA`, `SEMESTRE_RADICA`, `DIA_SEMANA_RADICA`.
  - Creación de clave geográfica `GEO_KEY = DEPARTAMENTO_MUNICIPIO`.

#### Pendientes dentro de Fase 1

- [ ] Normalización detallada de nombres de municipios (ej. "Bogotá D.C." vs "Bogotá").
- [ ] Revisión/normalización adicional de `DIVIPOLA` donde aplique.
- [ ] Limpieza específica de campos de área (si se integran en versiones futuras del dataset).

### 🔄 Fase 2: Detección Basada en Reglas y Estadística

**Estado**: 🧩 En diseño / pendiente de implementación en código.

Según `docs/task.md`, esta fase debe incluir:

- [ ] **Reglas de negocio (Hard Rules)**:
  - Regla de Rango: valor < 10% del avalúo catastral.
  - Regla de Tiempo: más de 2 ventas en menos de 6 meses.
  - Regla de Integridad: valor > 0 con áreas = 0.
- [ ] **Detección estadística (Z-Score)**:
  - Cálculo de Z-Score del precio por m² por municipio.
  - Marcación de transacciones con |Z| > 3 como posibles anomalías.

La estructura de datos generada en Fase 1 (valores ajustados, campos temporales y `GEO_KEY`) ya deja preparada la base para implementar estas reglas en los siguientes notebooks/scripts.

### 🔮 Fases Futuras

- **Fase 4**: Modelado de detección de anomalías
- **Fase 5**: Implementación de alertas y monitoreo
- **Fase 6**: Dashboard y visualización
- **Fase 7**: Reportes y documentación

## 📁 Estructura del Proyecto

```
igac-anomalias-inmobiliarias/
├── data/                      # Datos (no versionados en Git)
│   ├── raw/                   # Datos crudos originales
│   ├── processed/             # Datos procesados y estandarizados
│   └── models/                # Modelos entrenados
│
├── notebooks/                 # Jupyter notebooks de análisis
│   ├── 01_exploracion_inicial.ipynb    # Análisis exploratorio inicial
│   ├── 02_limpieza_datos.ipynb         # Proceso de limpieza de datos
│   ├── 03_estandarizacion.ipynb        # Estandarización de datos
│   ├── 04_entrenamiento_model.ipynb    # Entrenamiento de modelos
│   ├── 05_deteccion_anomalias.ipynb    # Detección de anomalías
│   └── 06_analisis_texto_nlp.ipynb     # Análisis de texto con NLP
│
├── src/                       # Código fuente Python
│   ├── etl/                   # Pipeline de ETL
│   │   ├── __init__.py
│   │   ├── data_cleaner.py    # Limpieza de datos
│   │   ├── data_loader.py     # Carga de datos
│   │   ├── normalizer.py      # Normalización de datos
│   │   └── validators.py      # Validación de datos
│   │
│   └── utils/                 # Utilidades y helpers
│
├── docs/                      # Documentación
│   ├── task.md                # Fases y procesos del reto
│   └── documentation.md       # Documentación detallada
│
├── README.md                  # Este archivo
├── requirements.txt           # Dependencias de Python
└── pyproject.toml            # Configuración de Poetry (si se usa)
```

## 🔧 Instalación

### Requisitos Previos

- Python 3.8 o superior
- Git
- (Opcional) Poetry para gestión de dependencias

### Configuración del Entorno

1. **Clonar el repositorio**

   ```bash
   git clone <repository-url>
   cd igac-anomalias-inmobiliarias
   ```

2. **Configurar entorno virtual (recomendado)**

   ```bash
   # Windows
   python -m venv venv
   .\venv\Scripts\activate
   
   # Linux/MacOS
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Instalar dependencias**

   ```bash
   pip install -r requirements.txt
   
   # O usando Poetry (si está configurado)
   poetry install
   ```

4. **Configurar variables de entorno**
   Crear un archivo `.env` en la raíz del proyecto con las siguientes variables:

   ```
   # Rutas de datos
   RAW_DATA_PATH=./data/raw/
   PROCESSED_DATA_PATH=./data/processed/
   MODELS_PATH=./data/models/
   
   # Configuración de procesamiento
   NUM_WORKERS=4  # Número de workers para procesamiento paralelo
   CHUNK_SIZE=100000  # Tamaño de chunks para procesamiento
   ```

## 🚀 Uso

### Cargar y procesar datos

1. **Cargar datos crudos**

   ```python
   from src.etl.data_loader import load_raw_data
   
   # Cargar datos en modo lazy (recomendado para conjuntos grandes)
   df = load_raw_data(lazy=True)
   ```

2. **Ejecutar limpieza de datos**

   ```python
   from src.etl.data_cleaner import clean_data
   
   # Limpiar datos
   df_cleaned = clean_data(df)
   ```

3. **Ejecutar notebooks de análisis**
   Los notebooks están numerados en orden secuencial:
   - `01_exploracion_inicial.ipynb`: Análisis exploratorio inicial
   - `02_limpieza_datos.ipynb`: Proceso de limpieza de datos
   - `03_estandarizacion.ipynb`: Estandarización de datos
   - `04_entrenamiento_model.ipynb`: Entrenamiento de modelos
   - `05_deteccion_anomalias.ipynb`: Detección de anomalías
   - `06_analisis_texto_nlp.ipynb`: Análisis de texto con NLP

## 📝 Notas Importantes

### Rendimiento

- **Volumen de datos**: El conjunto completo contiene ~34 millones de registros
- **Uso de memoria**: Se recomienda al menos 16GB de RAM para procesamiento
- **Procesamiento en chunks**: Las funciones principales soportan procesamiento por lotes

### Calidad de Datos

- **Nombres de municipios**: Se han normalizado variaciones (ej: "Bogotá D.C." → "Bogotá")
- **Valores faltantes**: Se han manejado siguiendo estrategias documentadas
- **Validaciones**: Se incluyen validaciones de integridad en `src/etl/validators.py`

### Consideraciones Técnicas

- **Reproducibilidad**: Se recomienda usar entornos virtuales
- **Versionado**: Se sigue Semantic Versioning (SemVer) para el código
- **Documentación**: La documentación se actualiza con cada cambio significativo

## Autores

- Juan Carlos Charfuelan
- Keyla Daniela Cartagena
- Dylber Denylson Cabrera

---

**Última actualización**: 2025-11-25
