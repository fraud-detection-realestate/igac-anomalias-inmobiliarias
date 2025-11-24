# Guía de Configuración - Fraud Detection Real Estate

## 📋 Requisitos Previos

- Python 3.9 o superior
- Entorno virtual (venv) ya configurado
- Dataset IGAC_2015_-_2023.csv en `data/raw/`

## 🚀 Instalación

### 1. Activar el Entorno Virtual

```powershell
# En Windows PowerShell
.\venv\Scripts\Activate.ps1

# O en CMD
.\venv\Scripts\activate.bat
```

### 2. Instalar Dependencias

```powershell
pip install -r requirements.txt
```

Esto instalará:

- **Polars** - Procesamiento eficiente de datos
- **Pandas** - Análisis complementario
- **PyArrow** - Formato Parquet
- **Jupyter** - Notebooks interactivos
- **Matplotlib/Seaborn** - Visualización
- **Great Expectations** - Validación de calidad

### 3. Verificar Instalación

```powershell
python -c "import polars; print(f'Polars {polars.__version__} instalado correctamente')"
```

## 📊 Estructura del Proyecto

```
fraud-detection-realestate/
├── data/
│   ├── raw/                    # IGAC_2015_-_2023.csv
│   ├── processed/              # Datos procesados (Parquet)
│   ├── interim/                # Datos intermedios
│   └── external/               # Datos externos (IPC, etc.)
├── notebooks/
│   ├── 01_exploracion_inicial.ipynb
│   ├── 02_limpieza_datos.ipynb
│   ├── 03_estandarizacion.ipynb
│   └── 04_validacion_calidad.ipynb
├── src/
│   ├── etl/
│   │   ├── data_loader.py      # Carga eficiente de datos
│   │   ├── data_cleaner.py     # Limpieza y normalización
│   │   ├── normalizer.py       # Estandarización y ajuste IPC
│   │   └── validators.py       # Validación de calidad
│   └── utils/
│       └── config.py            # Configuración y constantes
└── requirements.txt
```

## 🔧 Uso de los Notebooks

### Notebook 1: Exploración Inicial

**Objetivo**: Entender la estructura del dataset

```powershell
jupyter notebook notebooks/01_exploracion_inicial.ipynb
```

**Contenido**:

- Información general del dataset
- Análisis de tipos de datos
- Valores nulos
- Distribución temporal y geográfica
- Análisis de valores monetarios

### Notebook 2: Limpieza de Datos

**Objetivo**: Limpiar y preparar los datos

```powershell
jupyter notebook notebooks/02_limpieza_datos.ipynb
```

**Contenido**:

- Carga del dataset completo (lazy loading)
- Normalización de municipios y departamentos
- Parseo de fechas
- Limpieza de valores numéricos
- Eliminación de duplicados
- Guardado en formato Parquet

**⚠️ Nota**: Este notebook procesa ~9.5 GB de datos y puede tomar varios minutos.

### Notebook 3: Estandarización

**Objetivo**: Estandarizar valores para comparabilidad

```powershell
jupyter notebook notebooks/03_estandarizacion.ipynb
```

**Contenido**:

- Ajuste de valores monetarios por IPC (2015-2024)
- Creación de campos temporales derivados
- Normalización de códigos DIVIPOLA
- Generación de clave geográfica
- Cálculo de métricas adicionales

### Notebook 4: Validación de Calidad

**Objetivo**: Validar la calidad del dataset procesado

```powershell
jupyter notebook notebooks/04_validacion_calidad.ipynb
```

**Contenido**:

- Reporte de calidad de datos
- Validación de rangos
- Detección de outliers
- Análisis de completitud
- Métricas finales del dataset

## 🔍 Módulos ETL

### data_loader.py

Funciones para carga eficiente de datos:

```python
from etl.data_loader import load_csv_sample, load_full_dataset_lazy, save_to_parquet

# Cargar muestra
sample = load_csv_sample(n_rows=10000)

# Cargar dataset completo (lazy)
lf = load_full_dataset_lazy()

# Guardar en Parquet
save_to_parquet(df, output_path)
```

### data_cleaner.py

Funciones de limpieza:

```python
from etl.data_cleaner import apply_all_cleaning

# Aplicar todas las limpiezas
df_clean = apply_all_cleaning(df)
```

### normalizer.py

Funciones de estandarización:

```python
from etl.normalizer import apply_all_standardization

# Aplicar estandarización
df_std = apply_all_standardization(df_clean)
```

### validators.py

Funciones de validación:

```python
from etl.validators import generate_quality_report, detect_outliers

# Generar reporte
report = generate_quality_report(df)

# Detectar outliers
df_outliers = detect_outliers(df, 'VALOR_AJUSTADO')
```

## 📝 Configuración

El archivo `src/utils/config.py` contiene:

- **Rutas de archivos**: Ubicaciones de datos raw, procesados, etc.
- **Parámetros de procesamiento**: Tamaño de chunks, muestras
- **Datos del IPC**: Índice de Precios al Consumidor 2015-2025
- **Mapeos**: Normalización de municipios
- **Rangos válidos**: Para validación de datos

### Actualizar datos del IPC

Si necesitas actualizar los valores del IPC, edita `src/utils/config.py`:

```python
IPC_DATA = {
    2015: 100.0,
    2016: 107.5,
    # ... actualizar con datos reales del DANE
    2024: 160.0,
    2025: 168.0,
}
```

## 🐛 Solución de Problemas

### Error: "Module not found"

```powershell
# Asegúrate de estar en el entorno virtual
.\venv\Scripts\Activate.ps1

# Reinstala las dependencias
pip install -r requirements.txt
```

### Error: "Memory Error" al procesar dataset completo

El dataset es muy grande (~9.5 GB). Soluciones:

1. **Usar lazy loading**: Los notebooks ya lo implementan
2. **Procesar en chunks**: Ajustar `CHUNK_SIZE` en `config.py`
3. **Aumentar memoria virtual**: Configurar swap en Windows

### Error: "File not found" para IGAC CSV

Verifica que el archivo esté en la ubicación correcta:

```powershell
# Debe estar en:
data/raw/IGAC_2015_-_2023.csv
```

## 📈 Próximos Pasos

Después de completar la fase de ETL:

1. **Fase 3**: Definición de la "Normalidad"
   - Análisis estadístico por municipio
   - Establecer líneas base

2. **Fase 4**: Detección de Anomalías
   - Implementar modelos de detección
   - Reglas de negocio

3. **Fase 5**: Visualización y Monitoreo
   - Dashboard interactivo
   - Sistema de alertas

## 📞 Soporte

Para problemas o preguntas:

- Revisar logs de Jupyter
- Verificar versiones de dependencias: `pip list`
- Consultar documentación de Polars: <https://pola-rs.github.io/polars/>

---

**Última actualización**: 2025-11-22
