from pathlib import Path

# Rutas del proyecto
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
INTERIM_DATA_DIR = DATA_DIR / "interim"
EXTERNAL_DATA_DIR = DATA_DIR / "external"

# Archivo principal
IGAC_CSV_FILE = RAW_DATA_DIR / "IGAC_2015_-_2023.csv"
CLEANED_PARQUET_FILE = PROCESSED_DATA_DIR / "igac_cleaned.parquet"
STANDARDIZED_PARQUET_FILE = PROCESSED_DATA_DIR / "igac_standardized.parquet"

ML_PARQUET_FILE = PROCESSED_DATA_DIR / "igac_ml_base.parquet"

# Parámetros de procesamiento
CHUNK_SIZE = 100000  # Número de filas por chunk
SAMPLE_SIZE = 10000  # Tamaño de muestra para exploración rápida

# Columnas del dataset
COLUMNS = [
    "PK",
    "MATRICULA",
    "FECHA_RADICA_TEXTO",
    "FECHA_APERTURA_TEXTO",
    "YEAR_RADICA",
    "ORIP",
    "DIVIPOLA",
    "DEPARTAMENTO",
    "MUNICIPIO",
    "TIPO_PREDIO_ZONA",
    "CATEGORIA_RURALIDAD",
    "NUM_ANOTACION",
    "ESTADO_FOLIO",
    "FOLIOS_DERIVADOS",
    "Dinámica_Inmobiliaria",
    "COD_NATUJUR",
    "NOMBRE_NATUJUR",
    "NUMERO_CATASTRAL",
    "NUMERO_CATASTRAL_ANTIGUO",
    "DOCUMENTO_JUSTIFICATIVO",
    "COUNT_A",
    "COUNT_DE",
    "PREDIOS_NUEVOS",
    "TIENE_VALOR",
    "TIENE_MAS_DE_UN_VALOR",
    "VALOR",
]

# Columnas críticas que no deben tener nulos
CRITICAL_COLUMNS = ["PK", "MUNICIPIO", "DEPARTAMENTO", "YEAR_RADICA", "VALOR"]

# Columnas numéricas
NUMERIC_COLUMNS = [
    "YEAR_RADICA",
    "COUNT_A",
    "COUNT_DE",
    "PREDIOS_NUEVOS",
    "TIENE_VALOR",
    "TIENE_MAS_DE_UN_VALOR",
    "VALOR",
]

# Columnas de fecha
# DATE_COLUMNS = ["FECHA_RADICA_TEXTO", "FECHA_APERTURA_TEXTO"]
DATE_COLUMNS = ["FECHA_RADICA_TEXTO"]


# Mapeo de nombres de municipios (casos comunes de inconsistencia)
MUNICIPALITY_MAPPING = {
    "BOGOTA D.C.": "BOGOTÁ",
    "BOGOTA": "BOGOTÁ",
    "BOGOTA DC": "BOGOTÁ",
    "MEDELLIN": "MEDELLÍN",
    "CALI": "CALI",
    "BARRANQUILLA": "BARRANQUILLA",
    "CARTAGENA": "CARTAGENA DE INDIAS",
    # Agregar más según se identifiquen en la exploración
}

# Año base para ajuste inflacionario
BASE_YEAR = 2024

# IPC anual (Índice de Precios al Consumidor) - Datos del DANE
# Estos son valores de ejemplo, deben actualizarse con datos reales
IPC_DATA = {
    2015: 100.0,
    2016: 107.5,
    2017: 111.8,
    2018: 115.3,
    2019: 119.4,
    2020: 121.5,
    2021: 126.9,
    2022: 139.4,
    2023: 151.2,
    2024: 160.0,
    2025: 168.0,  # Estimado
}

# Rangos válidos para validación
VALID_RANGES = {
    "YEAR_RADICA": (2015, 2025),
    "VALOR": (0, 1e12),  # Hasta 1 billón de pesos
}

# Configuración de logging
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_LEVEL = "INFO"
