"""
Módulo para normalización y estandarización de datos.
Incluye ajuste por inflación, creación de campos derivados, etc.
"""
import polars as pl
import pandas as pd
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))
from utils.config import IPC_DATA, BASE_YEAR


def adjust_for_inflation(
    df: pl.DataFrame | pd.DataFrame,
    value_column: str = "VALOR",
    year_column: str = "YEAR_RADICA",
    base_year: int = BASE_YEAR
) -> pl.DataFrame | pd.DataFrame:
    """
    Ajusta valores monetarios por inflación usando IPC.
    
    Args:
        df: DataFrame con valores a ajustar
        value_column: Nombre de la columna con valores monetarios
        year_column: Nombre de la columna con el año
        base_year: Año base para el ajuste
        
    Returns:
        DataFrame con columna adicional VALOR_AJUSTADO
    """
    print(f"Ajustando valores por inflación (año base: {base_year})...")
    
    # Crear factor de ajuste para cada año
    base_ipc = IPC_DATA[base_year]
    
    if isinstance(df, pl.DataFrame):
        # Crear mapeo de año a factor de ajuste
        adjustment_map = {
            year: base_ipc / ipc 
            for year, ipc in IPC_DATA.items()
        }
        
        # Aplicar ajuste
        df = df.with_columns([
            (pl.col(value_column) * 
             pl.col(year_column).map_dict(adjustment_map, default=1.0))
            .alias("VALOR_AJUSTADO")
        ])
    else:
        # Pandas
        df['FACTOR_AJUSTE'] = df[year_column].map(
            lambda year: base_ipc / IPC_DATA.get(year, base_ipc)
        )
        df['VALOR_AJUSTADO'] = df[value_column] * df['FACTOR_AJUSTE']
        df = df.drop('FACTOR_AJUSTE', axis=1)
    
    print("✓ Valores ajustados por inflación")
    return df


def create_temporal_features(
    df: pl.DataFrame | pd.DataFrame,
    date_column: str = "FECHA_RADICA_TEXTO"
) -> pl.DataFrame | pd.DataFrame:
    """
    Crea campos temporales derivados (mes, trimestre, semestre).
    
    Args:
        df: DataFrame con columna de fecha
        date_column: Nombre de la columna de fecha
        
    Returns:
        DataFrame con campos temporales adicionales
    """
    print("Creando campos temporales derivados...")
    
    if isinstance(df, pl.DataFrame):
        df = df.with_columns([
            pl.col(date_column).dt.month().alias("MES_RADICA"),
            pl.col(date_column).dt.quarter().alias("TRIMESTRE_RADICA"),
            ((pl.col(date_column).dt.month() - 1) // 6 + 1).alias("SEMESTRE_RADICA"),
            pl.col(date_column).dt.weekday().alias("DIA_SEMANA_RADICA")
        ])
    else:
        df['MES_RADICA'] = df[date_column].dt.month
        df['TRIMESTRE_RADICA'] = df[date_column].dt.quarter
        df['SEMESTRE_RADICA'] = (df[date_column].dt.month - 1) // 6 + 1
        df['DIA_SEMANA_RADICA'] = df[date_column].dt.weekday
    
    print("✓ Campos temporales creados")
    return df


def normalize_divipola_codes(df: pl.DataFrame | pd.DataFrame) -> pl.DataFrame | pd.DataFrame:
    """
    Normaliza códigos DIVIPOLA (formato estándar de 5 dígitos).
    
    Args:
        df: DataFrame con columna DIVIPOLA
        
    Returns:
        DataFrame con códigos normalizados
    """
    print("Normalizando códigos DIVIPOLA...")
    
    if isinstance(df, pl.DataFrame):
        df = df.with_columns([
            pl.col("DIVIPOLA").str.strip_chars().str.zfill(5).alias("DIVIPOLA")
        ])
    else:
        df['DIVIPOLA'] = df['DIVIPOLA'].astype(str).str.strip().str.zfill(5)
    
    print("✓ Códigos DIVIPOLA normalizados")
    return df


def create_geographic_key(df: pl.DataFrame | pd.DataFrame) -> pl.DataFrame | pd.DataFrame:
    """
    Crea una clave geográfica única combinando departamento y municipio.
    
    Args:
        df: DataFrame
        
    Returns:
        DataFrame con columna GEO_KEY
    """
    print("Creando clave geográfica...")
    
    if isinstance(df, pl.DataFrame):
        df = df.with_columns([
            (pl.col("DEPARTAMENTO") + "_" + pl.col("MUNICIPIO")).alias("GEO_KEY")
        ])
    else:
        df['GEO_KEY'] = df['DEPARTAMENTO'] + "_" + df['MUNICIPIO']
    
    print("✓ Clave geográfica creada")
    return df


def calculate_derived_metrics(df: pl.DataFrame | pd.DataFrame) -> pl.DataFrame | pd.DataFrame:
    """
    Calcula métricas derivadas adicionales.
    Por ejemplo: valor por m², indicadores binarios, etc.
    
    Args:
        df: DataFrame
        
    Returns:
        DataFrame con métricas adicionales
    """
    print("Calculando métricas derivadas...")
    
    # Nota: Estas métricas requieren campos adicionales que pueden no estar en el dataset
    # Se incluyen como ejemplo y deben ajustarse según los datos reales
    
    if isinstance(df, pl.DataFrame):
        # Crear indicador de transacción de alto valor (ejemplo)
        df = df.with_columns([
            (pl.col("VALOR_AJUSTADO") > 500000000).cast(pl.Int8).alias("ALTO_VALOR")
        ])
    else:
        df['ALTO_VALOR'] = (df['VALOR_AJUSTADO'] > 500000000).astype(int)
    
    print("✓ Métricas derivadas calculadas")
    return df


def apply_all_standardization(df: pl.DataFrame | pd.DataFrame) -> pl.DataFrame | pd.DataFrame:
    """
    Aplica todas las funciones de estandarización en secuencia.
    
    Args:
        df: DataFrame limpio
        
    Returns:
        DataFrame estandarizado
    """
    print("\n" + "="*50)
    print("INICIANDO ESTANDARIZACIÓN DE DATOS")
    print("="*50 + "\n")
    
    df = normalize_divipola_codes(df)
    df = create_temporal_features(df)
    df = adjust_for_inflation(df)
    df = create_geographic_key(df)
    df = calculate_derived_metrics(df)
    
    print("\n" + "="*50)
    print("ESTANDARIZACIÓN FINALIZADA")
    print("="*50 + "\n")
    
    return df


if __name__ == "__main__":
    # Ejemplo de uso
    from data_loader import load_csv_sample
    from data_cleaner import apply_all_cleaning
    
    print("Cargando y limpiando muestra...")
    sample = load_csv_sample(n_rows=1000)
    cleaned = apply_all_cleaning(sample)
    
    print("\nAplicando estandarización...")
    standardized = apply_all_standardization(cleaned)
    
    print("\nDatos estandarizados:")
    print(standardized.head())
    print("\nColumnas finales:")
    print(standardized.columns)
