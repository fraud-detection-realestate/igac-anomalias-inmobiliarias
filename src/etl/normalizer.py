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
    value_column: float = "VALOR",
    year_column: float = "YEAR_RADICA",
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
        # Evitar usar métodos no soportados por Expr (por ejemplo map_dict en algunas versiones).
        # Construimos un DataFrame pequeño con factores y hacemos un `join` seguro.
        factor_items = [
            (int(year), float(base_ipc / ipc))
            for year, ipc in IPC_DATA.items()
        ]

        adjustment_df = pl.DataFrame({
            "YEAR": [y for y, _ in factor_items],
            "FACTOR": [f for _, f in factor_items]
        })

        # Asegurar una clave de join en ambos lados y cast seguro
        join_key = "__YEAR_JOIN__"
        df = df.with_columns([
            pl.col(year_column).cast(pl.Int64, strict=False).alias(join_key)
        ])

        # Renombrar columna YEAR en adjustment_df para que coincida con la llave temporal
        adjustment_df = adjustment_df.with_columns([
            pl.col("YEAR").cast(pl.Int64)
        ]).rename({"YEAR": join_key})

        # Hacer left join para traer el factor correspondiente (si no existe, usar 1.0)
        df = df.join(adjustment_df, on=join_key, how="left")

        # Calcular VALOR_AJUSTADO y limpiar columnas temporales
        df = df.with_columns([
            (pl.col(value_column) * pl.col("FACTOR").fill_null(1.0)).alias("VALOR_AJUSTADO")
        ]).drop(["FACTOR", join_key])
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
    Crea características temporales para TODAS las columnas de fecha
    """
    print("Creando campos temporales...")

    if isinstance(df, pl.DataFrame):
        # Procesar TODAS las columnas de fecha que existan
        date_columns_to_process = [
            col for col in ["FECHA_RADICA_TEXTO", "FECHA_APERTURA_TEXTO"] 
            if col in df.columns
        ]
        
        for col in date_columns_to_process:
            print(f"  Procesando {col} (tipo: {df[col].dtype})...")
            
            # Determinar si necesita parseo o ya es fecha
            if df[col].dtype in [pl.Date, pl.Datetime]:
                date_expr = pl.col(col)
                print("    Ya es tipo Date/Datetime")
            else:
                # La columna es string, parsear desde formato dd/mm/yyyy
                print("    Parseando desde string...")
                date_expr = pl.coalesce([
                    # Tu formato principal: dd/mm/yyyy (20/12/2015)
                    pl.col(col).str.strptime(pl.Date, format="%d/%m/%Y", strict=False),
                    
                    # Formatos alternativos por si acaso
                    pl.col(col).str.strptime(pl.Date, format="%Y-%m-%d %H:%M:%S", strict=False),
                    pl.col(col).str.strptime(pl.Date, format="%Y-%m-%d", strict=False),
                    pl.col(col).str.strptime(pl.Date, format="%d-%m-%Y", strict=False),
                    pl.col(col).str.strptime(pl.Date, format="%Y/%m/%d", strict=False),
                    
                    # Año corto como fallback
                    pl.col(col).str.strptime(pl.Date, format="%d/%m/%y", strict=False),
                    pl.col(col).str.strptime(pl.Date, format="%d-%m-%y", strict=False),
                ])
            
            # Crear sufijos únicos por columna
            suffix = "_RADICA" if "RADICA" in col else "_APERTURA"
            
            # Crear columnas temporales
            df = df.with_columns([
                date_expr.dt.year().alias(f"ANIO{suffix}"),
                date_expr.dt.month().alias(f"MES{suffix}"),
                date_expr.dt.quarter().alias(f"TRIMESTRE{suffix}"),
                ((date_expr.dt.month() - 1) // 6 + 1).alias(f"SEMESTRE{suffix}"),
                date_expr.dt.weekday().alias(f"DIA_SEMANA{suffix}")
            ])
            
            # Verificar cuántas fechas no se parsearon
            failed = df[f"MES{suffix}"].null_count()
            total = len(df)
            if failed > 0:
                print(f"    ⚠ {failed:,} de {total:,} fechas no parseadas ({failed/total*100:.2f}%)")
            else:
                print(f"    ✓ Todas las fechas parseadas correctamente")

    else:  # Pandas
        if date_column in df.columns:
            # Parseo flexible con dayfirst=True para formato dd/mm/yyyy
            df[date_column + '_parsed'] = pd.to_datetime(
                df[date_column], 
                format='%d/%m/%Y',  # Especificar formato exacto
                errors='coerce'
            )
            df['ANIO_RADICA'] = df[date_column + '_parsed'].dt.year
            df['MES_RADICA'] = df[date_column + '_parsed'].dt.month
            df['TRIMESTRE_RADICA'] = df[date_column + '_parsed'].dt.quarter
            df['SEMESTRE_RADICA'] = (df[date_column + '_parsed'].dt.month - 1) // 6 + 1
            df['DIA_SEMANA_RADICA'] = df[date_column + '_parsed'].dt.weekday
            
            failed = df[date_column + '_parsed'].isna().sum()
            if failed > 0:
                print(f"  ⚠ {failed:,} fechas no parseadas")

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
    
    #df = normalize_divipola_codes(df)
    df = create_temporal_features(df)
    df = adjust_for_inflation(df)
    df = create_geographic_key(df)
    #df = calculate_derived_metrics(df)
    
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
