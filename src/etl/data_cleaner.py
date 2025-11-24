"""
Módulo para limpieza de datos del IGAC.
Incluye funciones para normalizar strings, limpiar valores numéricos,
manejar nulos y duplicados.
"""

import polars as pl
import pandas as pd
import numpy as np
import re
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from utils.config import (
    MUNICIPALITY_MAPPING,
    CRITICAL_COLUMNS,
    NUMERIC_COLUMNS,
    DATE_COLUMNS,
)

# ===============================================================
# 1. LIMPIEZA DE STRINGS
# ===============================================================


def clean_string_columns(df: pl.DataFrame | pd.DataFrame):
    """
    Limpia columnas de texto garantizando:
    - Se castea a String cuando sea necesario.
    - Se aplican operaciones .str.* SOLAMENTE a columnas string.

    Esto evita errores como: expected `String`, got `i64`.
    """

    print("🧹 LIMPIANDO COLUMNAS STRING...")

    # Columnas que normalmente deberían ser string
    expected_str_columns = [
        "MATRICULA",
        #"ORIP",
        "DIVIPOLA",
        "TIPO_PREDIO_ZONA",
        "CATEGORIA_RURALIDAD",
        "ESTADO_FOLIO",
        "NOMBRE_NATUJUR",
    ]

    if isinstance(df, pd.DataFrame):
        for col in expected_str_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.upper()
        return df

    # --- POLARS ---
    safe_cols = [c for c in expected_str_columns if c in df.columns]

    if not safe_cols:
        print("   ➤ No se detectaron columnas string para limpiar.")
        return df

    print(f"   ➤ Columnas detectadas: {safe_cols}")

    # 1. Convertir a string
    df = df.with_columns(
        [pl.col(col).cast(pl.String, strict=False).alias(col) for col in safe_cols]
    )

    # 2. Aplicar strip + uppercase
    df = df.with_columns(
        [
            pl.col(col).str.strip_chars().str.to_uppercase().alias(col)
            for col in safe_cols
        ]
    )

    print("✓ Columnas string limpiadas")
    return df


# ===============================================================
# 2. MUNICIPIOS
# ===============================================================


def clean_municipality_names(df: pl.DataFrame) -> pl.DataFrame:
    print("→ Normalizando nombres de municipios...")

    if "MUNICIPIO" not in df.columns:
        print("⚠ La columna MUNICIPIO no existe, saltando limpieza.")
        return df

    # Convertir asegura que sea string
    df = df.with_columns(
        pl.col("MUNICIPIO")
        .cast(pl.String)
        .str.to_uppercase()
        .str.replace_all(r"[ÁÀÄ]", "A")
        .str.replace_all(r"[ÉÈË]", "E")
        .str.replace_all(r"[ÍÌÏ]", "I")
        .str.replace_all(r"[ÓÒÖ]", "O")
        .str.replace_all(r"[ÚÙÜ]", "U")
        .alias("MUNICIPIO")
    )

    # Aplicar mapeo corregido
    for old, new in MUNICIPALITY_MAPPING.items():
        old_norm = (
            old.upper()
            .replace("Á", "A")
            .replace("É", "E")
            .replace("Í", "I")
            .replace("Ó", "O")
            .replace("Ú", "U")
        )

        df = df.with_columns(
            pl.when(pl.col("MUNICIPIO") == old_norm)
            .then(pl.lit(new.upper()))  # ← AQUÍ ESTABA EL ERROR
            .otherwise(pl.col("MUNICIPIO"))
            .alias("MUNICIPIO")
        )

    return df


# ===============================================================
# 3. DEPARTAMENTOS
# ===============================================================


def clean_department_names(df):
    print("Normalizando nombres de departamentos...")

    if isinstance(df, pd.DataFrame):
        df["DEPARTAMENTO"] = df["DEPARTAMENTO"].astype(str).str.upper().str.strip()
        return df

    df = df.with_columns(
        [
            pl.col("DEPARTAMENTO")
            .cast(pl.String)
            .str.to_uppercase()
            .str.strip_chars()
            .alias("DEPARTAMENTO")
        ]
    )

    print("✓ Departamentos normalizados")
    return df


# ===============================================================
# 4. FECHAS
# ===============================================================


def parse_dates(df: pl.DataFrame | pd.DataFrame) -> pl.DataFrame | pd.DataFrame:
   
    print("  📅 Parseando y estandarizando fechas...")

    if isinstance(df, pl.DataFrame):
        for col in DATE_COLUMNS:
            if col not in df.columns:
                continue
            
            print(f"     Procesando {col} (tipo: {df[col].dtype})...")
            
            # Contar nulos ANTES
            null_count_before = df[col].null_count()
            
            # Si ya es Date o Datetime, convertir directamente
            if df[col].dtype in [pl.Date, pl.Datetime]:
                df = df.with_columns(
                    pl.col(col).dt.strftime("%d/%m/%Y").alias(col)  # ← %Y (año 4 dígitos)
                )
                print(f"       ✓ Convertido de datetime a dd/mm/yyyy")
                continue
            
            # Si es numérico (solo año), mantener como está
            if df[col].dtype in [pl.Int64, pl.Int32, pl.Float64, pl.Float32]:
                print(f"       ⚠ Columna numérica, se mantiene")
                continue
            
            # Convertir a string si no lo es
            if df[col].dtype not in [pl.String, pl.Utf8]:
                df = df.with_columns(
                    pl.col(col).cast(pl.Utf8, strict=False).alias(col)
                )
            
            # Parsear TODOS los formatos posibles con coalesce
            df = df.with_columns(
                pl.coalesce([
                    # Formato: 2018-02-05 00:00:00 (con hora)
                    pl.col(col).str.strptime(pl.Date, format="%Y-%m-%d %H:%M:%S", strict=False),
                    
                    # Formato: 2018-02-05 (sin hora)
                    pl.col(col).str.strptime(pl.Date, format="%Y-%m-%d", strict=False),
                    
                    # Formato: 05/02/2018 (dd/mm/yyyy) - año completo
                    pl.col(col).str.strptime(pl.Date, format="%d/%m/%Y", strict=False),
                    
                    # Formato: 05/02/18 (dd/mm/yy) - año corto
                    pl.col(col).str.strptime(pl.Date, format="%d/%m/%y", strict=False),
                    
                    # Formato: 05-02-2018 (dd-mm-yyyy)
                    pl.col(col).str.strptime(pl.Date, format="%d-%m-%Y", strict=False),
                    
                    # Formato: 05-02-18 (dd-mm-yy)
                    pl.col(col).str.strptime(pl.Date, format="%d-%m-%y", strict=False),
                    
                    # Formato: 2018/02/05 (yyyy/mm/dd)
                    pl.col(col).str.strptime(pl.Date, format="%Y/%m/%d", strict=False),
                ])
                .dt.strftime("%d/%m/%Y")  # ← Cambio clave: %Y en lugar de %y
                .alias(col)
            )
            
            # Contar nulos DESPUÉS
            null_count_after = df[col].null_count()
            
            if null_count_after > null_count_before:
                print(f"       ⚠ ADVERTENCIA: {null_count_after - null_count_before:,} fechas no pudieron parsearse")
            else:
                print(f"       ✓ Parseado exitoso: {null_count_before:,} → {null_count_after:,} nulos")

    else:  # Pandas
        for col in DATE_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
                df[col] = df[col].dt.strftime("%d/%m/%Y")  # ← %Y (año 4 dígitos)

    print("  ✓ Fechas estandarizadas al formato dd/mm/yyyy")
    return df


# ===============================================================
# 5. NUMÉRICOS
# ===============================================================


def clean_numeric_values(df):
    print("Limpiando valores numéricos...")

    if isinstance(df, pd.DataFrame):
        for col in NUMERIC_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                if col == "VALOR":
                    df.loc[df[col] < 0, col] = None
        return df

    # --- Polars ---
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df = df.with_columns(
                [pl.col(col).cast(pl.Float64, strict=False).alias(col)]
            )

            if col == "VALOR":
                df = df.with_columns(
                    [
                        pl.when(pl.col(col) < 0)
                        .then(None)
                        .otherwise(pl.col(col))
                        .alias(col)
                    ]
                )

    print("✓ Valores numéricos limpios")
    return df


# ===============================================================
# 6. NULOS
# ===============================================================


def handle_missing_values(df, strategy="fill"):
    """Maneja valores nulos en DataFrame de Polars de forma simple y directa"""
    
    print(f"Manejando valores nulos: estrategia '{strategy}'")
    
    # Reemplazos específicos
    specific_fill = {
        "NUMERO_CATASTRAL": "SIN_NUMERO_CATASTRAL",
        "NUMERO_CATASTRAL_ANTIGUO": "SIN_NUMERO_CATASTRAL_ANTIGUO",
        "ESTADO_FOLIO": "INACTIVO",
        "FOLIOS_DERIVADOS": "SIN_FOLIO",
        "DOCUMENTO_JUSTIFICATIVO": "SIN_DOCUMENTO_JUSTIFICATIVO",
        #"ORIP": "SIN_ORIP"
        }
  
    
    # Crear expresiones para todas las columnas de una vez
    fill_exprs = []
    
    for col, fill_value in specific_fill.items():
        if col not in df.columns:
            continue
        
        # Contar nulos antes
        null_count = df[col].null_count()
        if null_count > 0:
            print(f"     {col}: {null_count:,} nulos detectados")
        
        # Reemplazar nulos Y strings vacíos/problemáticos
        expr = (
            pl.when(
                pl.col(col).is_null() |
                (pl.col(col).cast(pl.Utf8).str.strip_chars() == "") |
                (pl.col(col).cast(pl.Utf8).str.to_lowercase().is_in(["null", "none", "nan", "n/a"]))
            )
            .then(pl.lit(fill_value))
            .otherwise(pl.col(col))
            .alias(col)
        )
        fill_exprs.append(expr)
    
    # Aplicar todos los reemplazos de una vez
    if fill_exprs:
        df = df.with_columns(fill_exprs)
        print(f"     ✓ Procesadas {len(fill_exprs)} columnas")
    
    # Estrategia general para el resto
    if strategy == "fill":
        df = df.fill_null(0)
        print("     ✓ Nulos restantes llenados con 0")
    elif strategy == "drop":
        df = df.drop_nulls()
    
    return df

# ===============================================================
# 7. DUPLICADOS
# ===============================================================


def remove_duplicates(df, subset_columns=None):
    print("Eliminando duplicados...")

    if subset_columns is None:
        subset_columns = ["PK"]

    initial = len(df)

    if isinstance(df, pd.DataFrame):
        df = df.drop_duplicates(subset=subset_columns)
        return df

    df = df.unique(subset=subset_columns)

    final = len(df)
    print(f"✓ Eliminados {initial - final} duplicados")
    return df


# ===============================================================
# 8. FUNCIÓN PRINCIPAL
# ===============================================================


def apply_all_cleaning(df):
    """
    Ejecuta secuencia completa de limpieza.
    Orden correcto para evitar errores de tipos.
    """

    print("\n" + "=" * 60)
    print("🚀 INICIANDO PROCESO COMPLETO DE LIMPIEZA")
    print("=" * 60)

    df = clean_string_columns(df)
    df = clean_municipality_names(df)
    df = clean_department_names(df)
    df = parse_dates(df)
    df = clean_numeric_values(df)
    df = handle_missing_values(df)
    df = remove_duplicates(df)

    print("=" * 60)
    print("✅ LIMPIEZA COMPLETA FINALIZADA")
    print("=" * 60)

    return df


# ===============================================================
# 9. PRUEBAS LOCALES
# ===============================================================

if __name__ == "__main__":
    from data_loader import load_csv_sample

    print("Cargando muestra de datos...")
    sample = load_csv_sample(n_rows=1000)

    print("\nAplicando limpieza...")
    cleaned = apply_all_cleaning(sample)

    print("\nDatos limpios:")
    print(cleaned.head())
