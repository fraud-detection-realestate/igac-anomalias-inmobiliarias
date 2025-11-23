"""
Módulo para limpieza de datos del IGAC.
Incluye funciones para normalizar strings, limpiar valores numéricos,
manejar nulos y duplicados.
"""

import polars as pl
import pandas as pd
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
        "ORIP",
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
    print("Parseando columnas de fecha...")

    if isinstance(df, pl.DataFrame):
        for col in DATE_COLUMNS:
            if col in df.columns:
                # Si ya es datetime → no hacer nada
                if df[col].dtype == pl.Datetime:
                    continue

                # Si no es string → forzar a string para poder hacer str.strptime
                if df[col].dtype != pl.String:
                    df = df.with_columns(
                        [pl.col(col).cast(pl.String, strict=False).alias(col)]
                    )

                # Ahora sí parsear
                df = df.with_columns(
                    [pl.col(col).str.strptime(pl.Datetime, strict=False).alias(col)]
                )

    else:
        for col in DATE_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

    print("✓ Fechas parseadas")
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


def handle_missing_values(df, strategy="report"):
    print(f"Manejando valores nulos: estrategia {strategy}")

    if isinstance(df, pd.DataFrame):
        if strategy == "drop":
            df = df.dropna(subset=CRITICAL_COLUMNS)
        elif strategy == "fill":
            df = df.fillna(0)
        return df

    if strategy == "drop":
        for col in CRITICAL_COLUMNS:
            if col in df.columns:
                df = df.filter(pl.col(col).is_not_null())

    elif strategy == "fill":
        df = df.fill_null(0)

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
