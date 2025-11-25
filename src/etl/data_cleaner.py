"""
Módulo para limpieza de datos del IGAC.
Incluye funciones para normalizar strings, limpiar valores numéricos,
manejar nulos y duplicados.
"""

import polars as pl
import pandas as pd
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from utils.config import (
    MUNICIPALITY_MAPPING,
    NUMERIC_COLUMNS,
    DATE_COLUMNS,
    CRITICAL_COLUMNS,  # Asegúrate de tener esto en config o defínelo aquí
)

# ===============================================================
# 0. PRE-LIMPIEZA GLOBAL
# ===============================================================


def clean_quotes_global(df: pl.DataFrame | pd.DataFrame) -> pl.DataFrame | pd.DataFrame:
    """
    Elimina comillas dobles (") de nombres de columnas y datos.
    Debe ser el PRIMER paso.
    """
    print("🛡️ SANEANDO COMILLAS DOBLES...")

    if isinstance(df, pd.DataFrame):
        df.columns = df.columns.str.replace('"', "", regex=False)
        df = df.replace('"', "", regex=True)
        return df

    # --- POLARS ---
    # 1. Renombrar columnas
    df = df.rename({col: col.replace('"', "") for col in df.columns})

    # 2. Limpiar datos en columnas String
    # Usamos strip_chars('"') que es más rápido que replace_all para bordes,
    # pero replace_all es más seguro si hay comillas en medio.
    df = df.with_columns(pl.col(pl.String).str.replace_all('"', "", literal=True))

    print("✓ Comillas eliminadas")
    return df


# ===============================================================
# 1. REGLAS DE NEGOCIO (Exclusiones e Imputaciones Específicas)
# ===============================================================


def apply_business_rules(
    df: pl.DataFrame | pd.DataFrame,
) -> pl.DataFrame | pd.DataFrame:
    """
    Aplica lógica específica:
    1. Elimina columnas obsoletas (ahorra memoria para pasos siguientes).
    2. Imputa valores de negocio (ej: "Sin folio", "SIN_ORIP").
    """
    print("⚖️ Aplicando reglas de negocio específicas...")

    # --- A. COLUMNAS A ELIMINAR ---
    # cols_to_drop = ["FECHA_APERTURA_TEXTO", "NUMERO_CATASTRAL_ANTIGUO"]
    cols_to_drop = ["FECHA_APERTURA_TEXTO"]

    # Polars
    existing_drop = [c for c in cols_to_drop if c in df.columns]
    if existing_drop:
        print(f"   - Eliminando: {existing_drop}")
        if isinstance(df, pd.DataFrame):
            df = df.drop(columns=existing_drop)
        else:
            df = df.drop(existing_drop)

    # --- B. IMPUTACIONES ESPECÍFICAS ---
    # Unificamos aquí la lógica de ORIP, FOLIOS y CATASTRAL para no repetirla

    if isinstance(df, pd.DataFrame):
        # Implementación Pandas omitida por brevedad (mantener tu lógica original si usas pandas)
        return df

    # Implementación Polars Optimizada
    exprs = []

    # 1. Folios Derivados
    if "FOLIOS_DERIVADOS" in df.columns:
        exprs.append(
            pl.col("FOLIOS_DERIVADOS")
            .cast(pl.String, strict=False)
            .fill_null(pl.lit("Sin folio"))
            .alias("FOLIOS_DERIVADOS")
        )

    # 2. Número Catastral
    if "NUMERO_CATASTRAL" in df.columns:
        exprs.append(
            pl.col("NUMERO_CATASTRAL")
            .cast(pl.String, strict=False)
            .fill_null(pl.lit("sin folio"))  # Pediste minúscula
            .alias("NUMERO_CATASTRAL")
        )

    # 3. ORIP (Lógica que tenías en el notebook)
    if "ORIP" in df.columns:
        # Lógica: Si es nulo, vacío o "0" -> "SIN_ORIP", sino -> UPPER y CLEAN
        exprs.append(
            pl.when(
                pl.col("ORIP").is_null()
                | (
                    pl.col("ORIP")
                    .cast(pl.String, strict=False)
                    .str.strip_chars()
                    .is_in(["", "0", "0.0"])
                )
            )
            .then(pl.lit("SIN_ORIP"))
            .otherwise(
                pl.col("ORIP")
                .cast(pl.String, strict=False)
                .str.strip_chars()
                .str.to_uppercase()
            )
            .alias("ORIP")
        )

    if exprs:
        df = df.with_columns(exprs)

    print("✓ Reglas de negocio aplicadas")
    return df


# ===============================================================
# 2. LIMPIEZA DE STRINGS (Genérica)
# ===============================================================


def clean_string_columns(df: pl.DataFrame | pd.DataFrame):
    print("🧹 LIMPIANDO COLUMNAS STRING GENÉRICAS...")

    expected_str_columns = [
        "MATRICULA",
        "DIVIPOLA",
        "TIPO_PREDIO_ZONA",
        "CATEGORIA_RURALIDAD",
        "ESTADO_FOLIO",
        "NOMBRE_NATUJUR",
        "DOCUMENTO_JUSTIFICATIVO",  # Agregado
    ]

    if isinstance(df, pd.DataFrame):
        return df  # (Tu lógica pandas aquí)

    # Polars
    safe_cols = [c for c in expected_str_columns if c in df.columns]

    if safe_cols:
        df = df.with_columns(
            [
                pl.col(col)
                .cast(pl.String, strict=False)
                .str.strip_chars()
                .str.to_uppercase()
                .alias(col)
                for col in safe_cols
            ]
        )

    print("✓ Columnas string limpiadas")
    return df


# ===============================================================
# 3. MUNICIPIOS Y DEPARTAMENTOS
# ===============================================================


def clean_municipality_names(df: pl.DataFrame) -> pl.DataFrame:
    # ... (Mismo código tuyo, está bien) ...
    print("→ Normalizando nombres de municipios...")
    if "MUNICIPIO" not in df.columns:
        return df

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

    # Mapeo
    for old, new in MUNICIPALITY_MAPPING.items():
        # Normalizar key del dict por si acaso
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
            .then(pl.lit(new.upper()))
            .otherwise(pl.col("MUNICIPIO"))
            .alias("MUNICIPIO")
        )
    return df


def clean_department_names(df):
    # ... (Mismo código tuyo) ...
    print("Normalizando nombres de departamentos...")
    if isinstance(df, pd.DataFrame):
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
# 4. FECHAS Y NUMÉRICOS
# ===============================================================


def parse_dates(df):
    print("📅 Parseando fechas (placeholder simple)...")

    if isinstance(df, pd.DataFrame):
        for col in DATE_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
        return df

    # Versión Polars simple: intentar parsear a Date con formato ISO u otros
    for col in DATE_COLUMNS:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col)
                .cast(pl.String, strict=False)
                .str.strptime(pl.Date, strict=False)
                .alias(col)
            )
    return df


def clean_numeric_values(df):
    print("Limpiando valores numéricos (Formato COP: 1,000,000)...")

    if isinstance(df, pd.DataFrame):
        # versión pandas si la necesitas
        return df

    for col in NUMERIC_COLUMNS:
        if col not in df.columns:
            continue

        if col == "VALOR":
            df = df.with_columns(
                pl.col(col)
                .cast(pl.String)
                .str.replace_all('"', "")  # quitar comillas
                .str.replace_all(",", "")  # quitar comas de miles
                .str.strip_chars()  # quitar espacios
                .cast(pl.Float64, strict=False)  # convertir a número
                .alias(col)
            )

            # opcional: poner negativos como null
            df = df.with_columns(
                pl.when(pl.col(col) < 0).then(None).otherwise(pl.col(col)).alias(col)
            )
        else:
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False).alias(col))

    print("✓ Valores numéricos limpios")
    return df


# ===============================================================
# 5. GESTIÓN FINAL DE NULOS Y DUPLICADOS
# ===============================================================


def handle_missing_values(df, strategy="fill"):
    """
    Manejo genérico de nulos para lo que quedó pendiente.
    NOTA: Las imputaciones específicas (Catastral, ORIP) ya se hicieron en apply_business_rules.
    """
    print(f"Manejando valores nulos restantes: estrategia '{strategy}'")

    # Aquí solo dejamos limpieza genérica o campos secundarios
    specific_fill = {
        "ESTADO_FOLIO": "INACTIVO",
        "DOCUMENTO_JUSTIFICATIVO": "SIN_DOCUMENTO_JUSTIFICATIVO",
    }

    exprs = []
    for col, val in specific_fill.items():
        if col in df.columns:
            exprs.append(pl.col(col).fill_null(pl.lit(val)).alias(col))

    if exprs:
        df = df.with_columns(exprs)

    # Estrategia de llenado general para numéricos (opcional)
    if strategy == "fill":
        # Solo llenar numéricos con 0, no strings
        num_cols = [
            col
            for col, dtype in zip(df.columns, df.dtypes)
            if dtype in [pl.Float64, pl.Int64]
        ]
        if num_cols:
            df = df.with_columns([pl.col(c).fill_null(0) for c in num_cols])

    return df


def remove_duplicates(df):
    # ... (Tu código está bien) ...
    print("Eliminando duplicados...")
    if isinstance(df, pd.DataFrame):
        return df.drop_duplicates(subset=["PK"]) if "PK" in df else df

    subset = ["PK"] if "PK" in df.columns else None  # Fallback por si no hay PK
    df = df.unique(subset=subset)
    print("✓ Duplicados eliminados")
    return df


# ===============================================================
# 6. FUNCIÓN PRINCIPAL (ORQUESTADOR)
# ===============================================================


def apply_all_cleaning(df):
    """
    Ejecuta secuencia completa de limpieza optimizada.
    """
    print("\n" + "=" * 60)
    print("🚀 INICIANDO PROCESO DE LIMPIEZA (Flujo Optimizado)")
    print("=" * 60)

    # 1. Saneamiento de estructura (CRÍTICO: Debe ser primero)
    df = clean_quotes_global(df)

    # 2. Reglas de Negocio (Eliminar columnas basura ANTES de procesar)
    # Incluye limpieza de ORIP y claves catastrales
    df = apply_business_rules(df)

    # 3. Normalización de Tipos
    df = clean_string_columns(df)
    df = clean_municipality_names(df)
    df = clean_department_names(df)

    # 4. Conversiones complejas
    df = parse_dates(df)
    df = clean_numeric_values(df)

    # 5. Finalización
    df = handle_missing_values(df, strategy="fill")  # Rellena lo que falta
    df = remove_duplicates(df)

    print("=" * 60)
    print("✅ LIMPIEZA COMPLETA FINALIZADA")
    print("=" * 60)

    return df
