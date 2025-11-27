"""
Módulo para validación de calidad de datos.
Genera reportes, detecta outliers y valida rangos.
"""

import polars as pl
import pandas as pd
from pathlib import Path
from typing import Dict, Any
import sys

sys.path.append(str(Path(__file__).parent.parent))
from utils.config import VALID_RANGES, CRITICAL_COLUMNS


def validate_data_quality(df: pl.DataFrame | pd.DataFrame) -> Dict[str, Any]:
    """
    Valida la calidad general del dataset.

    Args:
        df: DataFrame a validar

    Returns:
        Diccionario con métricas de calidad
    """
    print("Validando calidad de datos...")

    quality_report = {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "null_counts": {},
        "duplicate_count": 0,
        "data_types": {},
    }

    if isinstance(df, pl.DataFrame):
        # Contar nulos
        null_counts = df.null_count()
        quality_report["null_counts"] = {col: null_counts[col][0] for col in df.columns}

        # Tipos de datos
        quality_report["data_types"] = {
            col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)
        }

        # Duplicados en PK
        if "PK" in df.columns:
            unique_pks = df["PK"].n_unique()
            quality_report["duplicate_count"] = len(df) - unique_pks
    else:
        # Pandas
        quality_report["null_counts"] = df.isnull().sum().to_dict()
        quality_report["data_types"] = df.dtypes.astype(str).to_dict()

        if "PK" in df.columns:
            quality_report["duplicate_count"] = len(df) - df["PK"].nunique()

    print("✓ Validación completada")
    return quality_report


def generate_quality_report(df: pl.DataFrame | pd.DataFrame) -> str:
    """
    Genera un reporte detallado de calidad de datos.

    Args:
        df: DataFrame

    Returns:
        String con el reporte formateado
    """
    print("Generando reporte de calidad...")

    quality = validate_data_quality(df)

    report = []
    report.append("\n" + "=" * 60)
    report.append("REPORTE DE CALIDAD DE DATOS")
    report.append("=" * 60)

    report.append(f"\n📊 Dimensiones del Dataset:")
    report.append(f"   - Filas: {quality['total_rows']:,}")
    report.append(f"   - Columnas: {quality['total_columns']}")

    report.append(f"\n🔍 Valores Nulos:")
    null_summary = {k: v for k, v in quality["null_counts"].items() if v > 0}
    if null_summary:
        for col, count in sorted(
            null_summary.items(), key=lambda x: x[1], reverse=True
        )[:10]:
            pct = (count / quality["total_rows"]) * 100
            report.append(f"   - {col}: {count:,} ({pct:.2f}%)")
    else:
        report.append("   ✓ No hay valores nulos")

    report.append(f"\n🔄 Duplicados:")
    if quality["duplicate_count"] > 0:
        pct = (quality["duplicate_count"] / quality["total_rows"]) * 100
        report.append(f"   ⚠ {quality['duplicate_count']:,} duplicados ({pct:.2f}%)")
    else:
        report.append("   ✓ No hay duplicados")

    report.append(f"\n📋 Tipos de Datos:")
    type_counts = {}
    for dtype in quality["data_types"].values():
        type_counts[dtype] = type_counts.get(dtype, 0) + 1
    for dtype, count in type_counts.items():
        report.append(f"   - {dtype}: {count} columnas")

    report.append("\n" + "=" * 60 + "\n")

    report_text = "\n".join(report)
    print(report_text)

    return report_text


def detect_outliers(
    df: pl.DataFrame | pd.DataFrame,
    column: str,
    method: str = "iqr",
    threshold: float = 3.0,
) -> pl.DataFrame | pd.DataFrame:
    """
    Detecta outliers en una columna numérica.

    Args:
        df: DataFrame
        column: Nombre de la columna
        method: 'iqr' (rango intercuartil) o 'zscore' (puntuación z)
        threshold: Umbral para detección (1.5 para IQR, 3.0 para z-score)

    Returns:
        DataFrame con columna adicional indicando outliers
    """
    print(f"Detectando outliers en columna '{column}' (método: {method})...")

    outlier_col = f"{column}_OUTLIER"

    if isinstance(df, pl.DataFrame):
        if method == "iqr":
            q1 = df[column].quantile(0.25)
            q3 = df[column].quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - threshold * iqr
            upper_bound = q3 + threshold * iqr

            df = df.with_columns(
                [
                    ((pl.col(column) < lower_bound) | (pl.col(column) > upper_bound))
                    .cast(pl.Int8)
                    .alias(outlier_col)
                ]
            )
        else:  # zscore
            mean = df[column].mean()
            std = df[column].std()

            df = df.with_columns(
                [
                    (((pl.col(column) - mean) / std).abs() > threshold)
                    .cast(pl.Int8)
                    .alias(outlier_col)
                ]
            )
    else:
        # Pandas
        if method == "iqr":
            q1 = df[column].quantile(0.25)
            q3 = df[column].quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - threshold * iqr
            upper_bound = q3 + threshold * iqr

            df[outlier_col] = (
                (df[column] < lower_bound) | (df[column] > upper_bound)
            ).astype(int)
        else:  # zscore
            mean = df[column].mean()
            std = df[column].std()
            df[outlier_col] = (((df[column] - mean) / std).abs() > threshold).astype(
                int
            )

    if isinstance(df, pl.DataFrame):
        outlier_count = df[outlier_col].sum()
    else:
        outlier_count = df[outlier_col].sum()

    pct = (outlier_count / len(df)) * 100
    print(f"✓ Outliers detectados: {outlier_count:,} ({pct:.2f}%)")

    return df


def validate_ranges(
    df: pl.DataFrame | pd.DataFrame,
    column: str,
    min_val: float = None,
    max_val: float = None,
) -> Dict[str, Any]:
    """
    Valida que los valores estén dentro de un rango esperado.

    Args:
        df: DataFrame
        column: Nombre de la columna
        min_val: Valor mínimo esperado
        max_val: Valor máximo esperado

    Returns:
        Diccionario con resultados de validación
    """
    print(f"Validando rangos para columna '{column}'...")

    if isinstance(df, pl.DataFrame):
        actual_min = df[column].min()
        actual_max = df[column].max()

        if min_val is not None:
            below_min = df.filter(pl.col(column) < min_val).shape[0]
        else:
            below_min = 0

        if max_val is not None:
            above_max = df.filter(pl.col(column) > max_val).shape[0]
        else:
            above_max = 0
    else:
        actual_min = df[column].min()
        actual_max = df[column].max()

        below_min = (df[column] < min_val).sum() if min_val is not None else 0
        above_max = (df[column] > max_val).sum() if max_val is not None else 0

    result = {
        "column": column,
        "actual_min": actual_min,
        "actual_max": actual_max,
        "expected_min": min_val,
        "expected_max": max_val,
        "below_min_count": below_min,
        "above_max_count": above_max,
        "valid": below_min == 0 and above_max == 0,
    }

    if result["valid"]:
        print(f"✓ Todos los valores están en el rango esperado")
    else:
        print(f"⚠ Valores fuera de rango: {below_min + above_max}")

    return result


def validate_critical_columns(df: pl.DataFrame | pd.DataFrame) -> bool:
    """
    Valida que las columnas críticas no tengan valores nulos.

    Args:
        df: DataFrame

    Returns:
        True si todas las columnas críticas son válidas
    """
    print("Validando columnas críticas...")

    all_valid = True

    for col in CRITICAL_COLUMNS:
        if col not in df.columns:
            print(f"⚠ Columna crítica '{col}' no encontrada")
            all_valid = False
            continue

        if isinstance(df, pl.DataFrame):
            null_count = df[col].null_count()
        else:
            null_count = df[col].isnull().sum()

        if null_count > 0:
            pct = (null_count / len(df)) * 100
            print(f"⚠ Columna '{col}': {null_count:,} nulos ({pct:.2f}%)")
            all_valid = False
        else:
            print(f"✓ Columna '{col}': sin nulos")

    return all_valid


if __name__ == "__main__":
    # Ejemplo de uso
    from data_loader import load_csv_sample
    from data_cleaner import apply_all_cleaning

    print("Cargando y limpiando muestra...")
    sample = load_csv_sample(n_rows=1000)
    cleaned = apply_all_cleaning(sample)

    print("\nGenerando reporte de calidad...")
    report = generate_quality_report(cleaned)

    print("\nValidando columnas críticas...")
    is_valid = validate_critical_columns(cleaned)
    print(f"\n¿Dataset válido? {is_valid}")
