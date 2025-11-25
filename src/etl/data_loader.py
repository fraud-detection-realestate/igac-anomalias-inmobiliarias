"""
Módulo para carga eficiente de datos del IGAC.
Soporta carga en chunks y lazy loading con Polars.
"""

import polars as pl
import pandas as pd
from pathlib import Path
from typing import Optional, Iterator
from tqdm import tqdm
import sys
import os

sys.path.append(str(Path(__file__).parent.parent))
from utils.config import CHUNK_SIZE, SAMPLE_SIZE, IGAC_CSV_FILE


def get_dataset_info(file_path: Path = IGAC_CSV_FILE) -> dict:
    """
    Obtiene información básica del dataset sin cargarlo completamente.

    Args:
        file_path: Ruta al archivo CSV

    Returns:
        Diccionario con información del dataset
    """
    import os

    file_size = os.path.getsize(file_path)
    file_size_gb = file_size / (1024**3)

    # Leer solo el header
    with open(file_path, "r", encoding="utf-8") as f:
        header = f.readline()
        columns = [col.strip('"') for col in header.strip().split(",")]

    # Estimar número de filas (aproximado)
    with open(file_path, "r", encoding="utf-8") as f:
        sample_lines = sum(1 for _ in f)

    info = {
        "file_path": str(file_path),
        "file_size_bytes": file_size,
        "file_size_gb": round(file_size_gb, 2),
        "num_columns": len(columns),
        "columns": columns,
        "estimated_rows": sample_lines - 1,  # Menos el header
    }

    return info


def load_csv_sample(
    file_path: Path = IGAC_CSV_FILE, n_rows: int = SAMPLE_SIZE, use_polars: bool = True
) -> pl.DataFrame | pd.DataFrame:
    """
    Carga una muestra del dataset para exploración rápida.

    Args:
        file_path: Ruta al archivo CSV
        n_rows: Número de filas a cargar
        use_polars: Si True usa Polars, si False usa Pandas

    Returns:
        DataFrame con la muestra
    """
    print(f"Cargando muestra de {n_rows} filas...")

    if use_polars:
        df = pl.read_csv(
            file_path, n_rows=n_rows, try_parse_dates=True, ignore_errors=True
        )
    else:
        df = pd.read_csv(file_path, nrows=n_rows, low_memory=False)

    print(f"✓ Muestra cargada: {df.shape[0]} filas, {df.shape[1]} columnas")
    return df


def load_csv_chunked(
    file_path: Path = IGAC_CSV_FILE,
    chunk_size: int = CHUNK_SIZE,
    use_polars: bool = True,
) -> Iterator[pl.DataFrame] | Iterator[pd.DataFrame]:
    """
    Carga el dataset en chunks para procesamiento eficiente.

    Args:
        file_path: Ruta al archivo CSV
        chunk_size: Número de filas por chunk
        use_polars: Si True usa Polars, si False usa Pandas

    Yields:
        Chunks del DataFrame
    """
    if use_polars:
        # Polars no tiene chunking nativo, usar batched reader
        reader = pl.read_csv_batched(
            file_path, batch_size=chunk_size, try_parse_dates=True, ignore_errors=True
        )

        batches = reader.next_batches(10)  # Procesar en lotes
        while batches:
            for batch in batches:
                yield batch
            batches = reader.next_batches(10)
    else:
        # Pandas tiene soporte nativo para chunks
        for chunk in pd.read_csv(file_path, chunksize=chunk_size, low_memory=False):
            yield chunk


def load_full_dataset_lazy(file_path: Path = IGAC_CSV_FILE) -> pl.LazyFrame:
    """
    Carga el dataset completo usando lazy evaluation de Polars.
    No carga los datos en memoria hasta que se ejecute una operación.

    Args:
        file_path: Ruta al archivo CSV

    Returns:
        LazyFrame de Polars
    """
    print("Cargando dataset en modo lazy (sin cargar en memoria)...")

    lf = pl.scan_csv(file_path, try_parse_dates=True, ignore_errors=True)

    print("✓ Dataset cargado en modo lazy")
    return lf


def save_to_parquet(
    df: pl.DataFrame | pd.DataFrame,
    output_path: str | Path,  # ← Actualizamos el type hint para permitir str
    compression: str = "snappy",
) -> None:
    """
    Guarda un DataFrame en formato Parquet.
    """

    # --- CORRECCIÓN CLAVE ---
    # Convertimos siempre a objeto Path.
    # Esto arregla el error "AttributeError: 'str' object has no attribute 'parent'"
    output_path = Path(output_path)
    # ------------------------

    print(f"Guardando datos en formato Parquet: {output_path}")

    # Crear directorio si no existe (Ahora funciona seguro)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if isinstance(df, pl.DataFrame):
        df.write_parquet(output_path, compression=compression)
    else:
        df.to_parquet(output_path, compression=compression, index=False)

    # Mostrar tamaño del archivo
    # Usamos stat() que es nativo de Path, es más limpio que importar os
    try:
        file_size_mb = output_path.stat().st_size / (1024**2)
        print(f"✓ Archivo guardado: {file_size_mb:.2f} MB")
    except Exception:
        print("✓ Archivo guardado (no se pudo verificar tamaño)")


def load_from_parquet(
    file_path: Path, use_polars: bool = True
) -> pl.DataFrame | pd.DataFrame:
    """
    Carga un archivo Parquet.

    Args:
        file_path: Ruta al archivo Parquet
        use_polars: Si True usa Polars, si False usa Pandas

    Returns:
        DataFrame cargado
    """
    print(f"Cargando datos desde Parquet: {file_path}")

    if use_polars:
        df = pl.read_parquet(file_path)
    else:
        df = pd.read_parquet(file_path)

    print(f"✓ Datos cargados: {df.shape[0]} filas, {df.shape[1]} columnas")
    return df


if __name__ == "__main__":
    # Ejemplo de uso
    print("=== Información del Dataset ===")
    info = get_dataset_info()
    for key, value in info.items():
        if key != "columns":
            print(f"{key}: {value}")

    print("\n=== Cargando Muestra ===")
    sample = load_csv_sample(n_rows=1000)
    print(sample.head())
