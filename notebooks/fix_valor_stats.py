# Corrección para el error de VALOR como string
# Copiar este código en la celda de "Estadísticas de valores" del notebook
import polars as pl
# Estadísticas de valores
# Primero convertir VALOR a numérico (puede estar como string en el CSV)
df_sample_numeric = df_sample.with_columns([
    pl.col('VALOR').cast(pl.Float64, strict=False).alias('VALOR')
])

valor_stats = df_sample_numeric.select([
    pl.col('VALOR').min().alias('Mínimo'),
    pl.col('VALOR').max().alias('Máximo'),
    pl.col('VALOR').mean().alias('Promedio'),
    pl.col('VALOR').median().alias('Mediana'),
    pl.col('VALOR').std().alias('Desv_Estándar')
])

print("\nEstadísticas de Valores de Transacción:")
print(valor_stats)
