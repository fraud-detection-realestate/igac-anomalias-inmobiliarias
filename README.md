# Instituto de Monitoreo y Detección de Anomalías en Dinámica Inmobiliaria en Colombia

## 📋 Descripción del Proyecto

Sistema de análisis y detección de anomalías en transacciones inmobiliarias en Colombia, utilizando datos históricos del IGAC (Instituto Geográfico Agustín Codazzi) del período 2015-2025. El proyecto tiene como objetivo identificar patrones irregulares en el mercado inmobiliario que puedan indicar fraude, lavado de activos o manipulación de precios.

## 🎯 Objetivos

- **Monitoreo continuo**: Análisis de ~34 millones de registros de transacciones inmobiliarias
- **Detección de anomalías**: Identificación de patrones irregulares en precios, volúmenes y comportamientos
- **Normalización de datos**: Estandarización de información para análisis comparativo entre regiones y períodos
- **Análisis temporal**: Seguimiento de la evolución del mercado inmobiliario colombiano

## 🛠️ Stack Tecnológico

### Procesamiento de Datos

- **Python**: Lenguaje principal para análisis y procesamiento
- **Polars/Dask**: Manejo eficiente de grandes volúmenes de datos (preferido sobre Pandas para 34M+ filas)
- **SQL**: Almacenamiento y consultas estructuradas

### Análisis y Visualización

- **Estadística descriptiva**: Cálculo de métricas por municipio y región
- **Machine Learning**: Modelos de detección de anomalías (a definir)

## 📊 Estructura del Dataset

### Campos Principales

| Campo | Descripción |
|-------|-------------|
| `PK` | Identificador único |
| `MATRICULA` | Número de matrícula inmobiliaria |
| `FECHA_RADICA_TEXTO` | Fecha de radicación |
| `FECHA_APERTURA_TEXTO` | Fecha de apertura |
| `YEAR_RADICA` | Año de radicación |
| `ORIP` | Oficina de Registro de Instrumentos Públicos |
| `DIVIPOLA` | Código de división político-administrativa |
| `DEPARTAMENTO` | Departamento |
| `MUNICIPIO` | Municipio |
| `TIPO_PREDIO_ZONA` | Tipo de predio y zona |
| `CATEGORIA_RURALIDAD` | Categoría rural/urbana |
| `NUM_ANOTACION` | Número de anotación |
| `ESTADO_FOLIO` | Estado del folio |
| `FOLIOS_DERIVADOS` | Folios derivados |
| `Dinámica_Inmobiliaria` | Tipo de dinámica |
| `COD_NATUJUR` | Código naturaleza jurídica |
| `NOMBRE_NATUJUR` | Nombre naturaleza jurídica |
| `NUMERO_CATASTRAL` | Número catastral actual |
| `NUMERO_CATASTRAL_ANTIGUO` | Número catastral anterior |
| `DOCUMENTO_JUSTIFICATIVO` | Documento justificativo |
| `COUNT_A` | Contador A |
| `COUNT_DE` | Contador DE |
| `PREDIOS_NUEVOS` | Indicador de predios nuevos |
| `TIENE_VALOR` | Indicador de valor presente |
| `TIENE_MAS_DE_UN_VALOR` | Indicador de múltiples valores |

- [ ] **Normalización geográfica**:
  - Estandarizar nombres de municipios
  - Validar códigos DIVIPOLA
- [ ] **Ajuste monetario**:
  - Convertir todas las monedas a pesos corrientes del año actual
  - Ajustar por inflación/IPC para comparabilidad 2015-2025
- [ ] **Limpieza de campos críticos**:
  - Valor de Transacción
  - Área del Terreno
  - Área Construida
  - Validación de tipos de datos

### ✅ Fase 3: Definición de la "Normalidad"

**Estado**: 🔄 Pendiente

#### Actividades

- [ ] **Análisis estadístico por municipio**:
  - Calcular promedio del valor del m²
  - Calcular desviación estándar
  - Determinar volumen promedio de transacciones por mes
- [ ] **Establecer líneas base**:
  - Definir rangos normales por región
  - Identificar patrones estacionales
  - Documentar comportamientos típicos del mercado

### 🔮 Fases Futuras

- **Fase 4**: Modelado de detección de anomalías
- **Fase 5**: Implementación de alertas y monitoreo
- **Fase 6**: Dashboard y visualización
- **Fase 7**: Reportes y documentación

## 📁 Estructura del Proyecto

```
fraud-detection-realestate/
├── README.md                 # Este archivo
├── task.md                   # Lista de tareas y procesos
├── requirements.txt          # Dependencias Python
├── data/                     # Datos (no versionados)
│   ├── raw/                  # Datos crudos
│   ├── processed/            # Datos procesados
│   └── results/              # Resultados de análisis
├── notebooks/                # Jupyter notebooks para análisis
├── src/                      # Código fuente
│   ├── etl/                  # Scripts de ETL
│   ├── analysis/             # Scripts de análisis
│   └── models/               # Modelos de ML
└── docs/                     # Documentación adicional
```

## 🔧 Instalación

```bash
# Clonar el repositorio
git clone <repository-url>
cd fraud-detection-realestate

# Crear entorno virtual
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt
```

## 📝 Notas Importantes

- **Volumen de datos**: ~34 millones de registros requieren optimización en procesamiento
- **Ajuste inflacionario**: Crítico para comparaciones temporales válidas
- **Calidad de datos**: Se esperan inconsistencias en nombres de municipios y formatos

## 🤝 Contribuciones

Este proyecto está en desarrollo activo. Las contribuciones son bienvenidas siguiendo las mejores prácticas de análisis de datos y detección de fraude.

## 📄 Licencia

[Definir licencia]

---

**Última actualización**: 2025-11-22
