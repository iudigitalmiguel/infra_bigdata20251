# infra_bigdata20251

# Proyecto ETL de Datos de Anime desde Jikan API

Este proyecto implementa un proceso ETL (Extract, Transform, Load) para obtener datos de personajes de anime desde la API Jikan, almacenarlos en una base de datos SQLite y generar evidencias del proceso.

## Descripción de la solución

El sistema realiza las siguientes operaciones:

1. **Extracción**: Obtiene datos de personajes de anime desde la API Jikan mediante solicitudes HTTP.
2. **Transformación**: Procesa los datos obtenidos para adaptarlos al esquema de la base de datos.
3. **Carga**: Almacena los datos en una base de datos SQLite.
4. **Auditoría**: Genera archivos de evidencia (reporte de auditoría y muestra en Excel) para verificar la integridad del proceso.

La implementación utiliza un enfoque orientado a objetos con una clase principal `AnimeDataProcessor` que encapsula todo el flujo ETL y gestiona los recursos necesarios.

## Requisitos previos

- Python 3.8 o superior
- Git

## Instalación y ejecución

### Clonar el repositorio

```bash
git clone https://github.com/tu-usuario/tu-repositorio.git
cd tu-repositorio
```

### Instalar dependencias

```bash
pip install -r requirements.txt
```

O instalar manualmente:

```bash
pip install requests pandas openpyxl
```

### Ejecutar el script principal

```bash
python src/bigdata/ingesta.py
```

## Estructura del proyecto

```
├── .github/
│   └── workflows/
│       └── bigdata.yml
├── src/
│   └── bigdata/
│       ├── static/
│       │   ├── auditoria/
│       │   │   └── auditoria.txt
│       │   └── db/
│       │       ├── anime_data.db
│       │       └── muestra_anime.xlsx
│       └── ingesta.py
├── README.md
├── requirements.txt
└── setup.py
```

## Funcionamiento detallado

El script `ingesta.py` implementa un flujo ETL completo:

1. **Extracción**: Realiza una petición HTTP a la API Jikan para obtener datos de personajes de anime.
2. **Transformación**: Procesa la respuesta JSON y extrae los campos relevantes.
3. **Carga**: Crea una base de datos SQLite y almacena los datos procesados.
4. **Auditoría**:
   - Genera un archivo Excel con una muestra de los datos almacenados.
   - Crea un informe de auditoría que compara los datos extraídos con los almacenados.
   - Verifica la integridad de los datos analizando posibles discrepancias.

## Automatización con GitHub Actions

Este proyecto está configurado para ejecutarse automáticamente a través de GitHub Actions. El workflow está definido en el archivo `.github/workflows/bigdata.yml`.

### Funcionalidades del workflow:

1. Se ejecuta automáticamente en cada push al repositorio.
2. Configura un entorno Python.
3. Instala las dependencias necesarias.
4. Ejecuta el script de ingesta para realizar el proceso ETL.
5. Guarda los archivos generados (base de datos, muestra Excel y reporte de auditoría) como artefactos del workflow.

### Cómo verificar la ejecución:

1. Navega a la pestaña "Actions" en el repositorio de GitHub.
2. Selecciona la ejecución más reciente del workflow "ETL Anime Data Pipeline".
3. En la sección "Artifacts", podrás descargar:
   - La base de datos SQLite (`anime-database`)
   - El archivo Excel con la muestra de datos (`anime-sample`)
   - El reporte de auditoría (`audit-report`)

## Personalización

Si deseas cambiar los parámetros de la API o modificar el comportamiento del proceso ETL, puedes editar las siguientes secciones del archivo `ingesta.py`:

- Para cambiar los parámetros de la API, modifica el diccionario `parametros` en la sección principal del script.
- Para ajustar las rutas de los archivos generados, modifica los atributos `db_path`, `sample_path` y `audit_path` en el constructor de la clase.

## Contribuciones

Las contribuciones son bienvenidas. Por favor, crea un fork del repositorio y envía un Pull Request con tus mejoras.
