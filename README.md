# 🦉 OWL ETL - Procesamiento de Fondos de Inversión Colectiva

Pipeline ETL para extraer, transformar y cargar información de Fondos de Inversión Colectiva (FICs) en Colombia desde PDFs a PostgreSQL.

---

## 📋 Descripción
Este proyecto automatiza la extracción de información financiera de fichas técnicas de FICs usando:

- **LLMWhisperer**: Extracción de texto desde PDFs  
- **Google Gemini**: Procesamiento de texto a JSON estructurado  
- **PostgreSQL**: Almacenamiento estructurado de datos  
- **Python**: Pipeline ETL con procesamiento paralelo  

---

## 🏗️ Arquitectura
```txt
owl-ETL/
├── src/
│ ├── config/ # Configuración y conexión a BD
│ ├── etl/ # funciones ETL (extract, transform, load)
│ └── scripts/ # Scripts ejecutables
├── data/
│ ├── pdfs/ # PDFs originales
│ ├── json_raw/ # JSONs extraídos
│ └── json_transformed/ # JSONs transformados
├── logs/ # Logs de ejecución
```

---

## 🚀 Instalación

### 1. Clonar el repositorio
```bash
git clone <repository-url>
cd owl-ETL
```

### 2. Configurar entorno virtual
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

### 3. Instalar dependencias
```bash
pip install -r requirements.txt
```

## ⚙️ Configuración

### Variables de entorno (.env)
```env
# API Keys
LLMWHISPERER_API_KEY=tu_api_key_llmwhisperer
GEMINI_API_KEY=tu_api_key_gemini

# Base de datos PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=OWL
POSTGRES_USER=tu_usuario
POSTGRES_PASSWORD=tu_password

# Rutas
PDF_BASE_PATH=./data/pdfs
JSON_RAW_PATH=./data/json_raw
JSON_TRANSFORMED_PATH=./data/json_transformed
```

## ⚙️ Uso

### 1. Crear tablas en PostgreSQL (primera vez)
```bash
py -m src.scripts.create_tables
```

### 2. Extract (ejemplo)
```bash
#py -m src.scripts.script_extract --folder "../owl-web-scraping/fichasTecnicas/nombreBanco_año/mes" --workers noWorwers
py -m src.scripts.script_extract --folder "../owl-web-scraping/fichasTecnicas/bancoDeBogota_2025/07" --workers 5  
```

### 3. Transform (ejemplo)
```bash
# py -m src.scripts.script_transform --input data/json_raw_año_mes --output data/json_transformed_año_mes
py -m src.scripts.script_transform --input data/json_raw_2025_07 --output data/json_transformed_2025_07
```

### 4. Load (ejemplo)
```bash
# py -m src.scripts.script_load  --input data/json_transformed_año_mes --skip-list data/json_transformed_año_mes/skip_list.txt
py -m src.scripts.script_load  --input data/json_transformed_2025_07 --skip-list data/json_transformed_2025_07/skip_list.txt
```

### 5. Load - Usuarios Prueba
```bash
py -m src.scripts.usuarios_prueba 
```


## 📊 Parámetros de los Scripts
### script_extract.py
- --folder, -f: Carpeta con PDFs
- --workers, -w: Número de procesos paralelos (default: 3)
- --single, -s: Procesar un solo archivo PDF

### script_transform.py
- --input, -i: Carpeta con JSONs originales (/json_raw_año_mes)
- --output, -o: Carpeta para JSONs transformados (/json_transformed_año_mes)
- --workers, -w: Número de procesos paralelos (default: 3)
- --single, -s: Transformar un solo archivo JSON

### script_load.py
- --input, -i: Carpeta con JSONs transformados
- --workers, -w: Número de procesos paralelos (default: 3)
- --skip-list -s: Archivo con lista de archivos a omitir (JSON, TXT o lista separada por comas)
- --skip-files: Lista separada por comas de archivos a omitir