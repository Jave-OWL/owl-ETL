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
│ ├── etl/ # Módulos ETL (extract, transform, load)
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
py scripts/create_tables.py
```

### 2. Pipeline completo (PDF → JSON → BD)
```bash
py -m src.scripts.process_folder --folder "data/pdfs" --workers 5
```

### 3. Solo transformación (JSON existentes → BD)
```bash
py -m src.scripts.transform_folder --input data/json_raw --output data/json_transformed --workers 5
```

## 📊 Parámetros de los Scripts
### process_folder.py
- --folder, -f: Carpeta con PDFs (default: data/pdfs)
- --workers, -w: Número de procesos paralelos (default: 3)
- --single, -s: Procesar un solo archivo PDF

### transform_folder.py
- --input, -i: Carpeta con JSONs originales (default: data/json_raw)
- --output, -o: Carpeta para JSONs transformados (default: input/transformed)
- --workers, -w: Número de procesos paralelos (default: 3)
- --single, -s: Transformar un solo archivo JSON