import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
from src.config.settings import JSON_RAW_PATH
#from src.config.db import get_db_connection, get_db_session

logger = logging.getLogger(__name__)


def save_json_to_file(json_data: str, original_filename: str) -> str:
    """
    Guarda el JSON extraído en la carpeta designada

    Args:
        json_data: JSON string a guardar
        original_filename: Nombre del archivo original para naming

    Returns:
        Ruta donde se guardó el archivo
    """
    try:
        # Crear nombre de archivo único con timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = Path(original_filename).stem
        json_filename = f"{base_name}_{timestamp}.json"
        json_path = JSON_RAW_PATH / json_filename

        # Asegurar que la carpeta existe
        JSON_RAW_PATH.mkdir(parents=True, exist_ok=True)

        # Guardar el JSON
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(json.loads(json_data), f, indent=2, ensure_ascii=False)

        return str(json_path)

    except Exception as e:
        logger.error(f"Error guardando JSON: {str(e)}")
        raise


#def load_to_relational_db