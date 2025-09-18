import logging
import json
from pathlib import Path
from datetime import datetime
from .extract import extract_text_from_pdf, extract_json_from_text
from .load import save_json_to_file
from src.config.settings import JSON_RAW_PATH

logger = logging.getLogger(__name__)


def process_single_pdf(pdf_path: str, save_raw_json: bool = True) -> dict:
    """
    Procesa un solo PDF y extrae la información estructurada

    Args:
        pdf_path: Ruta al archivo PDF a procesar
        save_raw_json: Si se debe guardar el JSON crudo en archivo

    Returns:
        Diccionario con la información extraída del FIC
    """
    try:
        logger.info(f"Iniciando procesamiento de: {pdf_path}")

        # 1. EXTRACT: Extraer texto del PDF
        extracted_text = extract_text_from_pdf(pdf_path)

        # 2. EXTRACT: Convertir texto a JSON estructurado
        json_output = extract_json_from_text(extracted_text)

        # Parsear el JSON
        fic_data = json.loads(json_output)

        # 3. Guardar JSON crudo si se solicita
        if save_raw_json:
            save_path = save_json_to_file(json_output, Path(pdf_path).name) #.name accede al nombre del archivo
            logger.info(f"JSON guardado en: {save_path}")

        logger.info(f"Procesamiento completado exitosamente: {pdf_path}")
        return fic_data

    except Exception as e:
        logger.error(f"Error procesando {pdf_path}: {str(e)}")
        raise