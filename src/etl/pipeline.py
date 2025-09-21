import logging
import json
from pathlib import Path
from datetime import datetime
from .extract import extract_text_from_pdf, extract_json_from_text
from .load import save_json_to_file
from .transform import transform_fic_data, validar_datos_transformados
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

        # 3. TRANSFORM: Parsear el JSON
        fic_data = json.loads(json_output)

        # 4. LOAD: Guardar JSON crudo si se solicita (stage)
        if save_raw_json:
            save_path = save_json_to_file(json_output, Path(pdf_path).name) #.name accede al nombre del archivo
            logger.info(f"JSON guardado en: {save_path}")

        # 5. TRANSFORM:

        # 6. TRANSFORM:

        # 7. LOAD: Guardar JSON

        # 8. LOAD: Subir JSON de a Postgre

        logger.info(f"Procesamiento completado exitosamente: {pdf_path}")
        return fic_data

    except Exception as e:
        logger.error(f"Error procesando {pdf_path}: {str(e)}")
        raise




def pipeline_per_pdf(pdf_path: str, save_raw_json: bool = True) -> dict:
    """
    Procesa un solo PDF y extrae la información estructurada
    """
    try:
        logger.info(f"Iniciando procesamiento de: {pdf_path}")

        # 1. EXTRACT: Extraer texto del PDF
        extracted_text = extract_text_from_pdf(pdf_path)

        # 2. EXTRACT: Convertir texto a JSON estructurado
        json_output = extract_json_from_text(extracted_text)

        # 3. TRANSFORM: Parsear el JSON crudo
        raw_data = json.loads(json_output)

        # 4. LOAD: Guardar JSON crudo si se solicita (stage)
        if save_raw_json:
            save_path = save_json_to_file(json_output, Path(pdf_path).name)
            logger.info(f"JSON crudo guardado en: {save_path}")

        # 5. TRANSFORM: Limpiar y validar datos
        transformed_data = transform_fic_data(raw_data)

        # 6. TRANSFORM: Validar que la transformación fue exitosa  --esta f esta rara--
        if not validar_datos_transformados(transformed_data):
            raise ValueError("Los datos transformados no pasaron la validación")

        # 7. LOAD: Guardar JSON transformado
        transformed_json = json.dumps(transformed_data, indent=2, ensure_ascii=False)
        save_path_transformed = save_json_to_file(transformed_json, Path(pdf_path).name, "_transformed")
        logger.info(f"JSON transformado guardado en: {save_path_transformed}")

        # 8. LOAD: (Futuro) Subir a PostgreSQL

        logger.info(f"Procesamiento completado exitosamente: {pdf_path}")
        return transformed_data

    except Exception as e:
        logger.error(f"Error procesando {pdf_path}: {str(e)}")
        raise