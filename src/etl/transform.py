import json
import logging
from typing import Dict, Any
#from models.schemas import FICSchema

logger = logging.getLogger(__name__)


def clean_json_data(raw_json: str) -> Dict[str, Any]:
    """
    Limpia y valida el JSON extraído

    Args:
        raw_json: JSON crudo extraído de Gemini

    Returns:
        JSON limpio y validado
    """
    try:
        # Parsear JSON
        data = json.loads(raw_json)

        # Validar con Pydantic
        validated_data = FICSchema(**data)

        # Convertir de vuelta a dict
        cleaned_data = validated_data.dict()

        logger.info("JSON validado y limpiado exitosamente")
        return cleaned_data

    except json.JSONDecodeError as e:
        logger.error(f"Error decodificando JSON: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error validando datos: {str(e)}")
        raise


def normalize_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza los datos para la base de datos

    Args:
        data: Datos validados

    Returns:
        Datos normalizados para inserción en BD
    """
    try:
        normalized = data.copy()

        # Normalizar nombres de campos si es necesario
        if 'plazoDuracion' in normalized:
            normalized['plazo_duracion'] = normalized.pop('plazoDuracion')

        if 'composicion_portafolio' in normalized:
            comp = normalized['composicion_portafolio']
            if 'por_activo' in comp:
                comp['por_activo'] = [{'activo': item['activo'], 'participacion': item.get('porcentaje_participacion')}
                                      for item in comp['por_activo']]

        logger.info("Datos normalizados exitosamente")
        return normalized

    except Exception as e:
        logger.error(f"Error normalizando datos: {str(e)}")
        raise