import logging
import re
from typing import Dict, Any, List, Optional
from datetime import datetime
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)

# Lista de entidades calificadoras reconocidas en Colombia
ENTIDADES_CALIFICADORAS_RECONOCIDAS = {
    'FITCH RATINGS', 'FITCH', 'STANDARD & POOR\'S', 'S&P',
    'MOODY\'S', 'DBRS', 'BRC', 'BRC INVESTOR SERVICES',
    'VALORA', 'VALORAMERICA', 'CEC', 'CEPAL', 'ICFC'
}


def transform_fic_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transforma y limpia los datos brutos extraídos del PDF

    Args:
        raw_data: Datos brutos en formato diccionario

    Returns:
        Diccionario con datos transformados y validados
    """
    try:
        logger.info("Iniciando transformación de datos FIC")

        # Hacer una copia para no modificar el original
        transformed_data = raw_data.copy()

        # 1. Transformar porcentajes a float y validar sumas
        transformed_data = _transform_porcentajes(transformed_data)

        # 2. Validar y normalizar entidades calificadoras
        transformed_data = _transform_entidades_calificadoras(transformed_data)

        # 3. Transformar y validar fechas
        transformed_data = _transform_fechas(transformed_data)

        # 4. Transformar valores numéricos
        transformed_data = _transform_valores_numericos(transformed_data)

        # 5. Validar estructura general
        transformed_data = _validar_estructura_general(transformed_data)

        logger.info("Transformación de datos completada exitosamente")
        return transformed_data

    except Exception as e:
        logger.error(f"Error en transformación de datos: {str(e)}")
        raise


def _transform_porcentajes(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transforma todos los porcentajes a float y valida que sumen 100%
    """
    transformed = data.copy()

    # 1. Transformar plazo_duracion
    if 'plazo_duracion' in transformed and isinstance(transformed['plazo_duracion'], list):
        total_plazo = 0.0
        for item in transformed['plazo_duracion']:
            if 'participacion' in item:
                item['participacion'] = _parse_porcentaje(item['participacion'])
                total_plazo += item['participacion']

        # Validar suma de plazos (puede no ser exactamente 100% por redondeos)
        if 99.5 <= total_plazo <= 100.5:
            logger.debug(f"Suma plazo_duracion: {total_plazo:.2f}%")
        else:
            logger.warning(f"Suma de plazo_duracion fuera de rango: {total_plazo:.2f}%")

    # 2. Transformar composicion_portafolio
    if 'composicion_portafolio' in transformed:
        comp = transformed['composicion_portafolio']

        # Lista de todas las categorías de composición
        categorias = [
            'por_activo', 'por_tipo_de_renta', 'por_sector_economico',
            'por_pais_emisor', 'por_moneda', 'por_calificacion'
        ]

        for categoria in categorias:
            if categoria in comp and isinstance(comp[categoria], list):
                total_categoria = 0.0
                for item in comp[categoria]:
                    if 'participacion' in item:
                        item['participacion'] = _parse_porcentaje(item['participacion'])
                        if item['participacion'] is not None:
                            total_categoria += item['participacion']

                # Validar suma de categoría
                if total_categoria > 0:  # Solo validar si hay datos
                    if 99.5 <= total_categoria <= 100.5:
                        logger.debug(f"Suma {categoria}: {total_categoria:.2f}%")
                    else:
                        logger.warning(f"Suma de {categoria} fuera de rango: {total_categoria:.2f}%")

    # 3. Transformar principales_inversiones
    if 'principales_inversiones' in transformed and isinstance(transformed['principales_inversiones'], list):
        for inversion in transformed['principales_inversiones']:
            if 'participacion' in inversion and isinstance(inversion['participacion'], str):
                # Extraer el valor numérico del string (ej: "14.87%" -> 14.87)
                inversion['participacion'] = _parse_porcentaje_str(inversion['participacion'])



    # 4. Transformar rentabilidad_volatilidad
    if 'rentabilidad_volatilidad' in transformed and isinstance(transformed['rentabilidad_volatilidad'], list):
        for rv in transformed['rentabilidad_volatilidad']:
            if 'rentabilidad_historica_ea' in rv:
                rent = rv['rentabilidad_historica_ea']
                for key in rent:
                    if rent[key] and isinstance(rent[key], str):
                        rent[key] = _parse_porcentaje_str(rent[key])

            if 'volatilidad_historica' in rv:
                vol = rv['volatilidad_historica']
                for key in vol:
                    if vol[key] and isinstance(vol[key], str):
                        vol[key] = _parse_porcentaje_str(vol[key])

    return transformed


def _transform_entidades_calificadoras(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Valida y normaliza las entidades calificadoras
    """
    transformed = data.copy()

    if 'calificacion' in transformed:
        calificacion = transformed['calificacion']

        # Normalizar nombre de entidad calificadora
        if 'entidad_calificadora' in calificacion:
            entidad = calificacion['entidad_calificadora'].upper().strip()

            # Buscar coincidencia parcial en las entidades reconocidas
            entidad_normalizada = None
            for entidad_reconocida in ENTIDADES_CALIFICADORAS_RECONOCIDAS:
                if entidad_reconocida in entidad or entidad in entidad_reconocida:
                    entidad_normalizada = entidad_reconocida
                    break

            if entidad_normalizada:
                calificacion['entidad_calificadora'] = entidad_normalizada
                calificacion['entidad_calificadora_normalizada'] = True
            else:
                calificacion['entidad_calificadora_normalizada'] = False
                logger.warning(f"Entidad calificadora no reconocida: {entidad}")

    return transformed


def _transform_fechas(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transforma y valida formatos de fecha
    """
    transformed = data.copy()

    # Transformar fecha_corte
    if 'fic' in transformed and 'fecha_corte' in transformed['fic']:
        fecha_corte = transformed['fic']['fecha_corte']
        if fecha_corte and isinstance(fecha_corte, str):
            transformed['fic']['fecha_corte'] = _parse_fecha(fecha_corte)

    # Transformar fecha_inicio_operaciones
    if 'caracteristicas' in transformed and 'fecha_inicio_operaciones' in transformed['caracteristicas']:
        fecha_inicio = transformed['caracteristicas']['fecha_inicio_operaciones']
        if fecha_inicio and isinstance(fecha_inicio, str):
            transformed['caracteristicas']['fecha_inicio_operaciones'] = _parse_fecha(fecha_inicio)

    # Transformar fecha_ultima_calificacion
    if 'calificacion' in transformed and 'fecha_ultima_calificacion' in transformed['calificacion']:
        fecha_calificacion = transformed['calificacion']['fecha_ultima_calificacion']
        if fecha_calificacion and isinstance(fecha_calificacion, str):
            transformed['calificacion']['fecha_ultima_calificacion'] = _parse_fecha(fecha_calificacion)

    return transformed


def _transform_valores_numericos(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transforma y valida valores numéricos
    """
    transformed = data.copy()

    # Transformar valor de caracteristicas
    if 'caracteristicas' in transformed:
        car = transformed['caracteristicas']

        if 'valor' in car:
            car['valor'] = _parse_numero(car['valor'])

        if 'no_unidades_en_circulacion' in car:
            car['no_unidades_en_circulacion'] = _parse_numero(car['no_unidades_en_circulacion'])

    return transformed


def _validar_estructura_general(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validaciones generales de estructura de datos
    """
    transformed = data.copy()

    # Asegurar que todas las listas existan
    listas_requeridas = [
        'plazo_duracion', 'principales_inversiones', 'rentabilidad_volatilidad'
    ]

    for lista in listas_requeridas:
        if lista not in transformed or not isinstance(transformed[lista], list):
            transformed[lista] = []

    # Asegurar estructura de composicion_portafolio
    if 'composicion_portafolio' not in transformed:
        transformed['composicion_portafolio'] = {}

    comp = transformed['composicion_portafolio']
    categorias_composicion = [
        'por_activo', 'por_tipo_de_renta', 'por_sector_economico',
        'por_pais_emisor', 'por_moneda', 'por_calificacion'
    ]

    for categoria in categorias_composicion:
        if categoria not in comp or not isinstance(comp[categoria], list):
            comp[categoria] = []

    return transformed


# ===== FUNCIONES AUXILIARES =====

def _parse_porcentaje(value) -> Optional[float]:
    """Convierte cualquier valor a porcentaje float"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return _parse_porcentaje_str(value)
    return None


def _parse_porcentaje_str(value: str) -> Optional[float]:
    """Convierte string de porcentaje a float"""
    if not value or value == "":
        return None

    try:
        # Remover caracteres no numéricos excepto punto y signo negativo
        cleaned = re.sub(r'[^\d.\-%]', '', value.strip())
        cleaned = cleaned.replace('%', '').replace(',', '.')

        # Si termina con signo negativo (raro pero posible)
        if cleaned.endswith('-'):
            cleaned = '-' + cleaned[:-1]

        # Convertir a float
        result = float(cleaned)

        #======================  mirar si sirve asi  ======================
        # Si el valor original tenía % pero el número es grande, dividir por 100
        #if '%' in value and abs(result) > 1:
        #    result = result / 100

        return round(result, 4)

    except (ValueError, TypeError):
        logger.warning(f"No se pudo parsear porcentaje: {value}")
        return None


def _parse_numero(value) -> Optional[float]:
    """Convierte cualquier valor a número float"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            # Limpiar y convertir string
            cleaned = re.sub(r'[^\d.\-]', '', value.strip())
            cleaned = cleaned.replace(',', '.')
            return float(cleaned)
        except (ValueError, TypeError):
            logger.warning(f"No se pudo parsear número: {value}")
            return None
    return None


def _parse_fecha(fecha_str: str) -> Optional[str]:
    """
    Intenta parsear diferentes formatos de fecha y devuelve en formato YYYY-MM-DD
    """
    if not fecha_str:
        return None

    # Formatos comunes en Colombia
    formatos = [
        '%d/%m/%Y',  # 31/07/2025
        '%Y-%m-%d',  # 2025-07-31
        '%d-%m-%Y',  # 31-07-2025
        '%m/%d/%Y',  # 07/31/2025 (menos común)
    ]

    for formato in formatos:
        try:
            fecha_dt = datetime.strptime(fecha_str.strip(), formato)
            return fecha_dt.strftime('%Y-%m-%d')
        except ValueError:
            continue

    logger.warning(f"No se pudo parsear fecha: {fecha_str}")
    return fecha_str  # Devolver original si no se puede parsear


# ===== FUNCIÓN DE VALIDACIÓN RÁPIDA =====

def validar_datos_transformados(data: Dict[str, Any]) -> bool:
    """
    Validación rápida de los datos transformados
    """
    try:
        # Verificar estructura básica
        if 'fic' not in data or 'composicion_portafolio' not in data:
            return False

        # Verificar que los porcentajes sean números
        if 'plazo_duracion' in data:
            for item in data['plazo_duracion']:
                if not isinstance(item.get('participacion'), (float, int, type(None))):
                    return False

        return True
    except:
        return False