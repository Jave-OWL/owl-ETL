import logging
import re
from typing import Dict, Any, List, Optional
from datetime import datetime
from difflib import get_close_matches

logger = logging.getLogger(__name__)

# Lista de entidades calificadoras reconocidas en Colombia
ENTIDADES_CALIFICADORAS_RECONOCIDAS = {
    'FITCH RATINGS', 'FITCH', 'STANDARD & POOR\'S', 'S&P',
    'MOODY\'S', 'DBRS', 'BRC', 'BRC INVESTOR SERVICES',
    'VALORA', 'VALORAMERICA', 'CEC', 'CEPAL', 'ICFC'
}


def transform_fic_data(raw_data: Dict[str, Any], filename: str = "desconocido") -> Dict[str, Any]:
    """
    Transforma y limpia los datos brutos extraídos del PDF

    Args:
        raw_data: Datos brutos en formato diccionario
        filename: Nombre del archivo o identificador para logs

    Returns:
        Diccionario con datos transformados y validados
    """
    try:
        logger.info(f"Iniciando transformación de datos FIC: {filename}")

        # Hacer una copia para no modificar el original
        transformed_data = raw_data.copy()

        # Obtener información del FIC para logs contextuales
        fic_info = _obtener_info_fic(transformed_data, filename)

        # 1. Transformar porcentajes a float y validar sumas
        transformed_data = _transform_porcentajes(transformed_data, fic_info)

        # 2. Validar y normalizar entidades calificadoras con coincidencia difusa
        transformed_data = _transform_entidades_calificadoras(transformed_data, fic_info)

        # 3. Transformar y validar fechas
        transformed_data = _transform_fechas(transformed_data, fic_info)

        # 4. Transformar valores numéricos
        transformed_data = _transform_valores_numericos(transformed_data)

        # 5. Validar estructura general
        transformed_data = _validar_estructura_general(transformed_data)

        logger.info(f"Transformación de datos completada exitosamente: {filename}")
        return transformed_data

    except Exception as e:
        logger.error(f"Error en transformación de datos [{filename}]: {str(e)}")
        raise


def _obtener_info_fic(data: Dict[str, Any], filename: str) -> Dict[str, str]:
    """
    Extrae información del FIC para logs contextuales
    """
    info = {
        'filename': filename,
        'gestor': 'Desconocido',
        'nombre_fic': 'Desconocido'
    }

    try:
        if 'fic' in data:
            if 'gestor' in data['fic'] and data['fic']['gestor']:
                info['gestor'] = data['fic']['gestor']
            if 'nombre_fic' in data['fic'] and data['fic']['nombre_fic']:
                info['nombre_fic'] = data['fic']['nombre_fic']
    except:
        pass  # Si hay error, usar valores por defecto

    return info


def _transform_porcentajes(data: Dict[str, Any], fic_info: Dict[str, str]) -> Dict[str, Any]:
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
                if item['participacion'] is not None:
                    total_plazo += item['participacion']

        # Validar suma de plazos (debe ser ~100%)
        _validar_suma_porcentajes(total_plazo, 'plazo_duracion', fic_info)

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

                # Validar suma de categoría (solo si hay datos)
                if total_categoria > 0:
                    _validar_suma_porcentajes(total_categoria, categoria, fic_info)

    # 3. Transformar principales_inversiones
    if 'principales_inversiones' in transformed and isinstance(transformed['principales_inversiones'], list):
        total_inversiones = 0.0
        for inversion in transformed['principales_inversiones']:
            if 'participacion' in inversion:
                inversion['participacion'] = _parse_porcentaje(inversion['participacion'])
                if inversion['participacion'] is not None:
                    total_inversiones += inversion['participacion']

        # Validar suma de inversiones principales
        if total_inversiones > 0:
            _validar_suma_porcentajes(total_inversiones, 'principales_inversiones', fic_info)

    # 4. Transformar rentabilidad_volatilidad (sin validar suma)
    if 'rentabilidad_volatilidad' in transformed and isinstance(transformed['rentabilidad_volatilidad'], list):
        for rv in transformed['rentabilidad_volatilidad']:
            if 'rentabilidad_historica_ea' in rv:
                rent = rv['rentabilidad_historica_ea']
                for key in rent:
                    if rent[key] is not None:
                        rent[key] = _parse_porcentaje(rent[key])

            if 'volatilidad_historica' in rv:
                vol = rv['volatilidad_historica']
                for key in vol:
                    if vol[key] is not None:
                        vol[key] = _parse_porcentaje(vol[key])

    return transformed


def _validar_suma_porcentajes(suma: float, nombre_campo: str, fic_info: Dict[str, str]):
    """
    Valida que la suma de porcentajes sea aproximadamente 100%

    Args:
        suma: Suma total de porcentajes
        nombre_campo: Nombre del campo que se está validando
        fic_info: Información del FIC para logs contextuales
    """
    gestor = fic_info['gestor']
    nombre_fic = fic_info['nombre_fic']
    filename = fic_info['filename']

    if 99.5 <= suma <= 100.5:
        logger.debug(f"✓ [{gestor}] {nombre_campo}: {suma:.2f}%")
    else:
        logger.warning(
            f"SUMA FUERA DE RANGO - FIC: {nombre_fic} | "
            f"Gestor: {gestor} | Archivo: {filename} | "
            f"Campo: {nombre_campo} | Suma: {suma:.2f}%"
        )


def _transform_entidades_calificadoras(data: Dict[str, Any], fic_info: Dict[str, str]) -> Dict[str, Any]:
    """
    Valida y normaliza las entidades calificadoras con coincidencia difusa
    """
    transformed = data.copy()
    gestor = fic_info['gestor']
    filename = fic_info['filename']

    if 'calificacion' in transformed:
        calificacion = transformed['calificacion']

        # Normalizar nombre de entidad calificadora
        if 'entidad_calificadora' in calificacion and calificacion['entidad_calificadora']:
            entidad = calificacion['entidad_calificadora'].upper().strip()

            # Buscar coincidencia difusa (tolerancia 80%)
            entidad_normalizada = _buscar_coincidencia_difusa(entidad, ENTIDADES_CALIFICADORAS_RECONOCIDAS)

            if entidad_normalizada:
                calificacion['entidad_calificadora'] = entidad_normalizada
                calificacion['entidad_calificadora_normalizada'] = True
                logger.debug(f"✓ [{gestor}] Entidad calificadora normalizada: {entidad} → {entidad_normalizada}")
            else:
                calificacion['entidad_calificadora_normalizada'] = False
                logger.warning(
                    f"ENTIDAD NO RECONOCIDA - FIC: {fic_info['nombre_fic']} | "
                    f"Gestor: {gestor} | Archivo: {filename} | "
                    f"Entidad: {entidad}"
                )

    return transformed

def _buscar_coincidencia_difusa(texto: str, opciones: set, umbral: float = 0.8) -> Optional[str]:
    """
    Busca coincidencia difusa de texto en un conjunto de opciones
    """
    if not texto or not opciones:
        return None

    # Convertir set a lista para difflib
    opciones_lista = list(opciones)

    # Buscar coincidencias cercanas
    coincidencias = get_close_matches(texto, opciones_lista, n=1, cutoff=umbral)

    if coincidencias:
        return coincidencias[0]

    # Intentar búsqueda por subcadena si no hay coincidencia exacta
    for opcion in opciones_lista:
        if opcion in texto or texto in opcion:
            return opcion

    return None


def _transform_fechas(data: Dict[str, Any], fic_info: Dict[str, str]) -> Dict[str, Any]:
    """
    Transforma y valida formatos de fecha de manera más robusta
    """
    transformed = data.copy()
    gestor = fic_info['gestor']

    # Campos de fecha a transformar
    campos_fecha = [
        ('fic', 'fecha_corte'),
        ('caracteristicas', 'fecha_inicio_operaciones'),
        ('calificacion', 'fecha_ultima_calificacion')
    ]

    for seccion, campo in campos_fecha:
        if seccion in transformed and campo in transformed[seccion]:
            fecha_val = transformed[seccion][campo]
            if fecha_val and isinstance(fecha_val, str):
                fecha_parseada = _parse_fecha_robusta(fecha_val)
                if fecha_parseada:
                    transformed[seccion][campo] = fecha_parseada
                    logger.debug(f"✓ [{gestor}] Fecha {seccion}.{campo} normalizada: {fecha_val} → {fecha_parseada}")
                else:
                    logger.warning(
                        f"FECHA NO VÁLIDA - FIC: {fic_info['nombre_fic']} | "
                        f"Gestor: {gestor} | Campo: {seccion}.{campo} | "
                        f"Valor: {fecha_val}"
                    )

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


# ===== FUNCIONES AUXILIARES MEJORADAS =====

def _parse_porcentaje(value) -> Optional[float]:
    """Convierte cualquier valor a porcentaje float (ya en notación porcentual)"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        # Verificar si está en notación decimal (0.15) o porcentual (15.0)
        if 0 <= abs(value) <= 1.5:  # Si parece decimal
            return round(value * 100, 4)
        return round(float(value), 4)
    if isinstance(value, str):
        return _parse_porcentaje_str(value)
    return None


def _parse_porcentaje_str(value: str) -> Optional[float]:
    """Convierte string de porcentaje a float (notación porcentual)"""
    if not value or value == "":
        return None

    try:
        # Remover caracteres no numéricos excepto punto, coma y signo negativo
        cleaned = re.sub(r'[^\d.,\-%]', '', value.strip())
        cleaned = cleaned.replace(',', '.')

        # Remover % si existe
        tiene_porcentaje = '%' in cleaned
        cleaned = cleaned.replace('%', '')

        # Convertir a float
        result = float(cleaned)

        # Si tenía % o el número es pequeño, asegurar notación porcentual
        if tiene_porcentaje or abs(result) <= 1.5:
            result = result * 100

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


def _parse_fecha_robusta(fecha_str: str) -> Optional[str]:
    """
    Intenta parsear diferentes formatos de fecha de manera robusta
    """
    if not fecha_str:
        return None

    # Limpiar la fecha
    fecha_limpia = fecha_str.strip()

    # Formatos comunes en Colombia y estándares
    formatos = [
        '%d/%m/%Y',  # 31/07/2025
        '%Y-%m-%d',  # 2025-07-31
        '%d-%m-%Y',  # 31-07-2025
        '%m/%d/%Y',  # 07/31/2025
        '%d/%m/%y',  # 31/07/25
        '%Y/%m/%d',  # 2025/07/31
    ]

    for formato in formatos:
        try:
            fecha_dt = datetime.strptime(fecha_limpia, formato)
            # Validar que sea una fecha razonable (después de 1990)
            if fecha_dt.year >= 1990:
                return fecha_dt.strftime('%Y-%m-%d')
        except ValueError:
            continue

    return None  # No log warning aquí, se maneja en la función llamadora


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