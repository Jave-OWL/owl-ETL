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

        # 1. Transformar porcentajes (convertir a decimal si es necesario)
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
    Convierte porcentajes individualmente a formato decimal basado en análisis contextual
    """
    transformed = data.copy()

    # 1. Transformar plazo_duracion
    if 'plazo_duracion' in transformed and isinstance(transformed['plazo_duracion'], list):
        total_plazo = 0.0
        valores_originales = []
        valores_convertidos = []

        # Analizar el patrón de valores en esta categoría - trae cada participacion de la lista plazo_duracion
        valores = [item.get('participacion') for item in transformed['plazo_duracion']
                   if item.get('participacion') is not None]

        # Determinar el formato más probable para esta categoría
        formato_categoria = _determinar_formato_categoria(valores)

        for item in transformed['plazo_duracion']:
            if 'participacion' in item:
                valor_original = item['participacion']
                # Convertir basado en el formato de la categoría
                item['participacion'] = _convertir_porcentaje_inteligente(valor_original, formato_categoria)
                if item['participacion'] is not None:
                    total_plazo += item['participacion']
                    valores_originales.append(valor_original)
                    valores_convertidos.append(item['participacion'])

        # Log detallado para debugging
        logger.debug(f"🔍 {fic_info['nombre_fic']} - plazo_duracion ({formato_categoria}): "
                     f"Original: {valores_originales} → Convertido: {valores_convertidos} → Suma: {total_plazo:.4f}")

        # Validar suma de plazos (debe ser ~1.0 después de la conversión)
        _validar_suma_porcentajes(total_plazo, 'plazo_duracion', fic_info)

    # 2. Transformar composicion_portafolio - cada categoría por separado
    if 'composicion_portafolio' in transformed:
        comp = transformed['composicion_portafolio']

        categorias = [
            'por_activo', 'por_tipo_de_renta', 'por_sector_economico',
            'por_pais_emisor', 'por_moneda', 'por_calificacion'
        ]

        for categoria in categorias:
            if categoria in comp and isinstance(comp[categoria], list):
                total_categoria = 0.0
                valores_cat = [item.get('participacion') for item in comp[categoria]
                               if item.get('participacion') is not None]

                # Determinar formato para esta categoría específica
                formato_cat = _determinar_formato_categoria(valores_cat)

                for item in comp[categoria]:
                    if 'participacion' in item:
                        item['participacion'] = _convertir_porcentaje_inteligente(item['participacion'], formato_cat)
                        if item['participacion'] is not None:
                            total_categoria += item['participacion']

                # Log y validación
                if valores_cat:
                    logger.debug(
                        f"🔍 {fic_info['nombre_fic']} - {categoria} ({formato_cat}): Suma: {total_categoria:.4f}")

                if total_categoria > 0:
                    _validar_suma_porcentajes(total_categoria, categoria, fic_info)

    # 3. Transformar principales_inversiones
    if 'principales_inversiones' in transformed and isinstance(transformed['principales_inversiones'], list):
        total_inversiones = 0.0
        valores_inv = [inv.get('participacion') for inv in transformed['principales_inversiones']
                       if inv.get('participacion') is not None]

        formato_inv = _determinar_formato_categoria(valores_inv)

        for inversion in transformed['principales_inversiones']:
            if 'participacion' in inversion:
                inversion['participacion'] = _convertir_porcentaje_inteligente(inversion['participacion'], formato_inv)
                if inversion['participacion'] is not None:
                    total_inversiones += inversion['participacion']

        if valores_inv:
            logger.debug(
                f"🔍 {fic_info['nombre_fic']} - principales_inversiones ({formato_inv}): Suma: {total_inversiones:.4f}")

        if total_inversiones > 0:
            _validar_suma_porcentajes(total_inversiones, 'principales_inversiones', fic_info)

    # 4. Transformar rentabilidad_volatilidad (siempre decimal, sin validación de suma)
    if 'rentabilidad_volatilidad' in transformed and isinstance(transformed['rentabilidad_volatilidad'], list):
        for rv in transformed['rentabilidad_volatilidad']:
            if 'rentabilidad_historica_ea' in rv:
                rent = rv['rentabilidad_historica_ea']
                for key in rent:
                    if rent[key] is not None:
                        # Rentabilidades: convertir si > 1.0
                        rent[key] = _convertir_si_es_necesario(rent[key])
            if 'volatilidad_historica' in rv:
                vol = rv['volatilidad_historica']
                for key in vol:
                    if vol[key] is not None:
                        # Volatilidades: convertir si > 1.0
                        vol[key] = _convertir_si_es_necesario(vol[key])

    return transformed


def _determinar_formato_categoria(valores: List[float]) -> str:
    """
    Determina el formato más probable para una categoría basado en sus valores
    """
    if not valores:
        return 'desconocido'

    # Filtrar valores válidos
    valores_validos = [v for v in valores if v is not None]
    if not valores_validos:
        return 'desconocido'

    # Calcular suma actual
    suma_actual = sum(valores_validos)

    # Contar valores en diferentes rangos
    count_mayores_1 = sum(1 for v in valores_validos if v > 1.0)
    count_menores_1 = sum(1 for v in valores_validos if v <= 1.0)

    # Si la mayoría de valores son > 1 y la suma está cerca de 100, es x100
    if count_mayores_1 > count_menores_1 and 90 <= suma_actual <= 110:
        return 'x100'
    # Si la mayoría de valores son <= 1 y la suma está cerca de 1, es /100
    elif count_menores_1 > count_mayores_1 and 0.9 <= suma_actual <= 1.1:
        return '/100'
    # Si la suma está cerca de 100, probablemente es x100
    elif 90 <= suma_actual <= 110:
        return 'x100'
    # Si la suma está cerca de 1, probablemente es /100
    elif 0.9 <= suma_actual <= 1.1:
        return '/100'
    else:
        return 'desconocido'


def _convertir_porcentaje_inteligente(value, formato_categoria: str) -> Optional[float]:
    """
    Convierte un porcentaje individual de manera inteligente
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        # Si el formato de la categoría es claro, seguirlo
        if formato_categoria == 'x100':
            # En categoría x100, dividir todos los valores entre 100
            return round(value / 100.0, 6)
        elif formato_categoria == '/100':
            # En categoría /100, mantener todos los valores
            return round(float(value), 6)
        else:
            # Formato desconocido, decidir individualmente
            return _convertir_porcentaje_individual(value)

    if isinstance(value, str):
        try:
            cleaned = re.sub(r'[^\d.,\-%]', '', value.strip())
            cleaned = cleaned.replace(',', '.')
            result = float(cleaned)

            if formato_categoria == 'x100':
                return round(result / 100.0, 6)
            elif formato_categoria == '/100':
                return round(result, 6)
            else:
                return _convertir_porcentaje_individual(result)

        except (ValueError, TypeError):
            logger.warning(f"No se pudo parsear porcentaje: {value}")
            return None

    return None


def _convertir_porcentaje_individual(value) -> Optional[float]:
    """
    Conversión individual cuando no hay contexto de categoría
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        # Si el valor está entre 0.9 y 1.1, probablemente ya es decimal
        if 0.9 <= abs(value) <= 1.1:
            return round(float(value), 6)
        # Si el valor está entre 90 y 110, probablemente es x100
        elif 90 <= abs(value) <= 110:
            return round(value / 100.0, 6)
        # Para otros valores, usar regla general
        elif abs(value) > 1.1:
            return round(value / 100.0, 6)
        else:
            return round(float(value), 6)

    return None


def _convertir_si_es_necesario(value) -> Optional[float]:
    """
    Conversión simple para rentabilidad/volatilidad
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        if abs(value) > 1.0:
            return round(value / 100.0, 6)
        else:
            return round(float(value), 6)

    if isinstance(value, str):
        try:
            cleaned = re.sub(r'[^\d.,\-%]', '', value.strip())
            cleaned = cleaned.replace(',', '.')
            result = float(cleaned)

            if abs(result) > 1.0:
                return round(result / 100.0, 6)
            else:
                return round(result, 6)

        except (ValueError, TypeError):
            return None

    return None

def _validar_suma_porcentajes(suma: float, nombre_campo: str, fic_info: Dict[str, str]):
    """
    Valida que la suma de porcentajes sea aproximadamente 1.0 (formato decimal)
    """
    gestor = fic_info['gestor']
    nombre_fic = fic_info['nombre_fic']
    filename = fic_info['filename']

    # Validar que la suma esté cerca de 1.0 (formato decimal)
    if 0.95 <= suma <= 1.05:
        logger.debug(f"✓ [{gestor}] {nombre_campo}: {suma:.4f} (suma válida)")
    else:
        logger.warning(
            f"SUMA FUERA DE RANGO - FIC: {nombre_fic} | "
            f"Gestor: {gestor} | Archivo: {filename} | "
            f"Campo: {nombre_campo} | Suma: {suma:.4f} (esperado ~1.0)"
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
    Incluye formatos como 'jul-25', '31-jul-25', etc.
    """
    if not fecha_str:
        return None

    # Limpiar la fecha y estandarizar
    fecha_limpia = fecha_str.strip().lower()

    # Mapeo de meses en español a números
    meses_espanol = {
        'ene': '01', 'enero': '01',
        'feb': '02', 'febrero': '02',
        'mar': '03', 'marzo': '03',
        'abr': '04', 'abril': '04',
        'may': '05', 'mayo': '05',
        'jun': '06', 'junio': '06',
        'jul': '07', 'julio': '07',
        'ago': '08', 'agosto': '08',
        'sep': '09', 'septiembre': '09',
        'oct': '10', 'octubre': '10',
        'nov': '11', 'noviembre': '11',
        'dic': '12', 'diciembre': '12'
    }

    # Intentar diferentes patrones

    # Patrón 1: "jul-25" o "jul-2025"
    patron_mes_anio = r'^([a-z]+)-(\d{2,4})$'
    match_mes_anio = re.match(patron_mes_anio, fecha_limpia)
    if match_mes_anio:
        mes_str = match_mes_anio.group(1)
        anio_str = match_mes_anio.group(2)

        if mes_str in meses_espanol:
            mes_num = meses_espanol[mes_str]
            # Manejar año (si es 2 dígitos, asumir 2000+)
            if len(anio_str) == 2:
                anio_num = f"20{anio_str}"
            else:
                anio_num = anio_str

            # Para formato mes-año, usar el primer día del mes
            return f"{anio_num}-{mes_num}-01"

    # Patrón 2: "31-jul-25" o "31-jul-2025"
    patron_dia_mes_anio = r'^(\d{1,2})-([a-z]+)-(\d{2,4})$'
    match_dia_mes_anio = re.match(patron_dia_mes_anio, fecha_limpia)
    if match_dia_mes_anio:
        dia_str = match_dia_mes_anio.group(1)
        mes_str = match_dia_mes_anio.group(2)
        anio_str = match_dia_mes_anio.group(3)

        if mes_str in meses_espanol:
            mes_num = meses_espanol[mes_str]
            # Manejar año (si es 2 dígitos, asumir 2000+)
            if len(anio_str) == 2:
                anio_num = f"20{anio_str}"
            else:
                anio_num = anio_str

            # Validar día
            try:
                dia_num = int(dia_str)
                if 1 <= dia_num <= 31:
                    return f"{anio_num}-{mes_num}-{dia_num:02d}"
            except ValueError:
                pass

    # Patrón 3: "jul/25" o "jul/2025"
    patron_mes_anio_slash = r'^([a-z]+)/(\d{2,4})$'
    match_mes_anio_slash = re.match(patron_mes_anio_slash, fecha_limpia)
    if match_mes_anio_slash:
        mes_str = match_mes_anio_slash.group(1)
        anio_str = match_mes_anio_slash.group(2)

        if mes_str in meses_espanol:
            mes_num = meses_espanol[mes_str]
            if len(anio_str) == 2:
                anio_num = f"20{anio_str}"
            else:
                anio_num = anio_str

            return f"{anio_num}-{mes_num}-01"

    # Patrón 4: "31/jul/25" o "31/jul/2025"
    patron_dia_mes_anio_slash = r'^(\d{1,2})/([a-z]+)/(\d{2,4})$'
    match_dia_mes_anio_slash = re.match(patron_dia_mes_anio_slash, fecha_limpia)
    if match_dia_mes_anio_slash:
        dia_str = match_dia_mes_anio_slash.group(1)
        mes_str = match_dia_mes_anio_slash.group(2)
        anio_str = match_dia_mes_anio_slash.group(3)

        if mes_str in meses_espanol:
            mes_num = meses_espanol[mes_str]
            if len(anio_str) == 2:
                anio_num = f"20{anio_str}"
            else:
                anio_num = anio_str

            try:
                dia_num = int(dia_str)
                if 1 <= dia_num <= 31:
                    return f"{anio_num}-{mes_num}-{dia_num:02d}"
            except ValueError:
                pass

    # Formatos comunes en Colombia y estándares (backup)
    formatos_tradicionales = [
        '%d/%m/%Y',  # 31/07/2025
        '%Y-%m-%d',  # 2025-07-31
        '%d-%m-%Y',  # 31-07-2025
        '%m/%d/%Y',  # 07/31/2025
        '%d/%m/%y',  # 31/07/25
        '%Y/%m/%d',  # 2025/07/31
        '%d-%m-%y',  # 31-07-25
    ]

    for formato in formatos_tradicionales:
        try:
            fecha_dt = datetime.strptime(fecha_limpia, formato)
            # Validar que sea una fecha razonable (después de 1990)
            if fecha_dt.year >= 1990:
                return fecha_dt.strftime('%Y-%m-%d')
        except ValueError:
            continue

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