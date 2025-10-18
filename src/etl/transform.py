import logging
import re
from typing import Dict, Any, List, Optional
from datetime import datetime
from difflib import get_close_matches
import json
from pathlib import Path

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

        # 5. Agregar tipo fic
        transformed_data = _agregar_tipo_fic(transformed_data, fic_info)

        # 6. Agregar url
        transformed_data = _agregar_url(transformed_data, fic_info)

        # 7. Transformar caracteristicas valor
        # transformed_data = _transform_valores_monetarios(transformed_data, fic_info)

        # 8. Validar estructura general
        transformed_data = _validar_estructura_general(transformed_data)

        logger.info(f"Transformación de datos completada exitosamente: {filename}")
        return transformed_data

    except Exception as e:
        logger.error(f"Error en transformación de datos [{filename}]: {str(e)}")
        raise

def _extraer_tipo_fic(fic_data: Dict[str, Any]) -> str:
    """
    Extrae el tipo de FIC (Renta Fija, Renta Variable, Mixta, Alternativa)
    basándose en el texto de política_de_inversion
    """
    if 'fic' not in fic_data:
        return "Desconocido"

    politica = fic_data['fic'].get('politica_de_inversion', '').lower()

    # Palabras clave para cada tipo
    palabras_clave = {
        "Renta Fija": [
            'renta fija', 'bonos', 'cdt', 'certificado de depósito', 'tes',
            'títulos de deuda', 'deuda pública', 'tasa fija', 'fixed income',
            'instrumentos de deuda', 'papeles comerciales', 'obligaciones'
        ],
        "Renta Variable": [
            'renta variable', 'acciones', 'equity', 'bolsa de valores',
            'mercado accionario', 'stocks', 'índices accionarios',
            'participaciones', 'capitalización bursátil'
        ],
        "Mixta": [
            'mixta', 'balanced', 'balanceado', 'combinado',
            'renta fija y variable', 'fija y variable', 'diversificado',
            'multiactivo', 'multi-activo'
        ],
        "Alternativa": [
            'alternativa', 'alternative', 'hedge fund', 'fondos de cobertura',
            'private equity', 'capital privado', 'inmobiliario', 'real estate',
            'commodities', 'materias primas', 'infraestructura', 'derivados',
            'divisas', 'forex', 'opciones', 'futuros'
        ]
    }

    # Contar ocurrencias de cada tipo
    conteo_tipos = {tipo: 0 for tipo in palabras_clave}

    for tipo, palabras in palabras_clave.items():
        for palabra in palabras:
            if palabra in politica:
                conteo_tipos[tipo] += 1

    # Determinar el tipo con más coincidencias
    tipo_detectado = max(conteo_tipos.items(), key=lambda x: x[1])

    if tipo_detectado[1] > 0:
        return tipo_detectado[0]

    # Si no se detecta por palabras clave, usar lógica basada en contenido
    if 'renta fija' in politica and 'renta variable' in politica:
        return "Mixta"
    elif 'renta fija' in politica:
        return "Renta Fija"
    elif 'renta variable' in politica:
        return "Renta Variable"
    elif any(palabra in politica for palabra in ['alternativa', 'hedge', 'private equity', 'inmobiliario']):
        return "Alternativa"

    return "Desconocido"


def _agregar_url(data: Dict[str, Any], fic_info: Dict[str, str]) -> Dict[str, Any]:
    """
    Agrega la URL del FIC basándose en el archivo fics.json
    """
    transformed = data.copy()

    if 'fic' not in transformed:
        return transformed

    # Obtener información del FIC
    nombre_fic = fic_info['nombre_fic']
    filename = fic_info['filename']

    if nombre_fic == 'Desconocido':
        logger.warning(f"No se pudo determinar nombre_fic para buscar URL: {filename}")
        return transformed

    # Extraer nombre del banco del filename (formato: nombreBanco_nombreFic_raw.json)
    if '_' in filename:
        nombre_banco_raw = filename.split('_')[0].lower()

        # Normalizar nombre del banco para coincidir con las claves en fics.json
        nombre_banco_normalized = _normalizar_nombre_banco(nombre_banco_raw)
        logger.debug(f"Banco normalizado: '{nombre_banco_raw}' -> '{nombre_banco_normalized}'")
    else:
        logger.warning(f"No se pudo extraer nombre del banco del filename: {filename}")
        return transformed

    try:
        #TODO cambiarlo a la que es
        # Cargar el archivo fics.json - ruta relativa desde transform.py
        current_dir = Path(__file__).parent
        fics_json_path = current_dir.parent.parent / 'data' / 'fics.json'

        logger.debug(f"Buscando fics.json en: {fics_json_path}")

        if not fics_json_path.exists():
            logger.warning(f"Archivo fics.json no encontrado en: {fics_json_path}")
            return transformed

        with open(fics_json_path, 'r', encoding='utf-8') as f:
            fics_data = json.load(f)

        # Buscar el banco en los datos - usar coincidencia flexible
        banco_encontrado = _buscar_banco_coincidente(nombre_banco_normalized, fics_data.keys())

        if banco_encontrado:
            bancos_fics = fics_data[banco_encontrado]
            logger.debug(f"FICs encontrados para banco '{banco_encontrado}': {list(bancos_fics.keys())}")

            # Normalizar nombres para búsqueda
            nombre_fic_normalized = _normalizar_nombre_fic(nombre_fic)

            # Buscar coincidencia exacta o parcial
            url_encontrada = None
            mejor_coincidencia = None

            for fic_key, url in bancos_fics.items():
                fic_key_normalized = _normalizar_nombre_fic(fic_key)

                # Coincidencia exacta
                if fic_key_normalized == nombre_fic_normalized:
                    url_encontrada = url
                    mejor_coincidencia = f"exacta: {fic_key}"
                    break
                # Coincidencia parcial (el nombre del FIC contiene la clave o viceversa)
                elif (fic_key_normalized in nombre_fic_normalized or
                      nombre_fic_normalized in fic_key_normalized):
                    # Priorizar la coincidencia más larga (más específica)
                    if not url_encontrada or len(fic_key) > len(
                            mejor_coincidencia.split(': ')[1] if mejor_coincidencia else ''):
                        url_encontrada = url
                        mejor_coincidencia = f"parcial: {fic_key}"

            if url_encontrada:
                transformed['fic']['url'] = url_encontrada
                logger.info(f"URL agregada para '{nombre_fic}' (coincidencia {mejor_coincidencia}): {url_encontrada}")
            else:
                logger.warning(
                    f"No se encontró URL para FIC '{nombre_fic}' en banco '{banco_encontrado}'. Claves disponibles: {list(bancos_fics.keys())}")

        else:
            logger.warning(
                f"Banco '{nombre_banco_normalized}' no encontrado en fics.json. Bancos disponibles: {list(fics_data.keys())}")

    except Exception as e:
        logger.error(f"Error al cargar URL para {nombre_fic}: {str(e)}")

    return transformed


def _normalizar_nombre_banco(nombre_banco: str) -> str:
    """
    Normaliza el nombre del banco para coincidir con las claves en fics.json
    """
    # Mapeo de nombres de bancos del filename a las claves en fics.json
    mapeo_bancos = {
        'bancodebogota': 'bancoDeBogota',
        'bancodeoccidentefiduoccidente': 'bancoDeOccidenteFiduoccidente',
        'credicorpcapital': 'credicorpCapital',
        # Agregar más mapeos según sea necesario
    }

    # Primero buscar en el mapeo
    nombre_normalizado = nombre_banco.lower().replace(' ', '').replace('-', '').replace('_', '')
    if nombre_normalizado in mapeo_bancos:
        return mapeo_bancos[nombre_normalizado]

    # Si no está en el mapeo, intentar coincidencia flexible
    return nombre_banco


def _normalizar_nombre_fic(nombre_fic: str) -> str:
    """
    Normaliza el nombre del FIC para búsqueda
    """
    return (nombre_fic.lower()
            .replace(' ', '')
            .replace('-', '')
            .replace('_', '')
            .replace('fondodeinversioncolectiva', '')
            .replace('fic', '')
            .replace('abierto', '')
            .replace('cerrado', ''))


def _buscar_banco_coincidente(nombre_banco: str, bancos_disponibles: List[str]) -> Optional[str]:
    """
    Busca coincidencia flexible del nombre del banco
    """
    nombre_banco_normalized = nombre_banco.lower().replace(' ', '').replace('-', '').replace('_', '')

    for banco in bancos_disponibles:
        banco_normalized = banco.lower().replace(' ', '').replace('-', '').replace('_', '')

        # Coincidencia exacta
        if banco_normalized == nombre_banco_normalized:
            return banco

        # Coincidencia parcial
        if (nombre_banco_normalized in banco_normalized or
                banco_normalized in nombre_banco_normalized):
            return banco

    # Si no hay coincidencia, buscar la más cercana con difflib
    bancos_lista = list(bancos_disponibles)
    coincidencias = get_close_matches(nombre_banco, bancos_lista, n=1, cutoff=0.6)

    if coincidencias:
        logger.debug(f"Coincidencia difusa encontrada: '{nombre_banco}' -> '{coincidencias[0]}'")
        return coincidencias[0]

    return None


def _agregar_tipo_fic(data: Dict[str, Any], fic_info: Dict[str, str]) -> Dict[str, Any]:
    """
    Extrae y asigna el tipo de FIC basado en la política de inversión
    """
    transformed = data.copy()

    if 'fic' in transformed:
        tipo_fic = _extraer_tipo_fic(transformed)
        transformed['fic']['tipo'] = tipo_fic

        # Log informativo
        gestor = fic_info['gestor']
        nombre_fic = fic_info['nombre_fic']
        logger.info(f"Tipo detectado para {nombre_fic}: {tipo_fic}")

    return transformed

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