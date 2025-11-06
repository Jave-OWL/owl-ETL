import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
from src.config.settings import JSON_RAW_PATH
#from src.config.db import get_db_connection, get_db_session

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from src.config.db import (
    get_db_session, FIC, ComposicionPortafolio, PlazoDuracion,
    Caracteristicas, Calificacion, PrincipalInversion, RentabilidadHistorica, VolatilidadHistorica, RawJSON
)


logger = logging.getLogger(__name__)


def save_json_to_file(json_data: str, original_filename: str, source_folder: str = None) -> str:
    """
    Guarda el JSON extraído en la carpeta designada

    Args:
        json_data: JSON string a guardar
        original_filename: Nombre del archivo original para naming
        source_folder: Ruta de la carpeta fuente para extraer banco y fecha

    Returns:
        Ruta donde se guardó el archivo
    """
    try:
        # Extraer información de la carpeta fuente
        if source_folder:
            folder_path = Path(source_folder)
            # Obtener el nombre de la carpeta padre (ej: "bancoDeBogota_2025")
            parent_folder_name = folder_path.parent.name if folder_path.parent.name else folder_path.name

            # Extraer nombre del banco (quitar el año)
            nombre_banco = parent_folder_name.split('_')[0]  # "bancoDeBogota_2025" -> "bancoDeBogota"

            # Extraer año y mes de la estructura de carpetas
            año = parent_folder_name.split('_')[1] if '_' in parent_folder_name else "0000"
            mes = folder_path.name  # "07"

        else:
            # Valores por defecto si no se proporciona source_folder
            nombre_banco = "unknown"
            año = "0000"
            mes = "00"

        # Nombre del fondo (sin extensión .pdf)
        nombre_fondo = Path(original_filename).stem

        # Crear nombre de archivo
        json_filename = f"{nombre_banco}_{nombre_fondo}_raw.json"

        # Crear ruta de destino: data/json_raw_año_mes/
        json_dir = Path("data") / f"json_raw_{año}_{mes}"
        json_dir.mkdir(parents=True, exist_ok=True)

        json_path = json_dir / json_filename

        # Guardar el JSON
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(json.loads(json_data), f, indent=2, ensure_ascii=False)

        logger.info(f"JSON guardado en: {json_path}")
        return str(json_path)

    except Exception as e:
        logger.error(f"Error guardando JSON: {str(e)}")
        raise


def load_to_database(transformed_data: Dict[str, Any], filename: str = "desconocido") -> int:
    """
    Carga los datos transformados a PostgreSQL

    Args:
        transformed_data: Datos transformados del FIC
        filename: Nombre del archivo original

    Returns:
        ID del FIC insertado o actualizado en la base de datos
    """
    session = None
    fic_id = None
    is_update = False

    try:
        session = get_db_session()

        fic_data = transformed_data.get('fic', {})
        nombre_fic = fic_data.get('nombre_fic', '')
        url = fic_data.get('url', '')
        fecha_corte_nueva = fic_data.get('fecha_corte')

        # Verificar si ya existe un FIC con la misma URL antes de insertar
        fic_existente = session.query(FIC).filter(
            FIC.url == url,
            FIC.nombre_fic == nombre_fic
        ).first()

        if fic_existente and fecha_corte_nueva:
            fecha_existente = fic_existente.fecha_corte
            if fecha_existente and fecha_corte_nueva > fecha_existente:
                logger.info(f"FIC existente será actualizado con fecha más reciente: {fecha_corte_nueva}")
                is_update = True
                # Primero eliminar datos relacionados existentes
                _delete_related_data(session, fic_existente.id)

        # 1. Insertar o actualizar datos básicos del FIC
        fic_id = _insert_fic_data(session, transformed_data, filename)

        # 2. Insertar composición del portafolio (siempre insertar nuevos registros)
        _insert_composicion_portafolio(session, transformed_data, fic_id)

        # 3. Insertar plazos de duración
        _insert_plazo_duracion(session, transformed_data, fic_id)

        # 4. Insertar características
        _insert_caracteristicas(session, transformed_data, fic_id)

        # 5. Insertar calificación
        _insert_calificacion(session, transformed_data, fic_id)

        # 6. Insertar principales inversiones
        _insert_principales_inversiones(session, transformed_data, fic_id)

        # 7. Insertar rentabilidad y volatilidad
        _insert_rentabilidad_volatilidad(session, transformed_data, fic_id)

        # 8. Guardar JSON transformado como backup
        _insert_raw_json(session, transformed_data, fic_id, 'transformed', filename)

        # Commit de todas las transacciones
        session.commit()

        action = "actualizados" if is_update else "cargados"
        logger.info(f"Datos {action} exitosamente a PostgreSQL - FIC ID: {fic_id}")
        return fic_id

    except Exception as e:
        if session:
            session.rollback()
        logger.error(f"Error cargando datos a PostgreSQL: {str(e)}")
        raise
    finally:
        if session:
            session.close()


def _delete_related_data(session: Session, fic_id: int):
    """Eliminar datos relacionados de un FIC existente para actualización"""
    try:
        # Eliminar registros relacionados en orden para evitar problemas de FK
        session.query(ComposicionPortafolio).filter(ComposicionPortafolio.fic_id == fic_id).delete()
        session.query(PlazoDuracion).filter(PlazoDuracion.fic_id == fic_id).delete()
        session.query(Caracteristicas).filter(Caracteristicas.fic_id == fic_id).delete()
        session.query(Calificacion).filter(Calificacion.fic_id == fic_id).delete()
        session.query(PrincipalInversion).filter(PrincipalInversion.fic_id == fic_id).delete()
        session.query(RentabilidadHistorica).filter(RentabilidadHistorica.fic_id == fic_id).delete()
        session.query(VolatilidadHistorica).filter(VolatilidadHistorica.fic_id == fic_id).delete()

        logger.debug(f"Datos relacionados eliminados para FIC ID: {fic_id}")

    except Exception as e:
        logger.error(f"Error eliminando datos relacionados para FIC {fic_id}: {str(e)}")
        raise


def _insert_fic_data(session: Session, data: Dict[str, Any], filename: str) -> int:
    """Insertar datos básicos del FIC - con verificación de fecha más reciente"""
    fic_data = data.get('fic', {})

    nombre_fic = fic_data.get('nombre_fic', '')
    url = fic_data.get('url', '')
    fecha_corte_nueva = fic_data.get('fecha_corte')

    # Verificar si ya existe un FIC con la misma URL
    fic_existente = session.query(FIC).filter(
        FIC.url == url,
        FIC.nombre_fic == nombre_fic
    ).first()

    if fic_existente:
        # Comparar fechas de corte
        fecha_existente = fic_existente.fecha_corte
        fecha_nueva = fecha_corte_nueva

        logger.info(f"FIC existente encontrado: {nombre_fic}")
        logger.info(f"URL: {url}")
        logger.info(f"Fecha existente: {fecha_existente}")
        logger.info(f"Fecha nueva: {fecha_nueva}")

        # Determinar cuál fecha es más reciente
        if fecha_existente and fecha_nueva:
            if fecha_nueva > fecha_existente:
                # Actualizar el registro existente con los nuevos datos
                logger.info(f"Actualizando FIC con fecha más reciente: {fecha_nueva} (anterior: {fecha_existente})")

                # Actualizar campos
                fic_existente.gestor = fic_data.get('gestor', fic_existente.gestor)
                fic_existente.custodio = fic_data.get('custodio', fic_existente.custodio)
                fic_existente.fecha_corte = fecha_nueva
                fic_existente.politica_de_inversion = fic_data.get('politica_de_inversion',
                                                                   fic_existente.politica_de_inversion)
                fic_existente.tipo = fic_data.get('tipo', fic_existente.tipo)

                session.flush()
                return fic_existente.id
            else:
                logger.info(
                    f"FIC existente tiene fecha más reciente ({fecha_existente}), manteniendo registro existente")
                return fic_existente.id
        else:
            # Si alguna fecha es None, insertar nuevo registro por seguridad
            logger.warning("Una de las fechas de corte es None, insertando nuevo registro")

    # Crear nuevo FIC (si no existe o si hay que insertar nuevo)
    nuevo_fic = FIC(
        nombre_fic=nombre_fic,
        gestor=fic_data.get('gestor', ''),
        custodio=fic_data.get('custodio'),
        fecha_corte=fecha_corte_nueva,
        politica_de_inversion=fic_data.get('politica_de_inversion'),
        tipo=fic_data.get('tipo', ''),
        url=url,
    )

    session.add(nuevo_fic)
    session.flush()  # Para obtener el ID

    logger.debug(f"FIC insertado: {nuevo_fic.nombre_fic} - ID: {nuevo_fic.id}")
    return nuevo_fic.id


def _insert_composicion_portafolio(session: Session, data: Dict[str, Any], fic_id: int):
    """Insertar composición del portafolio"""
    composicion = data.get('composicion_portafolio', {})

    # Mapeo de categorías a nombres de tabla
    categorias = {
        'por_activo': 'activo',
        'por_tipo_de_renta': 'tipo_renta',
        'por_sector_economico': 'sector_economico',
        'por_pais_emisor': 'pais_emisor',
        'por_moneda': 'moneda',
        'por_calificacion': 'calificacion'
    }

    for categoria_key, tipo_composicion in categorias.items():
        items = composicion.get(categoria_key, [])
        for item in items:
            comp = ComposicionPortafolio(
                fic_id=fic_id,
                tipo_composicion=tipo_composicion,
                categoria=item.get('activo') or item.get('tipo') or item.get('sector') or
                          item.get('pais') or item.get('moneda') or item.get('calificacion', ''),
                participacion=item.get('participacion')
            )
            session.add(comp)


def _insert_plazo_duracion(session: Session, data: Dict[str, Any], fic_id: int):
    """Insertar plazos de duración"""
    plazos = data.get('plazo_duracion', [])

    for plazo in plazos:
        pd = PlazoDuracion(
            fic_id=fic_id,
            plazo=plazo.get('plazo', ''),
            participacion=plazo.get('participacion')
        )
        session.add(pd)


def _insert_caracteristicas(session: Session, data: Dict[str, Any], fic_id: int):
    """Insertar características del FIC"""
    caracteristicas = data.get('caracteristicas', {})

    car = Caracteristicas(
        fic_id=fic_id,
        tipo=caracteristicas.get('tipo'),
        valor=caracteristicas.get('valor'),
        fecha_inicio_operaciones=caracteristicas.get('fecha_inicio_operaciones'),
        no_unidades_en_circulacion=caracteristicas.get('no_unidades_en_circulacion')
    )
    session.add(car)


def _insert_calificacion(session: Session, data: Dict[str, Any], fic_id: int):
    """Insertar calificación del FIC"""
    calificacion = data.get('calificacion', {})

    cal = Calificacion(
        fic_id=fic_id,
        calificacion=calificacion.get('calificacion'),
        fecha_ultima_calificacion=calificacion.get('fecha_ultima_calificacion'),
        entidad_calificadora=calificacion.get('entidad_calificadora'),
        entidad_calificadora_normalizada=calificacion.get('entidad_calificadora_normalizada')
    )
    session.add(cal)


def _insert_principales_inversiones(session: Session, data: Dict[str, Any], fic_id: int):
    """Insertar principales inversiones"""
    inversiones = data.get('principales_inversiones', [])

    for inversion in inversiones:
        pi = PrincipalInversion(
            fic_id=fic_id,
            emisor=inversion.get('emisor', ''),
            participacion=inversion.get('participacion')
        )
        session.add(pi)


def _insert_rentabilidad_volatilidad(session: Session, data: Dict[str, Any], fic_id: int):
    """Insertar rentabilidad y volatilidad en tablas separadas"""
    rentabilidades = data.get('rentabilidad_volatilidad', [])

    for rv in rentabilidades:
        tipo_participacion = rv.get('tipo_de_participacion', '')
        rent_hist = rv.get('rentabilidad_historica_ea', {})
        vol_hist = rv.get('volatilidad_historica', {})

        # Insertar rentabilidad histórica
        rent_db = RentabilidadHistorica(
            fic_id=fic_id,
            tipo_participacion=tipo_participacion,
            ultimo_mes=rent_hist.get('ultimo_mes'),
            ultimos_6_meses=rent_hist.get('ultimos_6_meses'),
            anio_corrido=rent_hist.get('anio_corrido'),
            ultimo_anio=rent_hist.get('ultimo_anio'),
            ultimos_2_anios=rent_hist.get('ultimos_2_anios'),
            ultimos_3_anios=rent_hist.get('ultimos_3_anios')
        )
        session.add(rent_db)

        # Insertar volatilidad histórica
        vol_db = VolatilidadHistorica(
            fic_id=fic_id,
            tipo_participacion=tipo_participacion,
            ultimo_mes=vol_hist.get('ultimo_mes'),
            ultimos_6_meses=vol_hist.get('ultimos_6_meses'),
            anio_corrido=vol_hist.get('anio_corrido'),
            ultimo_anio=vol_hist.get('ultimo_anio'),
            ultimos_2_anios=vol_hist.get('ultimos_2_anios'),
            ultimos_3_anios=vol_hist.get('ultimos_3_anios')
        )
        session.add(vol_db)


def _insert_raw_json(session: Session, data: Dict[str, Any], fic_id: int, tipo: str, filename: str):
    """Insertar JSON como backup"""
    raw_json = RawJSON(
        fic_id=fic_id,
        json_data=data,
        tipo=tipo,
        filename=filename
    )
    session.add(raw_json)


# Función para cargar JSON existente
def load_existing_json_to_database(json_data: Dict[str, Any], filename: str = "desconocido") -> int:
    """
    Carga un JSON existente a la base de datos
    Útil para el script transform_folder.py
    """
    return load_to_database(json_data, filename)