#!/usr/bin/env python3
"""
Script para transformar json_raw a json_transformed
"""
import argparse
import json
import logging
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

# Añadir src al path
src_path = Path(__file__).parent.parent / "src"
sys.path.append(str(src_path))

from src.etl.transform import transform_fic_data, validar_datos_transformados
from src.config.settings import JSON_RAW_PATH, setup_logging

logger = setup_logging()


def extract_date_from_folder_name(folder_path: Path) -> tuple:
    """
    Extrae año y mes del nombre de la carpeta

    Args:
        folder_path: Ruta de la carpeta con formato *_año_mes

    Returns:
        Tupla (año, mes) o (None, None) si no encuentra el patrón
    """
    folder_name = folder_path.name

    # Buscar patrones como _2025_09, _2025_9, 2025_09, etc.
    pattern = r'.*?[_-]?(\d{4})[_-](\d{1,2})$'
    match = re.search(pattern, folder_name)

    if match:
        año = int(match.group(1))
        mes = int(match.group(2))
        logger.info(f"Fecha extraída de carpeta '{folder_name}': {año}-{mes:02d}")
        return año, mes

    logger.warning(f"No se pudo extraer fecha del nombre de carpeta: {folder_name}")
    return None, None


def extract_date_from_json_data(json_data: dict) -> tuple:
    """
    Extrae año y mes del campo fecha_corte en los datos JSON

    Args:
        json_data: Diccionario con datos transformados

    Returns:
        Tupla (año, mes) o (None, None) si no encuentra la fecha
    """
    try:
        # Buscar el campo fecha_corte dentro del objeto "fic"
        fic_data = json_data.get('fic', {})
        fecha_corte = fic_data.get('fecha_corte')

        if fecha_corte and isinstance(fecha_corte, str):
            # Asumir formato YYYY-MM-DD
            date_parts = fecha_corte.split('-')
            if len(date_parts) >= 2:
                año = int(date_parts[0])
                mes = int(date_parts[1])
                logger.debug(f"Fecha extraída del JSON: {año}-{mes:02d}")
                return año, mes

        logger.warning(f"No se pudo extraer fecha_corte del JSON o está en formato incorrecto")
        return None, None

    except Exception as e:
        logger.warning(f"Error extrayendo fecha del JSON: {str(e)}")
        return None, None


def validate_date_with_folder(json_data: dict, input_folder: Path) -> bool:
    """
    Valida que la fecha_corte del JSON coincida con el año-mes de la carpeta input

    Args:
        json_data: Datos transformados del JSON
        input_folder: Carpeta input con formato json_raw_año_mes

    Returns:
        True si coinciden las fechas, False en caso contrario
    """
    # Extraer fecha del nombre de la carpeta input
    folder_año, folder_mes = extract_date_from_folder_name(input_folder)

    # Extraer fecha del JSON
    json_año, json_mes = extract_date_from_json_data(json_data)

    # Si no se pudo extraer alguna fecha, considerar como válido
    if folder_año is None or folder_mes is None:
        logger.warning(f"No se pudo extraer fecha del nombre de carpeta: {input_folder.name}")
        return True
    if json_año is None or json_mes is None:
        logger.warning(f"No se pudo extraer fecha_corte del JSON")
        return True

    # Comparar fechas
    if folder_año == json_año and folder_mes == json_mes:
        logger.info(f"Fechas coinciden: carpeta({folder_año}-{folder_mes:02d}) = JSON({json_año}-{json_mes:02d})")
        return True
    else:
        logger.error(f"FECHAS NO COINCIDEN: "
                     f"carpeta({folder_año}-{folder_mes:02d}) vs "
                     f"JSON({json_año}-{json_mes:02d})")
        return False


def transform_single_json(json_path: Path, output_dir: Path) -> dict:
    """
    Transforma un solo archivo JSON

    Args:
        json_path: Ruta al archivo JSON original
        output_dir: Carpeta donde guardar el JSON transformado

    Returns:
        Diccionario con datos transformados
    """
    try:
        logger.info(f"Transformando: {json_path.name}")

        # 1. Leer JSON original
        with open(json_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)

        # 2. Aplicar transformación
        filename = json_path.stem  # Nombre sin extensión
        transformed_data = transform_fic_data(raw_data)

        # 3. Validar transformación
        if not validar_datos_transformados(transformed_data):
            raise ValueError("Los datos transformados no pasaron la validación")

        # 4. Guardar JSON transformado
        output_path = output_dir / f"{json_path.stem}_transformed.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(transformed_data, f, indent=2, ensure_ascii=False)

        logger.info(f"JSON transformado guardado: {output_path.name}")
        return transformed_data

    except Exception as e:
        logger.error(f"Error transformando {json_path.name}: {str(e)}")
        raise


def validate_transformed_files_with_folder(input_folder: Path, output_folder: Path) -> dict:
    """
    Valida que todos los JSONs transformados coincidan con la fecha de la carpeta input

    Args:
        input_folder: Carpeta input con formato json_raw_año_mes
        output_folder: Carpeta con JSONs transformados

    Returns:
        Diccionario con resultados de validación
    """
    validation_results = {
        'total_validated': 0,
        'date_matches': 0,
        'date_mismatches': 0,
        'mismatched_files': []
    }

    # Encontrar todos los JSONs transformados
    transformed_files = list(output_folder.glob("*_transformed.json"))
    validation_results['total_validated'] = len(transformed_files)

    if not transformed_files:
        logger.warning(f"No se encontraron archivos transformados en: {output_folder}")
        return validation_results

    logger.info(f"Validando {len(transformed_files)} archivos transformados contra carpeta: {input_folder.name}")

    # Extraer fecha de la carpeta una sola vez
    folder_año, folder_mes = extract_date_from_folder_name(input_folder)
    if folder_año is None or folder_mes is None:
        logger.error(f"No se pudo extraer fecha válida de la carpeta: {input_folder.name}")
        return validation_results

    for json_file in transformed_files:
        try:
            # Leer el archivo transformado
            with open(json_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)

            # Validar coincidencia de fechas con la carpeta input
            if validate_date_with_folder(json_data, input_folder):
                validation_results['date_matches'] += 1
            else:
                validation_results['date_mismatches'] += 1
                validation_results['mismatched_files'].append(json_file.name)

        except Exception as e:
            logger.error(f"Error validando {json_file.name}: {str(e)}")
            validation_results['date_mismatches'] += 1
            validation_results['mismatched_files'].append(json_file.name)

    return validation_results


def transform_json_folder(input_dir: Path, output_dir: Path, max_workers: int = 3) -> dict:
    """
    Transforma todos los JSONs en una carpeta

    Args:
        input_dir: Carpeta con JSONs originales
        output_dir: Carpeta donde guardar JSONs transformados
        max_workers: Número máximo de workers concurrentes

    Returns:
        Diccionario con resultados del procesamiento
    """
    results = {
        'total': 0,
        'success': 0,
        'failed': 0,
        'failed_files': []
    }

    # Encontrar todos los JSONs en la carpeta (excluir los ya transformados)
    json_files = [f for f in input_dir.glob("*.json")
                  if not f.name.endswith('_transformed.json')]
    results['total'] = len(json_files)

    if not json_files:
        logger.warning(f"No se encontraron JSONs en: {input_dir}")
        return results

    # Crear carpeta de salida si no existe
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Encontrados {len(json_files)} JSONs para transformar")

    # Procesar archivos con ThreadPool para concurrencia
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Crear futures para todos los archivos
        future_to_file = {
            executor.submit(transform_single_json, json_file, output_dir): json_file
            for json_file in json_files
        }

        # Procesar resultados conforme se completan
        for future in as_completed(future_to_file):
            json_file = future_to_file[future]
            try:
                result = future.result()
                results['success'] += 1
                logger.info(f"Transformado exitosamente: {json_file.name}")
                print(f"Transformado exitosamente: {json_file.name}")
            except Exception as e:
                results['failed'] += 1
                results['failed_files'].append(str(json_file))
                logger.error(f"Error transformando {json_file.name}: {str(e)}")
                print(f"Error transformando {json_file.name}: {str(e)}")

    return results



def create_skip_list_from_validation(validation_results: dict, output_path: Path):
    """
    Crea un archivo de lista de exclusión basado en resultados de validación

    Args:
        validation_results: Resultados de validación de fechas
        output_path: Ruta donde guardar el archivo de exclusión
    """
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("# Lista de archivos con fechas no coincidentes\n")
            f.write("# Generado automáticamente desde validación de fechas\n\n")

            for filename in validation_results.get('mismatched_files', []):
                # Guardar solo el nombre base para mayor flexibilidad
                base_name = filename.replace('_transformed.json', '')
                f.write(f"{base_name}\n")

        logger.info(f"Lista de exclusión creada: {output_path}")
        print(f"Lista de exclusión creada: {output_path}")

    except Exception as e:
        logger.error(f"Error creando lista de exclusión: {str(e)}")
        raise


def main():
    """Función principal del script"""
    parser = argparse.ArgumentParser(description='Transformar JSONs existentes de FICs')
    parser.add_argument('--input', '-i', type=str, default=str(JSON_RAW_PATH),
                        help='Carpeta con JSONs originales (por defecto: data/json_raw)')
    parser.add_argument('--output', '-o', type=str,
                        help='Carpeta para JSONs transformados (por defecto: input/transformed)')
    parser.add_argument('--workers', '-w', type=int, default=3,
                        help='Número máximo de workers concurrentes (por defecto: 3)')
    parser.add_argument('--single', '-s', type=str,
                        help='Transformar un solo archivo JSON en lugar de toda la carpeta')

    args = parser.parse_args()

    try:
        input_dir = Path(args.input)
        output_dir = Path(args.output) if args.output else input_dir / "transformed"

        if args.single:
            # Transformar un solo archivo
            json_file = Path(args.single)
            if not json_file.exists():
                logger.error(f"Archivo no encontrado: {json_file}")
                print(f"Error: Archivo no encontrado: {json_file}")
                sys.exit(1)

            logger.info(f"Transformando archivo individual: {json_file.name}")
            result = transform_single_json(json_file, output_dir)

            # Validar fecha después de transformar
            if validate_date_with_folder(result, input_dir):
                logger.info(f"Transformación completada: {json_file.name}")
                print(f"Archivo transformado exitosamente: {json_file.name}")
            else:
                logger.error(f"Transformación completada pero fecha no coincide: {json_file.name}")
                print(f"Advertencia: Archivo transformado pero fecha no coincide: {json_file.name}")

        else:
            # Transformar carpeta completa
            if not input_dir.exists():
                logger.error(f"Carpeta no encontrada: {input_dir}")
                print(f"Error: Carpeta no encontrada: {input_dir}")
                sys.exit(1)

            logger.info(f"Iniciando transformación de carpeta: {input_dir}")

            # 1. Transformar todos los archivos
            results = transform_json_folder(input_dir, output_dir, args.workers)

            # 2. Validar fechas de todos los archivos transformados
            validation_results = validate_transformed_files_with_folder(input_dir, output_dir)


            if validation_results['mismatched_files']:
                skip_list_path = output_dir / "skip_list.txt"
                create_skip_list_from_validation(validation_results, skip_list_path)

            # Mostrar resumen completo
            print("\n" + "=" * 60)
            print("RESUMEN COMPLETO DE TRANSFORMACIÓN")
            print("=" * 60)
            print(f"Carpeta entrada: {input_dir}")
            print(f"Carpeta salida: {output_dir}")
            folder_año, folder_mes = extract_date_from_folder_name(input_dir)
            print(f"Fecha esperada (de carpeta): {folder_año}-{folder_mes:02d}")
            print("\nTRANSFORMACIÓN:")
            print(f"  Total de JSONs: {results['total']}")
            print(f"  Exitosos: {results['success']}")
            print(f"  Fallidos: {results['failed']}")
            print("\nVALIDACIÓN DE FECHAS:")
            print(f"  Archivos validados: {validation_results['total_validated']}")
            print(f"  Fechas coincidentes: {validation_results['date_matches']}")
            print(f"  Fechas no coincidentes: {validation_results['date_mismatches']}")

            if results['failed_files']:
                print("\nArchivos con errores de transformación:")
                for file in results['failed_files']:
                    print(f"  - {file}")

            if validation_results['mismatched_files']:
                print("\nArchivos con fechas no coincidentes:")
                for file in validation_results['mismatched_files']:
                    print(f"  - {file}")

            print("=" * 60)

    except KeyboardInterrupt:
        logger.info("Transformación interrumpida por el usuario")
        print("\nTransformación interrumpida")
    except Exception as e:
        logger.error(f"Error en transformación: {str(e)}")
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()