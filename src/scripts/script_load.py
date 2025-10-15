#!/usr/bin/env python3
"""
Script para cargar JSONs transformados a la base de datos
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

from src.etl.load import load_existing_json_to_database
from src.config.settings import setup_logging

logger = setup_logging()


def load_single_json(json_path: Path, skip_files: set) -> dict:
    """
    Carga un solo archivo JSON a la base de datos

    Args:
        json_path: Ruta al archivo JSON transformado
        skip_files: Conjunto de archivos a omitir

    Returns:
        Diccionario con resultados de la carga
    """
    filename = json_path.name

    # Verificar si el archivo debe ser omitido
    if filename in skip_files:
        logger.info(f"Omitiendo archivo (en lista de exclusión): {filename}")
        return {
            'filename': filename,
            'status': 'skipped',
            'fic_id': None,
            'error': 'File in skip list'
        }

    try:
        logger.info(f"Cargando: {filename}")

        # 1. Leer JSON transformado
        with open(json_path, 'r', encoding='utf-8') as f:
            transformed_data = json.load(f)

        # 2. Cargar a la base de datos
        fic_id = load_existing_json_to_database(transformed_data, filename)

        logger.info(f"JSON cargado a PostgreSQL - FIC ID: {fic_id}")
        return {
            'filename': filename,
            'status': 'success',
            'fic_id': fic_id,
            'error': None
        }

    except Exception as e:
        logger.error(f"Error cargando {filename}: {str(e)}")
        return {
            'filename': filename,
            'status': 'failed',
            'fic_id': None,
            'error': str(e)
        }


def load_json_folder(input_dir: Path, skip_files: set, max_workers: int = 3) -> dict:
    """
    Carga todos los JSONs transformados en una carpeta

    Args:
        input_dir: Carpeta con JSONs transformados
        skip_files: Conjunto de archivos a omitir
        max_workers: Número máximo de workers concurrentes

    Returns:
        Diccionario con resultados del procesamiento
    """
    results = {
        'total': 0,
        'success': 0,
        'failed': 0,
        'skipped': 0,
        'loaded_files': [],
        'failed_files': [],
        'skipped_files': []
    }

    # Encontrar todos los JSONs transformados en la carpeta
    json_files = list(input_dir.glob("*_transformed.json"))
    results['total'] = len(json_files)

    if not json_files:
        logger.warning(f"No se encontraron JSONs transformados en: {input_dir}")
        return results

    logger.info(f"Encontrados {len(json_files)} JSONs transformados para cargar")
    logger.info(f"Archivos a omitir: {len(skip_files)}")

    # Procesar archivos con ThreadPool para concurrencia
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Crear futures para todos los archivos
        future_to_file = {
            executor.submit(load_single_json, json_file, skip_files): json_file
            for json_file in json_files
        }

        # Procesar resultados conforme se completan
        for future in as_completed(future_to_file):
            json_file = future_to_file[future]
            try:
                result = future.result()

                if result['status'] == 'success':
                    results['success'] += 1
                    results['loaded_files'].append(json_file.name)
                    logger.info(f"Cargado exitosamente: {json_file.name}")
                    print(f"✓ Cargado: {json_file.name} (FIC ID: {result['fic_id']})")

                elif result['status'] == 'skipped':
                    results['skipped'] += 1
                    results['skipped_files'].append(json_file.name)
                    logger.info(f"Omitido: {json_file.name}")
                    print(f"- Omitido: {json_file.name}")

                else:  # failed
                    results['failed'] += 1
                    results['failed_files'].append({
                        'file': json_file.name,
                        'error': result['error']
                    })
                    logger.error(f"Error cargando {json_file.name}: {result['error']}")
                    print(f"✗ Error: {json_file.name} - {result['error']}")

            except Exception as e:
                results['failed'] += 1
                results['failed_files'].append({
                    'file': json_file.name,
                    'error': str(e)
                })
                logger.error(f"Error procesando {json_file.name}: {str(e)}")
                print(f"✗ Error procesando {json_file.name}: {str(e)}")

    return results


def read_skip_list(skip_list_path: Path) -> set:
    """
    Lee la lista de archivos a omitir desde un archivo

    Args:
        skip_list_path: Ruta al archivo con la lista de exclusión

    Returns:
        Conjunto con nombres de archivos a omitir
    """
    skip_files = set()

    if not skip_list_path.exists():
        logger.warning(f"Archivo de lista de exclusión no encontrado: {skip_list_path}")
        return skip_files

    try:
        with open(skip_list_path, 'r', encoding='utf-8') as f:
            for line in f:
                filename = line.strip()
                if filename and not filename.startswith('#'):
                    # Asegurarse de que tenga la extensión .json si no la tiene
                    if not filename.endswith('.json'):
                        filename = f"{filename}_transformed.json"
                    else:
                        filename = filename.replace('.json', '_transformed.json')
                    skip_files.add(filename)

        logger.info(f"Leídos {len(skip_files)} archivos para omitir de: {skip_list_path}")
        return skip_files

    except Exception as e:
        logger.error(f"Error leyendo lista de exclusión {skip_list_path}: {str(e)}")
        return set()


def main():
    """Función principal del script"""
    parser = argparse.ArgumentParser(description='Cargar JSONs transformados a la base de datos')
    parser.add_argument('--input', '-i', type=str, required=True,
                        help='Carpeta con JSONs transformados (ej: json_transformed_2025_09)')
    parser.add_argument('--skip-list', '-s', type=str,
                        help='Archivo con lista de archivos a omitir (JSON, TXT o lista separada por comas)')
    parser.add_argument('--skip-files', type=str,
                        help='Lista separada por comas de archivos a omitir')
    parser.add_argument('--workers', '-w', type=int, default=3,
                        help='Número máximo de workers concurrentes (por defecto: 3)')

    args = parser.parse_args()

    try:
        input_dir = Path(args.input)

        if not input_dir.exists():
            logger.error(f"Carpeta no encontrada: {input_dir}")
            print(f"Error: Carpeta no encontrada: {input_dir}")
            sys.exit(1)

        # Construir conjunto de archivos a omitir
        skip_files = set()

        # 1. Leer de archivo de lista de exclusión
        if args.skip_list:
            skip_list_path = Path(args.skip_list)
            skip_files.update(read_skip_list(skip_list_path))

        # 2. Leer de lista directa en comando
        if args.skip_files:
            for filename in args.skip_files.split(','):
                filename = filename.strip()
                if filename:
                    if not filename.endswith('.json'):
                        filename = f"{filename}_transformed.json"
                    else:
                        filename = filename.replace('.json', '_transformed.json')
                    skip_files.add(filename)
            logger.info(f"Agregados {len(args.skip_files.split(','))} archivos de línea de comando")



        logger.info(f"Iniciando carga desde: {input_dir}")
        logger.info(f"Total de archivos a omitir: {len(skip_files)}")

        if skip_files:
            print(f"\nArchivos que se omitirán:")
            for filename in sorted(skip_files):
                print(f"  - {filename}")

        # Cargar archivos a la base de datos
        results = load_json_folder(input_dir, skip_files, args.workers)

        # Mostrar resumen
        print("\n" + "=" * 60)
        print("RESUMEN DE CARGA A BASE DE DATOS")
        print("=" * 60)
        print(f"Carpeta: {input_dir}")
        print(f"Total de JSONs: {results['total']}")
        print(f"Exitosos: {results['success']}")
        print(f"Fallidos: {results['failed']}")
        print(f"Omitidos: {results['skipped']}")

        if results['skipped_files']:
            print(f"\nArchivos omitidos ({results['skipped']}):")
            for file in sorted(results['skipped_files']):
                print(f"  - {file}")

        if results['failed_files']:
            print(f"\nArchivos con errores ({results['failed']}):")
            for file_info in results['failed_files']:
                print(f"  - {file_info['file']}: {file_info['error']}")

        if results['loaded_files']:
            print(f"\nArchivos cargados exitosamente ({results['success']}):")
            for file in sorted(results['loaded_files']):
                print(f"  - {file}")

        print("=" * 60)

    except KeyboardInterrupt:
        logger.info("Carga interrumpida por el usuario")
        print("\nCarga interrumpida")
    except Exception as e:
        logger.error(f"Error en carga: {str(e)}")
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()