#!/usr/bin/env python3
"""
Script solo para transformar JSONs existentes de FICs
"""
import argparse
import json
import logging
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Añadir src al path
src_path = Path(__file__).parent.parent / "src"
sys.path.append(str(src_path))

from src.etl.transform import transform_fic_data, validar_datos_transformados
from src.config.settings import JSON_RAW_PATH, setup_logging

logger = setup_logging()


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
            logger.info(f"Transformación completada: {json_file.name}")
            print(f"Archivo transformado exitosamente: {json_file.name}")

        else:
            # Transformar carpeta completa
            if not input_dir.exists():
                logger.error(f"Carpeta no encontrada: {input_dir}")
                print(f"Error: Carpeta no encontrada: {input_dir}")
                sys.exit(1)

            logger.info(f"Iniciando transformación de carpeta: {input_dir}")
            results = transform_json_folder(input_dir, output_dir, args.workers)

            # Mostrar resumen
            print("\n" + "=" * 50)
            print("RESUMEN DE TRANSFORMACIÓN")
            print("=" * 50)
            print(f"Carpeta entrada: {input_dir}")
            print(f"Carpeta salida: {output_dir}")
            print(f"Total de JSONs: {results['total']}")
            print(f"Exitosos: {results['success']}")
            print(f"Fallidos: {results['failed']}")

            if results['failed_files']:
                print("\nArchivos con errores:")
                for file in results['failed_files']:
                    print(f"  - {file}")

            print("=" * 50)

    except KeyboardInterrupt:
        logger.info("Transformación interrumpida por el usuario")
        print("\nTransformación interrumpida")
    except Exception as e:
        logger.error(f"Error en transformación: {str(e)}")
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()