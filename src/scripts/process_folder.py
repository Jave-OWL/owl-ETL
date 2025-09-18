#!/usr/bin/env python3
"""
Script para procesar todos los PDFs en una carpeta
"""
import argparse
import logging
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Importaciones internas
from src.etl.pipeline import process_single_pdf
from src.config.settings import PDF_BASE_PATH, setup_logging

# Configurar logging
logger = setup_logging()


def process_folder(pdf_folder: Path, max_workers: int = 3) -> dict:
    """
    Procesa todos los PDFs en una carpeta

    Args:
        pdf_folder: Carpeta con PDFs a procesar
        max_workers: Número máximo de procesos concurrentes

    Returns:
        Diccionario con resultados del procesamiento
    """
    results = {
        'total': 0,
        'success': 0,
        'failed': 0,
        'failed_files': []
    }

    # Encontrar todos los PDFs en la carpeta
    pdf_files = list(pdf_folder.glob("*.pdf"))
    results['total'] = len(pdf_files)

    if not pdf_files:
        logger.warning(f"No se encontraron PDFs en: {pdf_folder}")
        return results

    logger.info(f"Encontrados {len(pdf_files)} PDFs para procesar")

    # Procesar archivos con ThreadPool para concurrencia limitada
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Crear futures para todos los archivos
        future_to_file = {
            executor.submit(process_single_pdf, str(pdf_file)): pdf_file
            for pdf_file in pdf_files
        }

        # Procesar resultados conforme se completan
        for future in as_completed(future_to_file):
            pdf_file = future_to_file[future]
            try:
                result = future.result()
                results['success'] += 1
                logger.info(f"Procesado exitosamente: {pdf_file.name}")
            except Exception as e:
                results['failed'] += 1
                results['failed_files'].append(str(pdf_file))
                logger.error(f"Error procesando {pdf_file.name}: {str(e)}")

    return results


def validate_pdf_path(pdf_path: str) -> bool:
    """
    Valida que el archivo PDF exista y sea válido

    Args:
        pdf_path: Ruta al archivo PDF

    Returns:
        True si es válido, False en caso contrario
    """
    path = Path(pdf_path)
    return path.exists() and path.is_file() and path.suffix.lower() == '.pdf'

def main():
    """Función principal del script"""
    parser = argparse.ArgumentParser(description='Procesar PDFs de FICs y extraer información estructurada')
    parser.add_argument('--folder', '-f', type=str, default=str(PDF_BASE_PATH),
                        help='Carpeta con PDFs a procesar (por defecto: data/pdfs)')
    parser.add_argument('--workers', '-w', type=int, default=3,
                        help='Número máximo de workers concurrentes (por defecto: 3)')
    parser.add_argument('--single', '-s', type=str,
                        help='Procesar un solo archivo PDF en lugar de toda la carpeta')

    #lee los argumentos para acceder args.folder, args.workers, args.single.
    args = parser.parse_args()

    try:
        if args.single:
            # Procesar un solo archivo
            if validate_pdf_path(args.single):
                logger.info(f"Procesando archivo individual: {args.single}")
                result = process_single_pdf(args.single)
                logger.info(f"Procesamiento completado: {args.single}")
                print(f"Archivo procesado exitosamente: {args.single}")
            else:
                logger.error(f"Archivo no válido: {args.single}")
                print(f"Error: Archivo no válido o no encontrado: {args.single}")
                sys.exit(1)
        else:
            # Procesar carpeta completa
            folder_path = Path(args.folder)
            if not folder_path.exists():
                logger.error(f"Carpeta no encontrada: {folder_path}")
                print(f"Error: Carpeta no encontrada: {folder_path}")
                sys.exit(1)

            logger.info(f"Iniciando procesamiento de carpeta: {folder_path}")
            results = process_folder(folder_path, args.workers)

            # Mostrar resumen
            print("\n" + "=" * 50)
            print("RESUMEN DE PROCESAMIENTO")
            print("=" * 50)
            print(f"Total de PDFs: {results['total']}")
            print(f"Exitosos: {results['success']}")
            print(f"Fallidos: {results['failed']}")

            if results['failed_files']:
                print("\nArchivos con errores:")
                for file in results['failed_files']:
                    print(f"  - {file}")

            print("=" * 50)

    #Ctrl+C ejemplo
    except KeyboardInterrupt:
        logger.info("Procesamiento interrumpido por el usuario")
        print("\nProcesamiento interrumpido")
    except Exception as e:
        logger.error(f"Error en procesamiento: {str(e)}")
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()