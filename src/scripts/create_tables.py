#!/usr/bin/env python3
"""
Script para crear las tablas en PostgreSQL
"""
import sys
from pathlib import Path

# Añadir src al path
src_path = Path(__file__).parent.parent / "src"
sys.path.append(str(src_path))

from src.config.db import create_tables, test_connection


def main():
    print("Creando tablas en PostgreSQL...")

    # Probar conexión
    if not test_connection():
        print("No se pudo conectar a PostgreSQL")
        return

    # Crear tablas
    try:
        create_tables()
        print("Tablas creadas exitosamente")
    except Exception as e:
        print(f"Error creando tablas: {e}")


if __name__ == "__main__":
    main()