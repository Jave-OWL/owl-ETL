#!/usr/bin/env python3
"""
Script para crear las tablas en PostgreSQL
"""
import sys
import os
from pathlib import Path

# Añadir src al path - CORREGIDO
current_dir = Path(__file__).parent
src_path = current_dir.parent
sys.path.insert(0, str(src_path))

print(f"Python path: {sys.path}")

try:
    from config.db import create_tables, test_connection
    print("Módulos importados correctamente")
except ImportError as e:
    print(f"Error importando módulos: {e}")
    sys.exit(1)

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