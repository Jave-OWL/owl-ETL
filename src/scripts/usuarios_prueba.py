#!/usr/bin/env python3
"""
Script para crear usuarios de prueba y relacionarlos con FICs existentes
"""
import sys
from pathlib import Path
import logging

# Añadir src al path
src_path = Path(__file__).parent.parent / "src"
sys.path.append(str(src_path))

from src.config.db import get_db_session, Usuario, FIC, FICRecomendado
from src.config.settings import setup_logging

logger = setup_logging()


def create_test_users():
    """Crear usuarios de prueba: admin y usuario normal"""
    session = None
    try:
        session = get_db_session()

        # Verificar si ya existen usuarios de prueba
        existing_admin = session.query(Usuario).filter_by(correo="admin@owl.com").first()
        existing_user = session.query(Usuario).filter_by(correo="usuario@owl.com").first()
        existing_user_2 = session.query(Usuario).filter_by(correo="usuario2@owl.com").first()

        if existing_admin and existing_user and existing_user_2:
            logger.info("Usuarios de prueba ya existen en la base de datos")
            return existing_admin.id, existing_user.id, existing_user_2

        # 1. Crear usuario administrador
        admin_user = Usuario(
            nombre="Administrador OWL",
            correo="admin@owl.com",
            contrasenia="admin123",  # En producción esto debería estar hasheado
            is_admin=True,
            nivel_riesgo="",
            fecha_nacimiento="18/10/2004"
        )
        session.add(admin_user)

        # 2. Crear usuario normal
        normal_user = Usuario(
            nombre="Usuario Prueba",
            correo="usuario@owl.com",
            contrasenia="user123",  # En producción esto debería estar hasheado
            is_admin=False,
            nivel_riesgo="conservador",
            fecha_nacimiento="18/10/2005"
        )
        session.add(normal_user)

        # 3. = 2.
        normal_user_2 = Usuario(
            nombre="Usuario Prueba 2",
            correo="usuario2@owl.com",
            contrasenia="user123",  # En producción esto debería estar hasheado
            is_admin=False,
            nivel_riesgo="conservador",
            fecha_nacimiento="18/10/2002"
        )
        session.add(normal_user_2)

        session.commit()  # Commit para guardar los usuarios y obtener IDs

        logger.info(f"Usuarios creados: Admin ID={admin_user.id}, User ID={normal_user.id, normal_user_2.id}")
        return admin_user.id, normal_user.id, normal_user_2.id

    except Exception as e:
        logger.error(f"Error creando usuarios: {e}")
        if session:
            session.rollback()
        raise
    finally:
        if session:
            session.close()


def get_random_fic_ids(session, count=5):
    """Obtener IDs aleatorios de FICs existentes"""
    fics = session.query(FIC.id).limit(count).all()
    return [fic[0] for fic in fics]


def create_user_favorites(admin_id, user_id, user_id_2):
    """Crear relaciones de favoritos para los usuarios"""
    session = None
    try:
        session = get_db_session()

        # Obtener algunos FICs existentes
        fic_ids = get_random_fic_ids(session, 7)

        if not fic_ids:
            logger.warning("No hay FICs en la base de datos para crear favoritos")
            return

        # 1. Favoritos para el administrador (todos los FICs)
        for fic_id in fic_ids:
            # Verificar si ya existe la relación para evitar duplicados
            existing = session.query(FICRecomendado).filter_by(
                usuario_id=admin_id,
                fic_id=fic_id
            ).first()

            if not existing:
                favorite = FICRecomendado(
                    usuario_id=admin_id,
                    fic_id=fic_id
                )
                session.add(favorite)

        # 2. Favoritos para el usuario normal (solo los primeros 2 FICs)
        for fic_id in fic_ids[:2]:
            existing = session.query(FICRecomendado).filter_by(
                usuario_id=user_id,
                fic_id=fic_id
            ).first()

            if not existing:
                favorite = FICRecomendado(
                    usuario_id=user_id,
                    fic_id=fic_id
                )
                session.add(favorite)

        # 2. Favoritos para el usuario normal (solo los ultimos 2 FICs)
        for fic_id in fic_ids[2:]:
            existing = session.query(FICRecomendado).filter_by(
                usuario_id=user_id_2,
                fic_id=fic_id
            ).first()

            if not existing:
                favorite = FICRecomendado(
                    usuario_id=user_id_2,
                    fic_id=fic_id
                )
                session.add(favorite)

        session.commit()
        logger.info(f"Favoritos creados: Admin={len(fic_ids)} FICs, User={len(fic_ids[:2])} FICs, User_2={len(fic_ids[2:])} FICs")

    except Exception as e:
        logger.error(f"Error creando favoritos: {e}")
        if session:
            session.rollback()
        raise
    finally:
        if session:
            session.close()


def verify_data():
    """Verificar que los datos se crearon correctamente"""
    session = None
    try:
        session = get_db_session()

        # Verificar usuarios
        admin = session.query(Usuario).filter_by(correo="admin@owl.com").first()
        user = session.query(Usuario).filter_by(correo="usuario@owl.com").first()

        if not admin or not user:
            logger.error("Usuarios no encontrados")
            return False

        # Verificar favoritos usando la relación correcta "favoritos"
        admin_favorites = session.query(FICRecomendado).filter_by(usuario_id=admin.id).count()
        user_favorites = session.query(FICRecomendado).filter_by(usuario_id=user.id).count()

        logger.info(f"Verificación: Admin={admin_favorites} favoritos, User={user_favorites} favoritos")

        # Mostrar detalles de los favoritos
        admin_favs = session.query(FICRecomendado, FIC).join(FIC).filter(
            FICRecomendado.usuario_id == admin.id
        ).all()

        user_favs = session.query(FICRecomendado, FIC).join(FIC).filter(
            FICRecomendado.usuario_id == user.id
        ).all()

        print("\nDetalles de Favoritos:")
        print(f"Admin ({admin.nombre}):")
        for fav, fic in admin_favs:
            print(f"   - {fic.nombre_fic} ({fic.gestor})")

        print(f"Usuario ({user.nombre}):")
        for fav, fic in user_favs:
            print(f"   - {fic.nombre_fic} ({fic.gestor})")

        return True

    except Exception as e:
        logger.error(f"Error en verificación: {e}")
        return False
    finally:
        if session:
            session.close()


def main():
    """Función principal"""
    print("Iniciando creación de datos de prueba...")

    try:
        # 1. Verificar que hay FICs en la base de datos
        session = get_db_session()
        fic_count = session.query(FIC).count()
        session.close()

        if fic_count == 0:
            print("No hay FICs en la base de datos.")
            print("Ejecuta primero: python -m src.scripts.process_folder")
            return

        print(f"✅ Encontrados {fic_count} FICs en la base de datos")

        # 2. Crear usuarios
        print("👥 Creando usuarios de prueba...")
        admin_id, user_id, user_id_2= create_test_users()

        # 3. Crear favoritos
        print("⭐ Creando relaciones de favoritos...")
        create_user_favorites(admin_id, user_id, user_id_2)

        # 4. Verificar
        print("🔍 Verificando datos creados...")
        if verify_data():
            print("\n¡Datos de prueba creados exitosamente!")
            print("\nCredenciales de prueba:")
            print("   Administrador: admin@owl.com / admin123")
            print("   Usuario normal: usuario@owl.com / user123")
        else:
            print("Hubo problemas con la creación de datos")

    except Exception as e:
        print(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()

        print("\n💡 Solución de problemas:")
        print("   1. Asegúrate de que PostgreSQL esté ejecutándose")
        print("   2. Ejecuta: python scripts/create_tables.py")
        print("   3. Ejecuta: python -m src.scripts.process_folder")
        print("   4. Verifica que las relaciones en db.py sean correctas")


if __name__ == "__main__":
    main()