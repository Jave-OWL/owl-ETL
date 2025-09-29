import psycopg2
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer, DateTime, Text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from .settings import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
import logging

logger = logging.getLogger(__name__)

# PostgreSQL connection string
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# SQLAlchemy engine
engine = create_engine(DATABASE_URL) # el engine es objeto que (maneja conexiones, drivers, etc.).
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base para modelos
Base = declarative_base()


# Definición de tablas
class FIC(Base):
    __tablename__ = "fic"

    id = Column(Integer, primary_key=True, autoincrement=True)
    nombre_fic = Column(String(255), nullable=False)
    gestor = Column(String(255), nullable=False)
    custodio = Column(String(255))
    fecha_corte = Column(String(10))
    politica_de_inversion = Column(Text)
    created_at = Column(DateTime, server_default='NOW()')

    def __repr__(self):
        return f"<FIC(nombre='{self.nombre_fic}', gestor='{self.gestor}')>"


class ComposicionPortafolio(Base):
    __tablename__ = "composicion_portafolio"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fic_id = Column(Integer, nullable=False)
    tipo_composicion = Column(String(50), nullable=False)  # 'activo', 'tipo_renta', etc.
    categoria = Column(String(255), nullable=False)  # Nombre de la categoría
    participacion = Column(Float)  # En formato decimal
    created_at = Column(DateTime, server_default='NOW()')


class PlazoDuracion(Base):
    __tablename__ = "plazo_duracion"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fic_id = Column(Integer, nullable=False)
    plazo = Column(String(100), nullable=False)
    participacion = Column(Float)  # En formato decimal
    created_at = Column(DateTime, server_default='NOW()')


class Caracteristicas(Base):
    __tablename__ = "caracteristicas"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fic_id = Column(Integer, nullable=False)
    tipo = Column(String(100))
    valor = Column(Float)
    fecha_inicio_operaciones = Column(String(10))
    no_unidades_en_circulacion = Column(Float)
    created_at = Column(DateTime, server_default='NOW()')


class Calificacion(Base):
    __tablename__ = "calificacion"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fic_id = Column(Integer, nullable=False)
    calificacion = Column(String(50))
    fecha_ultima_calificacion = Column(String(10))
    entidad_calificadora = Column(String(255))
    entidad_calificadora_normalizada = Column(String(255))
    created_at = Column(DateTime, server_default='NOW()')


class PrincipalInversion(Base):
    __tablename__ = "principales_inversiones"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fic_id = Column(Integer, nullable=False)
    emisor = Column(String(255))
    participacion = Column(Float)  # En formato decimal
    created_at = Column(DateTime, server_default='NOW()')


class RentabilidadHistorica(Base):
    __tablename__ = "rentabilidad_historica"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fic_id = Column(Integer, nullable=False)
    tipo_participacion = Column(String(50), nullable=False)

    # Rentabilidad histórica efectiva anual
    ultimo_mes = Column(Float)  # Rentabilidad último mes
    ultimos_6_meses = Column(Float)  # Rentabilidad últimos 6 meses
    anio_corrido = Column(Float)  # Rentabilidad año corrido
    ultimo_anio = Column(Float)  # Rentabilidad último año
    ultimos_2_anios = Column(Float)  # Rentabilidad últimos 2 años
    ultimos_3_anios = Column(Float)  # Rentabilidad últimos 3 años

    created_at = Column(DateTime, server_default='NOW()')


class VolatilidadHistorica(Base):
    __tablename__ = "volatilidad_historica"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fic_id = Column(Integer, nullable=False)
    tipo_participacion = Column(String(50), nullable=False)

    # Volatilidad histórica
    ultimo_mes = Column(Float)  # Volatilidad último mes
    ultimos_6_meses = Column(Float)  # Volatilidad últimos 6 meses
    anio_corrido = Column(Float)  # Volatilidad año corrido
    ultimo_anio = Column(Float)  # Volatilidad último año
    ultimos_2_anios = Column(Float)  # Volatilidad últimos 2 años
    ultimos_3_anios = Column(Float)  # Volatilidad últimos 3 años

    created_at = Column(DateTime, server_default='NOW()')


# Tabla para almacenar JSON crudo (backup)
class RawJSON(Base):
    __tablename__ = "raw_json"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fic_id = Column(Integer, nullable=False)
    json_data = Column(JSONB)  # JSON completo
    tipo = Column(String(20))  # 'raw' o 'transformed'
    filename = Column(String(255))
    created_at = Column(DateTime, server_default='NOW()')


def get_db_connection():
    """Get a raw psycopg2 connection"""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        return conn
    except Exception as e:
        logger.error(f"Error conectando a PostgreSQL: {e}")
        raise


def get_db_session():
    """Get a SQLAlchemy session"""
    return SessionLocal()


def create_tables():
    """Crear todas las tablas si no existen"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Tablas de PostgreSQL creadas/verificadas exitosamente")
    except Exception as e:
        logger.error(f"Error creando tablas: {e}")
        raise


def test_connection():
    """Probar conexión a la base de datos"""
    try:
        conn = get_db_connection()
        conn.close()
        logger.info("Conexión a PostgreSQL exitosa")
        return True
    except Exception as e:
        logger.error(f"Error conectando a PostgreSQL: {e}")
        return False