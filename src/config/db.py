import psycopg2
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer, DateTime, Text, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
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
class Usuario(Base):
    __tablename__ = "usuario"

    id = Column(Integer, primary_key=True, autoincrement=True)
    nombre = Column(String(100), nullable=False)
    correo = Column(String(255), unique=True, nullable=False)
    contrasenia = Column(String(255), nullable=False)
    is_admin = Column(Boolean, default=False)
    nivel_riesgo = Column(String(50))
    created_at = Column(DateTime, server_default='NOW()')
    updated_at = Column(DateTime, server_default='NOW()', onupdate='NOW()')

    # Relaciones
    recomendado = relationship("FICRecomendado", back_populates="usuario")

    def __repr__(self):
        return f"<Usuario(nombre='{self.nombre}', correo='{self.correo}')>"


class FIC(Base):
    __tablename__ = "fic"

    id = Column(Integer, primary_key=True, autoincrement=True)
    nombre_fic = Column(String(255), nullable=False)
    gestor = Column(String(255), nullable=False)
    custodio = Column(String(255))
    fecha_corte = Column(String(10))
    politica_de_inversion = Column(Text)
    tipo = Column(String(100))
    created_at = Column(DateTime, server_default='NOW()')

    # Relaciones - CORREGIDO: Quitado cascade de las relaciones principales
    composiciones = relationship("ComposicionPortafolio", back_populates="fic")
    plazos = relationship("PlazoDuracion", back_populates="fic")
    caracteristicas = relationship("Caracteristicas", back_populates="fic", uselist=False)  # 1:1
    calificacion = relationship("Calificacion", back_populates="fic", uselist=False)  # 1:1
    inversiones = relationship("PrincipalInversion", back_populates="fic")
    rentabilidades = relationship("RentabilidadHistorica", back_populates="fic")
    volatilidades = relationship("VolatilidadHistorica", back_populates="fic")
    raw_jsons = relationship("RawJSON", back_populates="fic")
    recomendado = relationship("FICRecomendado", back_populates="fic")

    def __repr__(self):
        return f"<FIC(nombre='{self.nombre_fic}', gestor='{self.gestor}')>"


class FICRecomendado(Base):
    __tablename__ = "fic_recomendado"

    id = Column(Integer, primary_key=True, autoincrement=True)
    usuario_id = Column(Integer, ForeignKey('usuario.id', ondelete='CASCADE'), nullable=False)
    fic_id = Column(Integer, ForeignKey('fic.id', ondelete='CASCADE'), nullable=False)
    created_at = Column(DateTime, server_default='NOW()')

    # Relaciones
    usuario = relationship("Usuario", back_populates="recomendado")
    fic = relationship("FIC", back_populates="recomendado")

    # Unique constraint para evitar duplicados
    __table_args__ = (UniqueConstraint('usuario_id', 'fic_id', name='uq_usuario_fic'),)


class ComposicionPortafolio(Base):
    __tablename__ = "composicion_portafolio"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fic_id = Column(Integer, ForeignKey('fic.id', ondelete='CASCADE'), nullable=False)
    tipo_composicion = Column(String(50), nullable=False)  # 'activo', 'tipo_renta', etc.
    categoria = Column(String(255), nullable=False)  # Nombre de la categoría
    participacion = Column(Float)  # En formato decimal
    created_at = Column(DateTime, server_default='NOW()')

    # Relación
    fic = relationship("FIC", back_populates="composiciones")


class PlazoDuracion(Base):
    __tablename__ = "plazo_duracion"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fic_id = Column(Integer, ForeignKey('fic.id', ondelete='CASCADE'), nullable=False)
    plazo = Column(String(100), nullable=False)
    participacion = Column(Float)
    created_at = Column(DateTime, server_default='NOW()')

    # Relación
    fic = relationship("FIC", back_populates="plazos")


class Caracteristicas(Base):
    __tablename__ = "caracteristicas"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fic_id = Column(Integer, ForeignKey('fic.id', ondelete='CASCADE'), nullable=False, unique=True)  # UNIQUE para 1:1
    tipo = Column(String(100))
    valor = Column(Float)
    fecha_inicio_operaciones = Column(String(10))
    no_unidades_en_circulacion = Column(Float)
    created_at = Column(DateTime, server_default='NOW()')

    # Relación (1:1 con FIC)
    fic = relationship("FIC", back_populates="caracteristicas")


class Calificacion(Base):
    __tablename__ = "calificacion"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fic_id = Column(Integer, ForeignKey('fic.id', ondelete='CASCADE'), nullable=False, unique=True)  # UNIQUE para 1:1
    calificacion = Column(String(50))
    fecha_ultima_calificacion = Column(String(10))
    entidad_calificadora = Column(String(255))
    entidad_calificadora_normalizada = Column(String(255))
    created_at = Column(DateTime, server_default='NOW()')

    # Relación (1:1 con FIC)
    fic = relationship("FIC", back_populates="calificacion")


class PrincipalInversion(Base):
    __tablename__ = "principales_inversiones"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fic_id = Column(Integer, ForeignKey('fic.id', ondelete='CASCADE'), nullable=False)
    emisor = Column(String(255))
    participacion = Column(Float)  # En formato decimal
    created_at = Column(DateTime, server_default='NOW()')

    # Relación
    fic = relationship("FIC", back_populates="inversiones")


class RentabilidadHistorica(Base):
    __tablename__ = "rentabilidad_historica"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fic_id = Column(Integer, ForeignKey('fic.id', ondelete='CASCADE'), nullable=False)
    tipo_participacion = Column(String(50), nullable=False)
    ultimo_mes = Column(Float)  # Rentabilidad último mes
    ultimos_6_meses = Column(Float)  # Rentabilidad últimos 6 meses
    anio_corrido = Column(Float)  # Rentabilidad año corrido
    ultimo_anio = Column(Float)  # Rentabilidad último año
    ultimos_2_anios = Column(Float)  # Rentabilidad últimos 2 años
    ultimos_3_anios = Column(Float)  # Rentabilidad últimos 3 años
    created_at = Column(DateTime, server_default='NOW()')

    # Relación
    fic = relationship("FIC", back_populates="rentabilidades")


class VolatilidadHistorica(Base):
    __tablename__ = "volatilidad_historica"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fic_id = Column(Integer, ForeignKey('fic.id', ondelete='CASCADE'), nullable=False)
    tipo_participacion = Column(String(50), nullable=False)
    # Volatilidad histórica
    ultimo_mes = Column(Float)  # Volatilidad último mes
    ultimos_6_meses = Column(Float)  # Volatilidad últimos 6 meses
    anio_corrido = Column(Float)  # Volatilidad año corrido
    ultimo_anio = Column(Float)  # Volatilidad último año
    ultimos_2_anios = Column(Float)  # Volatilidad últimos 2 años
    ultimos_3_anios = Column(Float)  # Volatilidad últimos 3 años
    created_at = Column(DateTime, server_default='NOW()')

    # Relación
    fic = relationship("FIC", back_populates="volatilidades")


class RawJSON(Base):
    __tablename__ = "raw_json"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fic_id = Column(Integer, ForeignKey('fic.id', ondelete='CASCADE'), nullable=False)
    json_data = Column(JSONB)  # JSON completo
    tipo = Column(String(20))  # 'raw' o 'transformed'
    filename = Column(String(255))
    created_at = Column(DateTime, server_default='NOW()')

    # Relación
    fic = relationship("FIC", back_populates="raw_jsons")


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