import logging
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import pipeline.config as cfg

logger = logging.getLogger("Database")

# Setup Declarative Base for ORM
Base = declarative_base()

# 1. Establish Database Connection Engine with Fallback
try:
    # Attempt connecting to PostgreSQL
    logger.info("Initializing connection engine to PostgreSQL...")
    # Add brief connection timeouts (2 seconds) to fail-fast if PostgreSQL is not active
    engine = create_engine(
        cfg.POSTGRES_URL, 
        pool_pre_ping=True, 
        connect_args={"connect_timeout": 2}
    )
    # Test connection
    with engine.connect() as conn:
        logger.info("Database: PostgreSQL connection established successfully.")
except Exception as e:
    logger.warning(f"Database: PostgreSQL connection failed: {e}. Falling back to SQLite.")
    # Fallback to local SQLite DB
    engine = create_engine(
        cfg.SQLITE_URL,
        connect_args={"check_same_thread": False} # Required for sqlite multi-threading
    )
    logger.info(f"Database: SQLite session established at: {cfg.SQLITE_URL}")

# Create local ThreadLocal session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """FastAPI Dependency injector yielding database session instances."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@contextmanager
def get_db_context():
    """Thread-safe context manager for database sessions outside FastAPI routes."""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

def init_tables():
    """Initializes schema tables if not present in target DB."""
    try:
        logger.info("Creating database tables based on ORM models...")
        Base.metadata.create_all(bind=engine)
        logger.info("Database schemas initialized successfully.")
    except Exception as e:
        logger.error(f"Error creating database schemas: {e}")
