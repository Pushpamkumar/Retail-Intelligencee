import os
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

logger = logging.getLogger("Database")

Base = declarative_base()

# Read env variables or use defaults
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "store_intelligence")

POSTGRES_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
SQLITE_URL = "sqlite:///./store_intelligence.db"

try:
    logger.info("Initializing connection engine to PostgreSQL...")
    engine = create_engine(
        POSTGRES_URL, 
        pool_pre_ping=True, 
        connect_args={"connect_timeout": 10}
    )
    # Test connection
    with engine.connect() as conn:
        logger.info("Database: PostgreSQL connection established successfully.")
except Exception as e:
    logger.warning(f"Database: PostgreSQL connection failed: {e}. Falling back to SQLite.")
    engine = create_engine(
        SQLITE_URL,
        connect_args={"check_same_thread": False}
    )
    logger.info(f"Database: SQLite session established at: {SQLITE_URL}")

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
    """Context manager for database sessions outside FastAPI endpoints."""
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
        
        # Check and dynamically add missing columns in pos_transactions if needed
        # (e.g. when database was initialized from init.sql without timestamp/basket_value_inr)
        from sqlalchemy import inspect, text
        inspector = inspect(engine)
        columns = [col["name"] for col in inspector.get_columns("pos_transactions")]
        
        with engine.begin() as conn:
            if "timestamp" not in columns:
                logger.info("Adding missing column 'timestamp' to pos_transactions table...")
                try:
                    conn.execute(text("ALTER TABLE pos_transactions ADD COLUMN timestamp TIMESTAMP"))
                except Exception as e:
                    logger.error(f"Could not add column timestamp: {e}")
                    
            if "basket_value_inr" not in columns:
                logger.info("Adding missing column 'basket_value_inr' to pos_transactions table...")
                try:
                    conn.execute(text("ALTER TABLE pos_transactions ADD COLUMN basket_value_inr DOUBLE PRECISION"))
                except Exception as e:
                    try:
                        conn.execute(text("ALTER TABLE pos_transactions ADD COLUMN basket_value_inr FLOAT"))
                    except Exception as e2:
                        logger.error(f"Could not add column basket_value_inr: {e2}")
                        
        logger.info("Database schemas initialized successfully.")
    except Exception as e:
        logger.error(f"Error creating database schemas: {e}")
