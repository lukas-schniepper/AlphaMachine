# AlphaMachine_core/db.py

from sqlmodel import SQLModel, create_engine, Session
from sqlalchemy.exc import OperationalError
from AlphaMachine_core.config import DATABASE_URL

#  → Damit die Models registriert werden:

engine = create_engine(
    DATABASE_URL,
    echo=False,
)

def init_db():
    """
    Legt alle Tabellen aus models.py an, falls sie noch nicht existieren.
    """
    try:
        # weil wir AlphaMachine_core.models importiert haben,
        # enthält SQLModel.metadata jetzt alle drei Tables
        SQLModel.metadata.create_all(engine)
        print("✅ init_db: create_all() ausgeführt")
    except OperationalError as e:
        print(f"⚠️ Could not init DB tables: {e}")

def get_session() -> Session:
    return Session(engine)
