# AlphaMachine_core/db.py

from sqlmodel import create_engine, SQLModel, Session
from AlphaMachine_core.config import DATABASE_URL

# 1) Engine erzeugen
engine = create_engine(DATABASE_URL, echo=False)

# 2) Alle Tabellen (aus models.py) beim ersten Import automatisch anlegen
SQLModel.metadata.create_all(engine)

# 3) Factory-Funktion für Sessions
def get_session() -> Session:
    return Session(engine)
