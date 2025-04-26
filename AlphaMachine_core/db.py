# AlphaMachine_core/db.py

from sqlmodel import create_engine, SQLModel, Session
from AlphaMachine_core.config import DATABASE_URL
from sqlalchemy.exc import OperationalError

engine = create_engine(
    DATABASE_URL,
    echo=False,
    # IPv6-​Numerik hier hart eintragen:
    connect_args={"hostaddr": "2a05:d014:1c06:5f0a:43d1:6413:1c16:1782"}
)

def init_db():
    try:
        SQLModel.metadata.create_all(engine)
    except OperationalError as e:
        print(f"⚠️ Could not init DB tables: {e}")

def get_session() -> Session:
    return Session(engine)
