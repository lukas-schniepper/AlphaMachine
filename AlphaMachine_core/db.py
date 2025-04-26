import os
from sqlmodel import create_engine, SQLModel, Session
from AlphaMachine_core.config import DATABASE_URL

engine = create_engine(DATABASE_URL, echo=False)
SQLModel.metadata.create_all(engine)

def get_session() -> Session:
    return Session(engine)
