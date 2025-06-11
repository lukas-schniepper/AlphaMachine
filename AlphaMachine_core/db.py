# AlphaMachine_core/db.py
import contextlib # Importiere contextlib
from sqlalchemy.exc import OperationalError
from sqlmodel import SQLModel, create_engine, Session # SQLModel importieren für Session und create_all

# Importiere DATABASE_URL aus deiner zentralen Konfigurationsdatei
# Dieser Import führt den Code in config.py aus und sollte DATABASE_URL verfügbar machen
from AlphaMachine_core.config import DATABASE_URL 

# Debug-Print, um zu sehen, was hier ankommt
print(f"DEBUG [db.py]: Importierte DATABASE_URL (Auszug): ...{DATABASE_URL[-20:] if DATABASE_URL else 'NICHT DEFINIERT BEIM IMPORT IN DB.PY'}")

# Stelle sicher, dass DATABASE_URL auch wirklich einen Wert hat, bevor die Engine erstellt wird
if not DATABASE_URL:
    raise RuntimeError("FEHLER in db.py: DATABASE_URL ist None oder leer, bevor die Engine erstellt wird. Überprüfe config.py und Secrets/Umgebung.")

engine = create_engine(
    DATABASE_URL,
    echo=False, # Setze auf True für SQL-Logging beim Debuggen
    # Pool-Einstellungen könnten hier relevant sein, wenn du viele gleichzeitige Anfragen erwartest
    # z.B. pool_size=5, max_overflow=10
)

@contextlib.contextmanager # Dieser Dekorator ist entscheidend
def get_session():
    """
    Stellt eine SQLModel/SQLAlchemy Session als Context Manager bereit.
    Kümmert sich um commit, rollback und close.
    Verwendung:
        with get_session() as session:
            # ... deine Datenbankoperationen mit session ...
    """
    session = Session(engine) # Erstelle eine neue Session-Instanz
    print("DEBUG [db.py]: Neue Session in get_session() erstellt.")
    try:
        yield session # Stelle die Session dem 'with'-Block zur Verfügung
        session.commit() # Wenn der Block ohne Fehler durchläuft, committe
        print("DEBUG [db.py]: Session erfolgreich committet.")
    except Exception as e:
        print(f"FEHLER in get_session() with-Block: {e}. Führe Rollback aus.")
        session.rollback() # Bei Fehlern im 'with'-Block, rolle zurück
        raise # Re-raise die Exception, damit der Aufrufer sie behandeln kann
    finally:
        print("DEBUG [db.py]: Schließe Session in get_session().")
        session.close() # Schließe die Session immer, egal was passiert


def init_db():
    """
    Legt alle Tabellen an, die von SQLModel-Klassen definiert wurden,
    die importiert wurden, bevor diese Funktion aufgerufen wird.
    Stelle sicher, dass deine Modelle (TickerPeriod, TickerInfo, PriceData)
    SQLModel-Klassen sind und importiert wurden (z.B. in models.py und
    dann models.py irgendwo importiert wird, bevor init_db() läuft).
    """
    try:
        # Damit SQLModel.metadata.create_all(engine) funktioniert, müssen die
        # SQLModel-basierten Modellklassen (TickerPeriod, TickerInfo, PriceData)
        # bereits importiert und somit bei SQLModel.metadata registriert sein.
        # Der Import von AlphaMachine_core.models in data_manager.py oder app.py
        # sollte dafür sorgen, wenn diese Modelle von SQLModel erben.

        # WICHTIG: Stelle sicher, dass deine Modelle in models.py tatsächlich von SQLModel erben
        # z.B. class PriceData(SQLModel, table=True):
        #        ...
        print("INFO [db.py]: init_db() wird ausgeführt - versuche Tabellen zu erstellen...")
        SQLModel.metadata.create_all(engine)
        print("✅ INFO [db.py]: SQLModel.metadata.create_all(engine) erfolgreich ausgeführt (oder Tabellen existierten bereits).")
    except OperationalError as e:
        print(f"⚠️ WARNUNG [db.py]: Fehler beim Ausführen von create_all (möglicherweise DB nicht erreichbar oder Berechtigungsproblem): {e}")
    except Exception as e_init:
        print(f"⚠️ UNERWARTETER FEHLER in init_db(): {e_init}")
        import traceback
        traceback.print_exc()

# Die auskommentierte get_session() war eine Alternative, aber der Context Manager ist besser für 'with'.
# def get_session() -> Session:
# return Session(engine)