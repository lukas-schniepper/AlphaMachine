# minimal_dm_test.py
import os
import sys

# --- Pfad-Setup ---
current_script_path = os.path.dirname(os.path.abspath(__file__))
project_root = current_script_path
if project_root not in sys.path:
    sys.path.insert(0, project_root)
print(f"Python sys.path wurde um {project_root} erweitert.")

# --- Lade .env ---
try:
    from dotenv import load_dotenv
    dotenv_path = os.path.join(project_root, '.env')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path=dotenv_path, override=True)
        print(f".env-Datei von {dotenv_path} geladen.")
        # print(f"DATABASE_URL aus Umgebung: {os.getenv('DATABASE_URL')}") # Zum Debuggen
    else:
        print(f"Keine .env-Datei unter {dotenv_path} gefunden.")
except ImportError:
    print("python-dotenv nicht installiert.")
except Exception as e:
    print(f"Fehler beim Laden von .env: {e}")


# --- Test-Import ---
print("\nVersuche, StockDataManager zu importieren...")
try:
    from AlphaMachine_core.data_manager import StockDataManager
    print("StockDataManager ERFOLGREICH importiert.")
    
    print("\nVersuche, eine Instanz von StockDataManager zu erstellen...")
    sdm = StockDataManager()
    print("StockDataManager-Instanz ERFOLGREICH erstellt.")
    print("Minimaltest bestanden!")

except ImportError as e_imp:
    print(f"\n--- ImportError ---")
    print(f"FEHLER: {e_imp}")
    print("Traceback:")
    import traceback
    traceback.print_exc()
except NameError as e_name:
    print(f"\n--- NameError ---")
    print(f"FEHLER: {e_name}")
    print("Traceback:")
    import traceback
    traceback.print_exc()
except RuntimeError as e_rt:
    print(f"\n--- RuntimeError (wahrscheinlich DATABASE_URL) ---")
    print(f"FEHLER: {e_rt}")
    print("Traceback:")
    import traceback
    traceback.print_exc()
except Exception as e_other:
    print(f"\n--- Anderer Fehler ---")
    print(f"FEHLER: {e_other}")
    print("Traceback:")
    import traceback
    traceback.print_exc()