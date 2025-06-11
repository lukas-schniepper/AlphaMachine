# AlphaMachine_core/risk_overlay/indicator_factory.py
import importlib

def load_indicator(config: dict):
    """
    Lädt und instanziiert eine Indikatorklasse dynamisch.
    'config' ist ein Eintrag aus dem "indicators"-Array der overlay_config.json.
    Es muss 'path', 'class' und optional 'params' enthalten.
    """
    module_path = config.get('path')
    class_name = config.get('class')
    params = config.get('params', {})

    if not module_path or not class_name:
        raise ValueError("Indicator config must contain 'path' and 'class'. Config: {config}")

    try:
        module = importlib.import_module(module_path)
        IndicatorClass = getattr(module, class_name)
        instance = IndicatorClass(params=params) # Stelle sicher, dass die __init__ deiner Indikatorklassen 'params' akzeptiert
        return instance
    except ImportError:
        raise ImportError(f"Could not import module {module_path} for indicator.")
    except AttributeError:
        raise AttributeError(f"Could not find class {class_name} in module {module_path}.")
    except Exception as e:
        raise Exception(f"Error loading indicator {class_name} from {module_path}: {e}")