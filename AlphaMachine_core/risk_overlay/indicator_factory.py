import importlib

def load_indicator(path: str, class_name: str, **kwargs):
    module = importlib.import_module(path)
    cls = getattr(module, class_name)
    return cls(**kwargs)
