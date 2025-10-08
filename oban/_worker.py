import importlib

_worker_registry: dict[str, type] = {}


def resolve_worker(path: str) -> type:
    """Resolve a worker class by its path.

    First checks the worker registry for dynamically defined workers,
    then falls back to importing the module.
    """
    if path in _worker_registry:
        return _worker_registry[path]

    parts = path.split(".")
    mod_name, cls_name = ".".join(parts[:-1]), parts[-1]

    # TODO: Add try/except checks
    mod = importlib.import_module(mod_name)
    cls = getattr(mod, cls_name)

    return cls
