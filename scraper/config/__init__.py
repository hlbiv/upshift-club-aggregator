# Re-export top-level config symbols so that ``from config import X``
# works regardless of whether Python resolves ``config`` as this package
# or the sibling ``config.py`` module.  When pytest collects from the
# repo root, the package wins; this shim keeps the import contract
# consistent.

import importlib
import os
import sys

# Import the *module* ``config.py`` that lives next to this package.
# We can't do ``from .. import config`` because this IS the config
# package.  Instead, load it by path.
_config_module_path = os.path.join(os.path.dirname(__file__), "..", "config.py")
if os.path.isfile(_config_module_path):
    _spec = importlib.util.spec_from_file_location("_config_module", _config_module_path)
    if _spec and _spec.loader:
        _mod = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_mod)

        # Re-export every public name from config.py.
        for _name in dir(_mod):
            if not _name.startswith("_"):
                globals()[_name] = getattr(_mod, _name)
