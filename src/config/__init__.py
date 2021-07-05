import os

ROOT_PATH = os.path.dirname(os.path.dirname(__file__))
DEFAULT_CONFIG_FILE = os.path.join(ROOT_PATH, "application.yml")
__all__ = [
    "ROOT_PATH",
    "DEFAULT_CONFIG_FILE",
]
