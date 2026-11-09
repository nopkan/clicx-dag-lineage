"""Application resources and writable project data live in separate directories."""
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_ROOT.parent
CONFIG_DIR = PACKAGE_ROOT / 'config'
STATIC_DIR = PACKAGE_ROOT / 'static'
DEFAULT_DATA_DIR = PROJECT_ROOT / 'data'
