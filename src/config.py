"""Configuración central del scraper ONPE."""
from pathlib import Path

# API
BASE_URL = "https://resultadoelectoral.onpe.gob.pe/presentacion-backend"
ID_ELECCION = 10  # Presidencial
ID_AMBITO = 1     # Perú
UBIGEO_LIMA_REGION = "140000"
UBIGEO_LIMA_PROVINCIA = "140100"

# Rate limiting
REQUEST_DELAY_SECS = 1.0
MAX_RETRIES = 3
PAGE_SIZE = 50

# Tipos de archivo
TIPO_ESCRUTINIO = 1
TIPO_INSTALACION = 3
TIPO_SUFRAGIO = 4

TIPO_NOMBRES = {
    TIPO_ESCRUTINIO: "ESCRUTINIO",
    TIPO_INSTALACION: "INSTALACION",
    TIPO_SUFRAGIO: "SUFRAGIO",
}

# Top 5 candidatos — mapeo apellido → key CSV
CANDIDATOS_TOP5 = {
    "FUJIMORI": "Voto_Keiko",
    "LOPEZ ALIAGA": "Voto_Rafael",
    "LÓPEZ ALIAGA": "Voto_Rafael",
    "NIETO": "Voto_Nieto",
    "BELMONT": "Voto_Belmont",
    "SANCHEZ PALOMINO": "Voto_Roberto",
    "SÁNCHEZ PALOMINO": "Voto_Roberto",
}

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DATASET_CSV = DATA_DIR / "dataset_lima_provincia.csv"
ACTAS_SIN_PDF = DATA_DIR / "actas_sin_pdf.csv"
HORAS_CSV = DATA_DIR / "horas_instalacion.csv"
CHECKPOINT_DIR = DATA_DIR / ".checkpoints"
