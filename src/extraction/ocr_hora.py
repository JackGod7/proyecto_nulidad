"""OCR para extraer hora de instalación de actas tipo 3."""
import csv
import logging
import re
from pathlib import Path

from src.config import DATA_DIR, HORAS_CSV

logger = logging.getLogger(__name__)

# Patrón hora: HH:MM (24h o 12h con am/pm)
HORA_PATTERN = re.compile(
    r"(?:HORA\s*(?:DE\s*)?INSTALACI[OÓ]N\s*:?\s*)"
    r"(\d{1,2})\s*[:\.]\s*(\d{2})\s*(a\.?\s*m\.?|p\.?\s*m\.?)?",
    re.IGNORECASE,
)
HORA_SIMPLE = re.compile(r"(\d{1,2})\s*[:\.]\s*(\d{2})\s*(a\.?\s*m\.?|p\.?\s*m\.?)?")


def extraer_hora_de_texto(texto: str) -> tuple[str | None, float]:
    """Busca hora de instalación en texto OCR. Retorna (hora, confianza)."""
    # Buscar con contexto "HORA INSTALACION"
    match = HORA_PATTERN.search(texto)
    if match:
        h, m, ampm = match.group(1), match.group(2), match.group(3)
        hora = f"{h.zfill(2)}:{m}"
        if ampm:
            hora += f" {ampm.strip()}"
        return hora, 0.9

    # Fallback: cualquier hora en el texto
    match = HORA_SIMPLE.search(texto)
    if match:
        h, m, ampm = match.group(1), match.group(2), match.group(3)
        hora = f"{h.zfill(2)}:{m}"
        if ampm:
            hora += f" {ampm.strip()}"
        return hora, 0.5

    return None, 0.0


def procesar_pdfs_instalacion() -> None:
    """Procesa todos los PDFs de instalación y extrae horas."""
    try:
        from paddleocr import PaddleOCR
        from pdf2image import convert_from_path
    except ImportError:
        logger.error("Instalar dependencias OCR: uv pip install 'proyecto-nulidad[ocr]'")
        return

    ocr = PaddleOCR(use_angle_cls=True, lang="es", show_log=False)

    resultados = []
    pdfs = sorted(DATA_DIR.rglob("*_INSTALACION.pdf"))
    logger.info("PDFs de instalación encontrados: %d", len(pdfs))

    for pdf_path in pdfs:
        distrito = pdf_path.parent.name
        mesa = pdf_path.stem.replace("_INSTALACION", "")

        try:
            imagenes = convert_from_path(str(pdf_path), first_page=1, last_page=1, dpi=200)
            if not imagenes:
                continue

            result = ocr.ocr(imagenes[0], cls=True)
            texto = " ".join(
                line[1][0] for block in (result or []) for line in (block or [])
            )

            hora, confianza = extraer_hora_de_texto(texto)
            resultados.append({
                "DISTRITO": distrito,
                "MESA": mesa,
                "HORA_INSTALACION": hora or "",
                "CONFIANZA_OCR": confianza,
                "ARCHIVO_PDF": str(pdf_path.relative_to(DATA_DIR)),
            })

        except Exception as e:
            logger.error("Error OCR %s: %s", pdf_path.name, e)
            resultados.append({
                "DISTRITO": distrito,
                "MESA": mesa,
                "HORA_INSTALACION": "",
                "CONFIANZA_OCR": 0.0,
                "ARCHIVO_PDF": str(pdf_path.relative_to(DATA_DIR)),
            })

    # Guardar CSV
    HORAS_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(HORAS_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["DISTRITO", "MESA", "HORA_INSTALACION", "CONFIANZA_OCR", "ARCHIVO_PDF"])
        writer.writeheader()
        writer.writerows(resultados)

    logger.info("Horas extraídas: %d/%d", sum(1 for r in resultados if r["HORA_INSTALACION"]), len(resultados))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    procesar_pdfs_instalacion()
