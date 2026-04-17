"""Extractor de actas INSTALACION via Gemini 2.5 Flash (fallback cuando OpenAI sin cuota)."""
import json
import logging
import os
import time
from pathlib import Path

from dotenv import load_dotenv
from google import genai
from google.genai import types

from src.db.schema import get_conn, log_custodia
from src.extraction.instalacion_extractor import (
    PROMPT,
    _ensure_tabla_instalacion,
    _hora_a_minutos,
    _parse,
)

load_dotenv()
logger = logging.getLogger(__name__)

MODEL = "gemini-2.5-flash"
DELAY_BETWEEN = 1.5
MAX_RETRIES = 3


def extraer_instalacion_gemini(pdf_path: Path, client: genai.Client) -> dict:
    pdf_bytes = pdf_path.read_bytes()
    for intento in range(MAX_RETRIES):
        try:
            resp = client.models.generate_content(
                model=MODEL,
                contents=[
                    types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
                    PROMPT,
                ],
            )
            return _parse(resp.text)
        except Exception as e:
            if intento < MAX_RETRIES - 1:
                wait = 3 * (intento + 1)
                logger.warning("Error intento %d/%d (%s), esperando %ds...", intento + 1, MAX_RETRIES, str(e)[:80], wait)
                time.sleep(wait)
            else:
                raise


def procesar_instalaciones_gemini(
    distrito: str | None = None,
    limit: int | None = None,
    delay: float = DELAY_BETWEEN,
) -> None:
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    conn = get_conn()
    _ensure_tabla_instalacion(conn)

    query = """
        SELECT p.archivo_id, p.acta_id, p.mesa, p.distrito, p.nombre_destino
        FROM pdfs p
        WHERE p.tipo = 3
          AND p.descargado = 1
          AND p.archivo_en_disco = 1
          AND p.mesa NOT IN (
              SELECT mesa FROM instalaciones
              WHERE error IS NULL AND hora_instalacion_raw IS NOT NULL
          )
    """
    params: list = []
    if distrito:
        query += " AND p.distrito = ?"
        params.append(distrito.upper())
    if limit:
        query += f" LIMIT {limit}"

    rows = conn.execute(query, params).fetchall()
    total = len(rows)
    logger.info("Pendientes Gemini: %d", total)

    ok = 0
    errores = 0
    for i, row in enumerate(rows, 1):
        raw_path = Path(row["nombre_destino"])
        if not raw_path.is_absolute():
            from src.config import PDFS_DIR
            pdf_path = PDFS_DIR / row["distrito"].upper() / raw_path.name
        else:
            pdf_path = raw_path

        if not pdf_path.exists():
            logger.warning("No existe: %s", pdf_path)
            continue

        try:
            data = extraer_instalacion_gemini(pdf_path, client)
            hora_raw = data.get("hora_instalacion")
            hora_min = _hora_a_minutos(hora_raw)
            obs = data.get("observaciones")
            if obs and obs.lower() in ("no hay observaciones.", "no hay observaciones", "ninguna", "none", "null"):
                obs = None

            conn.execute(
                """INSERT OR REPLACE INTO instalaciones
                   (mesa, acta_id, archivo_id, distrito,
                    local_votacion, hora_instalacion_raw, hora_instalacion_min,
                    total_electores_habiles, material_buen_estado,
                    observaciones, gemini_raw, extraido_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,datetime('now'))""",
                (
                    data.get("mesa") or row["mesa"],
                    row["acta_id"],
                    row["archivo_id"],
                    data.get("distrito") or row["distrito"],
                    data.get("local_votacion"),
                    hora_raw,
                    hora_min,
                    data.get("total_electores_habiles"),
                    1 if data.get("material_buen_estado") else 0,
                    obs,
                    json.dumps(data, ensure_ascii=False),
                ),
            )
            conn.commit()

            log_custodia(
                conn, "GEMINI_INSTALACION",
                entidad_tipo="pdf", entidad_id=row["archivo_id"],
                detalle={"mesa": row["mesa"], "hora": hora_raw, "hora_min": hora_min},
            )

            ok += 1
            logger.info("[%d/%d] %s %s hora=%s (%s min) electores=%s",
                i, total, row["distrito"], row["mesa"],
                hora_raw, hora_min, data.get("total_electores_habiles"))

        except Exception as e:
            errores += 1
            logger.error("[%d/%d] ERROR %s: %s", i, total, row["mesa"], str(e)[:200])
            conn.execute(
                """INSERT OR REPLACE INTO instalaciones (mesa, acta_id, archivo_id, distrito, error)
                   VALUES (?,?,?,?,?)""",
                (row["mesa"], row["acta_id"], row["archivo_id"], row["distrito"], str(e)[:500]),
            )
            conn.commit()

        if i < total:
            time.sleep(delay)

    logger.info("Completado Gemini: %d ok, %d errores de %d", ok, errores, total)
    conn.close()


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    distrito_arg = sys.argv[1] if len(sys.argv) > 1 else None
    procesar_instalaciones_gemini(distrito=distrito_arg)
