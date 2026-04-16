"""Extractor masivo de actas PDF via Gemini 2.5 Flash."""
import json
import logging
import os
import time
from pathlib import Path

from dotenv import load_dotenv
from google import genai
from google.genai import types

from src.db.schema import get_conn, log_custodia

load_dotenv()
logger = logging.getLogger(__name__)

PROMPT = (
    "Eres un auditor electoral experto en actas peruanas ONPE.\n\n"
    "ESTRUCTURA DEL ACTA DE ESCRUTINIO:\n"
    "- La tabla tiene UNA SOLA columna de datos numéricos a la DERECHA: TOTAL DE VOTOS\n"
    "- A la IZQUIERDA de cada partido hay un número pequeño que es su POSICIÓN EN LA CÉDULA DE VOTACIÓN (sorteo ONPE). "
    "Este número NO es un voto. IGNÓRALO COMPLETAMENTE.\n"
    "- Los votos están escritos A MANO en la columna derecha dentro de recuadros.\n"
    "- Ejemplo: si ves '32 PARTIDO APRISTA PERUANO .... 2', el 32 es la posición y 2 son los votos.\n"
    "- Ejemplo: si ves '33 RENOVACIÓN POPULAR .... 70', el 33 es la posición y 70 son los votos.\n\n"
    "REGLA CRÍTICA: El número que aparece ANTES del nombre del partido NUNCA es un voto. "
    "SIEMPRE es la posición en la cédula. Los votos están en la columna de la DERECHA, dentro de casillas/recuadros.\n\n"
    "Extrae SOLO los votos (columna derecha) en este JSON:\n"
    '{"mesa": "numero", "distrito": "nombre", "total_electores_habiles": numero, '
    '"hora_inicio_escrutinio": "HH:MM p.m.", "hora_fin_escrutinio": "HH:MM p.m.", '
    '"votos": {"NOMBRE_PARTIDO": votos_int}, '
    '"total_ciudadanos_votaron": numero, "votos_blanco": numero, "votos_nulos": numero, '
    '"votos_impugnados": numero}\n'
    "Solo JSON, nada más."
)

MODEL = "gemini-2.5-flash"
DELAY_BETWEEN = 1.5  # segundos entre requests


def _parse_response(text: str) -> dict:
    clean = text.strip().removeprefix("```json").removesuffix("```").strip()
    return json.loads(clean)


def extraer_acta_pdf(pdf_path: Path, client: genai.Client) -> dict:
    """Extrae datos de un acta PDF. Retorna dict con datos extraídos."""
    pdf_bytes = pdf_path.read_bytes()
    resp = client.models.generate_content(
        model=MODEL,
        contents=[
            types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
            PROMPT,
        ],
    )
    return _parse_response(resp.text)


def procesar_pendientes(
    distrito: str | None = None,
    limit: int | None = None,
    delay: float = DELAY_BETWEEN,
) -> None:
    """Procesa PDFs de escrutinio pendientes de extracción Gemini."""
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    conn = get_conn()

    query = """
        SELECT p.archivo_id, p.acta_id, p.mesa, p.distrito, p.nombre_destino
        FROM pdfs p
        WHERE p.tipo = 1
          AND p.descargado = 1
          AND p.archivo_en_disco = 1
          AND p.gemini_extraido = 0
    """
    params: list = []
    if distrito:
        query += " AND p.distrito = ?"
        params.append(distrito.upper())
    if limit:
        query += f" LIMIT {limit}"

    rows = conn.execute(query, params).fetchall()
    total = len(rows)
    logger.info("PDFs pendientes Gemini: %d", total)

    ok = 0
    errores = 0
    for i, row in enumerate(rows, 1):
        pdf_path = Path(row["nombre_destino"])
        if not pdf_path.exists():
            logger.warning("No existe: %s", pdf_path)
            conn.execute(
                "UPDATE pdfs SET archivo_en_disco=0 WHERE archivo_id=?",
                (row["archivo_id"],),
            )
            conn.commit()
            continue

        try:
            data = extraer_acta_pdf(pdf_path, client)
            votos_json = json.dumps(data.get("votos", {}), ensure_ascii=False)

            conn.execute(
                """UPDATE pdfs SET
                    gemini_extraido=1,
                    gemini_votos_json=?,
                    gemini_hora_inicio=?,
                    gemini_hora_fin=?,
                    gemini_total_votaron=?,
                    gemini_raw_response=?,
                    gemini_extraido_at=datetime('now')
                WHERE archivo_id=?""",
                (
                    votos_json,
                    data.get("hora_inicio_escrutinio"),
                    data.get("hora_fin_escrutinio"),
                    data.get("total_ciudadanos_votaron"),
                    json.dumps(data, ensure_ascii=False),
                    row["archivo_id"],
                ),
            )
            conn.commit()

            log_custodia(
                conn,
                "GEMINI_EXTRACCION",
                entidad_tipo="pdf",
                entidad_id=row["archivo_id"],
                detalle={"mesa": row["mesa"], "distrito": row["distrito"], "partidos": len(data.get("votos", {}))},
            )

            ok += 1
            logger.info("[%d/%d] %s %s — %d partidos", i, total, row["distrito"], row["mesa"], len(data.get("votos", {})))

        except Exception as e:
            errores += 1
            logger.error("[%d/%d] ERROR %s: %s", i, total, row["mesa"], e)
            conn.execute(
                "UPDATE pdfs SET error=? WHERE archivo_id=?",
                (str(e)[:500], row["archivo_id"]),
            )
            conn.commit()

        if i < total:
            time.sleep(delay)

    logger.info("Completado: %d ok, %d errores de %d", ok, errores, total)
    conn.close()


def resumen_extraccion() -> None:
    """Imprime resumen del estado de extracción Gemini por distrito."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT distrito,
               COUNT(*) as total_pdfs,
               SUM(gemini_extraido) as extraidos,
               SUM(CASE WHEN descargado=1 AND archivo_en_disco=1 AND gemini_extraido=0 THEN 1 ELSE 0 END) as pendientes
        FROM pdfs
        WHERE tipo = 1
        GROUP BY distrito
        ORDER BY distrito
    """).fetchall()
    print(f"\n{'DISTRITO':<35} {'TOTAL':>7} {'EXTRAÍDO':>9} {'PENDIENTE':>10}")
    print("-" * 65)
    for r in rows:
        print(f"{r['distrito']:<35} {r['total_pdfs']:>7} {r['extraidos']:>9} {r['pendientes']:>10}")
    conn.close()


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    distrito_arg = sys.argv[1] if len(sys.argv) > 1 else None
    limit_arg = int(sys.argv[2]) if len(sys.argv) > 2 else None
    resumen_extraccion()
    procesar_pendientes(distrito=distrito_arg, limit=limit_arg)
    resumen_extraccion()
