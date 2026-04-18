"""Extractor masivo de actas de INSTALACION via OpenAI gpt-4o-mini."""
import json
import logging
import os
import re
import time
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

from src.db.schema import get_conn, log_custodia

load_dotenv()
logger = logging.getLogger(__name__)

MODEL = "gpt-4o-mini"
DELAY_BETWEEN = 1.0
MAX_RETRIES = 3

PROMPT = (
    "Eres un auditor electoral experto en actas peruanas ONPE.\n"
    "Lee el ACTA DE INSTALACION y extrae exactamente estos campos en JSON:\n"
    '{"mesa": "numero de mesa",'
    ' "departamento": "nombre",'
    ' "provincia": "nombre",'
    ' "distrito": "nombre",'
    ' "local_votacion": "nombre del local o null",'
    ' "hora_instalacion": "HH:MM a.m./p.m. exacta del acta",'
    ' "total_electores_habiles": numero,'
    ' "material_buen_estado": true o false,'
    ' "observaciones": "texto literal de observaciones o null si no hay"}\n'
    "La hora aparece como: 'siendo las HH:MM horas' o 'siendo las HH:MM a.m./p.m.'.\n"
    "material_buen_estado = true si el material electoral se recibio en buen estado.\n"
    "Solo JSON, nada mas."
)


def _parse(text: str) -> dict:
    clean = text.strip().removeprefix("```json").removesuffix("```").strip()
    return json.loads(clean)


def _hora_a_minutos(hora_str: str | None) -> int | None:
    """'08:12 a.m.' -> 492 minutos desde medianoche."""
    if not hora_str:
        return None
    m = re.search(r"(\d{1,2}):(\d{2})\s*(a\.?\s*m\.?|p\.?\s*m\.?)?", hora_str, re.IGNORECASE)
    if not m:
        return None
    h, mi = int(m.group(1)), int(m.group(2))
    periodo = re.sub(r"[\s.]", "", (m.group(3) or "")).lower()
    if periodo == "pm" and h != 12:
        h += 12
    elif periodo == "am" and h == 12:
        h = 0
    return h * 60 + mi


def extraer_instalacion(pdf_path: Path, client: OpenAI) -> dict:
    """Sube PDF a OpenAI Files API y extrae campos."""
    for intento in range(MAX_RETRIES):
        try:
            with open(pdf_path, "rb") as f:
                ofile = client.files.create(file=f, purpose="assistants")
            resp = client.responses.create(
                model=MODEL,
                input=[{"role": "user", "content": [
                    {"type": "input_file", "file_id": ofile.id},
                    {"type": "input_text", "text": PROMPT},
                ]}],
            )
            # Limpiar archivo remoto
            try:
                client.files.delete(ofile.id)
            except Exception:
                pass
            return _parse(resp.output_text)
        except Exception as e:
            if intento < MAX_RETRIES - 1:
                wait = 3 * (intento + 1)
                logger.warning("Error intento %d/%d (%s), esperando %ds...", intento + 1, MAX_RETRIES, str(e)[:60], wait)
                time.sleep(wait)
            else:
                raise


def _ensure_tabla_instalacion(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS instalaciones (
            mesa TEXT PRIMARY KEY,
            acta_id INTEGER,
            archivo_id TEXT UNIQUE,
            distrito TEXT NOT NULL,
            local_votacion TEXT,
            hora_instalacion_raw TEXT,
            hora_instalacion_min INTEGER,
            total_electores_habiles INTEGER,
            material_buen_estado INTEGER,
            observaciones TEXT,
            extraido_at TEXT,
            error TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_inst_distrito ON instalaciones(distrito)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_inst_hora ON instalaciones(hora_instalacion_min)")
    conn.commit()


def procesar_instalaciones(
    distrito: str | None = None,
    limit: int | None = None,
    delay: float = DELAY_BETWEEN,
) -> None:
    """Procesa PDFs de instalacion pendientes."""
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
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
    logger.info("Pendientes: %d", total)

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
            data = extraer_instalacion(pdf_path, client)
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
                    observaciones, extraido_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,datetime('now'))""",
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
                ),
            )
            conn.commit()

            log_custodia(
                conn, "OPENAI_INSTALACION",
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

    logger.info("Completado: %d ok, %d errores de %d", ok, errores, total)
    conn.close()


def resumen() -> None:
    conn = get_conn()
    _ensure_tabla_instalacion(conn)
    rows = conn.execute("""
        SELECT distrito, COUNT(*) as total,
               SUM(CASE WHEN error IS NULL AND hora_instalacion_raw IS NOT NULL THEN 1 ELSE 0 END) as ok,
               SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END) as err
        FROM instalaciones GROUP BY distrito ORDER BY distrito
    """).fetchall()
    pdfs = {r["distrito"]: r["total"] for r in conn.execute("""
        SELECT distrito, COUNT(*) as total FROM pdfs
        WHERE tipo=3 AND descargado=1 AND archivo_en_disco=1 GROUP BY distrito
    """).fetchall()}
    print(f"\n{'DISTRITO':<35} {'PDFs':>6} {'OK':>6} {'ERROR':>6}")
    print("-" * 58)
    for r in rows:
        print(f"{r['distrito']:<35} {pdfs.get(r['distrito'], '?'):>6} {r['ok']:>6} {r['err']:>6}")
    conn.close()


def exportar_csv(output: str = "data/instalaciones_miraflores.csv") -> None:
    import csv
    conn = get_conn()
    rows = conn.execute("""
        SELECT mesa, distrito, local_votacion,
               hora_instalacion_raw, hora_instalacion_min,
               total_electores_habiles, material_buen_estado, observaciones
        FROM instalaciones
        WHERE error IS NULL AND hora_instalacion_raw IS NOT NULL
        ORDER BY distrito, mesa
    """).fetchall()
    with open(output, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["MESA", "DISTRITO", "LOCAL_VOTACION",
                         "HORA_INSTALACION", "HORA_INSTALACION_MIN",
                         "TOTAL_ELECTORES_HABILES", "MATERIAL_BUEN_ESTADO", "OBSERVACIONES"])
        for r in rows:
            writer.writerow([r["mesa"], r["distrito"], r["local_votacion"],
                             r["hora_instalacion_raw"], r["hora_instalacion_min"],
                             r["total_electores_habiles"],
                             "SI" if r["material_buen_estado"] else "NO",
                             r["observaciones"]])
    print(f"CSV: {output} ({len(rows)} filas)")
    conn.close()


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("data/instalaciones.log")],
    )
    distrito_arg = sys.argv[1] if len(sys.argv) > 1 else None
    limit_arg = int(sys.argv[2]) if len(sys.argv) > 2 else None
    resumen()
    procesar_instalaciones(distrito=distrito_arg, limit=limit_arg)
    resumen()
    if not limit_arg:
        exportar_csv()
