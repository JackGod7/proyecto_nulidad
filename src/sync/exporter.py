"""Exporta datos locales a JSON para sincronización entre máquinas."""
import json
import sqlite3
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)
ROOT = Path(__file__).parent.parent.parent
EXPORT_DIR = ROOT / "sync" / "export"
DB_FILE = ROOT / "data" / "forensic.db"


def export_distrito(conn: sqlite3.Connection, distrito: str, machine_id: str) -> Path:
    """Exporta todas las actas y datos de un distrito a JSON."""
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    cur = conn.cursor()

    # Actas
    cur.execute("SELECT * FROM actas WHERE distrito=?", (distrito,))
    cols = [d[0] for d in cur.description]
    actas = [dict(zip(cols, row)) for row in cur.fetchall()]

    # Votos
    acta_ids = [a["acta_id"] for a in actas]
    votos: list[dict] = []
    if acta_ids:
        placeholders = ",".join("?" * len(acta_ids))
        cur.execute(f"SELECT * FROM votos_por_mesa WHERE acta_id IN ({placeholders})", acta_ids)
        cols_v = [d[0] for d in cur.description]
        votos = [dict(zip(cols_v, row)) for row in cur.fetchall()]

    # PDFs metadata (sin blob)
    pdfs: list[dict] = []
    if acta_ids:
        cur.execute(
            f"SELECT archivo_id, acta_id, mesa, tipo, sha256_hash, archivo_en_disco, "
            f"gemini_raw_response, descarga_at, error "
            f"FROM pdfs WHERE acta_id IN ({placeholders})", acta_ids
        )
        cols_p = [d[0] for d in cur.description]
        pdfs = [dict(zip(cols_p, row)) for row in cur.fetchall()]

    # Instalaciones
    cur.execute(
        "SELECT mesa, hora_instalacion_raw, hora_instalacion_min, total_electores_habiles, "
        "material_buen_estado, observaciones, extraido_at, error "
        "FROM instalaciones WHERE distrito=?", (distrito,)
    )
    cols_i = [d[0] for d in cur.description]
    instalaciones = [dict(zip(cols_i, row)) for row in cur.fetchall()]

    payload = {
        "machine_id": machine_id,
        "distrito": distrito,
        "exported_at": datetime.utcnow().isoformat(),
        "actas": actas,
        "votos": votos,
        "pdfs": pdfs,
        "instalaciones": instalaciones,
    }

    safe_name = distrito.replace(" ", "_").upper()
    out_file = EXPORT_DIR / f"{machine_id}_{safe_name}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, default=str)

    logger.info("Exported %s → %s (%d actas)", distrito, out_file.name, len(actas))
    return out_file


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    config_file = ROOT / "machine_config.json"
    if not config_file.exists():
        logger.error("machine_config.json not found")
        return

    with open(config_file, encoding="utf-8") as f:
        config = json.load(f)

    machine_id = config["machine_id"]
    distritos = config["distritos"]

    if not DB_FILE.exists():
        logger.error("forensic.db not found")
        return

    conn = sqlite3.connect(DB_FILE)
    exported = []
    for distrito in distritos:
        try:
            path = export_distrito(conn, distrito, machine_id)
            exported.append(str(path))
        except Exception as e:
            logger.error("Error exporting %s: %s", distrito, e)
    conn.close()

    print(f"\nExportados {len(exported)} distritos -> sync/export/")
    print("Copia esos archivos JSON al directorio sync/import/ de la máquina principal.")


if __name__ == "__main__":
    main()
