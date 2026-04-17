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
    """Exporta todas las actas y datos de un distrito a JSON (schema v2)."""
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    cur = conn.cursor()

    cur.execute("SELECT * FROM actas WHERE distrito=?", (distrito,))
    cols = [d[0] for d in cur.description]
    actas = [dict(zip(cols, row)) for row in cur.fetchall()]
    acta_ids = [a["acta_id"] for a in actas]

    votos: list[dict] = []
    pdfs: list[dict] = []
    instalaciones: list[dict] = []
    if acta_ids:
        placeholders = ",".join("?" * len(acta_ids))

        try:
            cur.execute(f"SELECT * FROM votos_por_mesa WHERE acta_id IN ({placeholders})", acta_ids)
            cv = [d[0] for d in cur.description]
            votos = [dict(zip(cv, row)) for row in cur.fetchall()]
        except sqlite3.OperationalError:
            pass

        cur.execute(f"SELECT * FROM pdfs WHERE acta_id IN ({placeholders})", acta_ids)
        cp = [d[0] for d in cur.description]
        pdfs = [dict(zip(cp, row)) for row in cur.fetchall()]

    try:
        cur.execute("SELECT * FROM instalaciones WHERE distrito=?", (distrito,))
        ci = [d[0] for d in cur.description]
        instalaciones = [dict(zip(ci, row)) for row in cur.fetchall()]
    except sqlite3.OperationalError:
        pass

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

    logger.info("Exported %s -> %s (%d actas, %d pdfs, %d inst)",
                distrito, out_file.name, len(actas), len(pdfs), len(instalaciones))
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

    if not DB_FILE.exists():
        logger.error("forensic.db not found")
        return

    conn = sqlite3.connect(DB_FILE)

    cur = conn.cursor()
    cur.execute("SELECT DISTINCT distrito FROM actas")
    distritos_db = [row[0] for row in cur.fetchall()]

    exported = []
    for distrito in distritos_db:
        try:
            path = export_distrito(conn, distrito, machine_id)
            exported.append(str(path))
        except Exception as e:
            logger.error("Error exporting %s: %s", distrito, e)
    conn.close()

    print(f"\nOK Exportados {len(exported)} distritos -> sync/export/")


if __name__ == "__main__":
    main()
