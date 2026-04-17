"""Fusiona JSON exportados de otras máquinas en la DB principal."""
import json
import sqlite3
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
ROOT = Path(__file__).parent.parent.parent
IMPORT_DIR = ROOT / "sync" / "import"
DB_FILE = ROOT / "data" / "forensic.db"


def merge_file(conn: sqlite3.Connection, path: Path) -> dict[str, int]:
    """Returns counts per table merged."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    cur = conn.cursor()
    counts: dict[str, int] = {"actas": 0, "votos": 0, "pdfs": 0, "instalaciones": 0}

    tables = [
        ("actas", "actas"),
        ("votos", "votos_por_mesa"),
        ("pdfs", "pdfs"),
        ("instalaciones", "instalaciones"),
    ]

    for json_key, table_name in tables:
        for row in data.get(json_key, []):
            cols = ", ".join(row.keys())
            placeholders = ", ".join("?" * len(row))
            try:
                cur.execute(
                    f"INSERT OR IGNORE INTO {table_name} ({cols}) VALUES ({placeholders})",
                    list(row.values())
                )
                if cur.rowcount:
                    counts[json_key] += 1
            except sqlite3.OperationalError as e:
                logger.warning("Skip %s row in %s: %s", table_name, path.name, e)

    conn.commit()
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if not IMPORT_DIR.exists():
        logger.error("sync/import/ no existe. Copia los JSON exportados ahí.")
        return

    json_files = list(IMPORT_DIR.glob("*.json"))
    if not json_files:
        logger.error("No hay archivos JSON en sync/import/")
        return

    conn = sqlite3.connect(DB_FILE)
    totals: dict[str, int] = {"actas": 0, "votos": 0, "pdfs": 0, "instalaciones": 0}

    for jf in sorted(json_files):
        c = merge_file(conn, jf)
        logger.info("%s -> actas=%d votos=%d pdfs=%d inst=%d",
                     jf.name, c["actas"], c["votos"], c["pdfs"], c["instalaciones"])
        for k in totals:
            totals[k] += c[k]

    conn.close()
    print(f"\nMerge OK: {totals['actas']} actas, {totals['votos']} votos, "
          f"{totals['pdfs']} pdfs, {totals['instalaciones']} instalaciones")
    print(f"Procesados: {len(json_files)} archivos")


if __name__ == "__main__":
    main()
