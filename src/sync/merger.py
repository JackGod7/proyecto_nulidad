"""Fusiona JSON exportados de otras máquinas en la DB principal."""
import json
import sqlite3
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
ROOT = Path(__file__).parent.parent.parent
IMPORT_DIR = ROOT / "sync" / "import"
DB_FILE = ROOT / "data" / "forensic.db"


def merge_file(conn: sqlite3.Connection, path: Path) -> tuple[int, int, int]:
    """Returns (actas_merged, votos_merged, pdfs_merged)."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    cur = conn.cursor()
    a_count = v_count = p_count = 0

    # Actas — INSERT OR IGNORE (no sobreescribir datos existentes)
    for acta in data.get("actas", []):
        cols = ", ".join(acta.keys())
        placeholders = ", ".join("?" * len(acta))
        cur.execute(
            f"INSERT OR IGNORE INTO actas ({cols}) VALUES ({placeholders})",
            list(acta.values())
        )
        if cur.rowcount:
            a_count += 1

    # Votos
    for voto in data.get("votos", []):
        cols = ", ".join(voto.keys())
        placeholders = ", ".join("?" * len(voto))
        cur.execute(
            f"INSERT OR IGNORE INTO votos_por_mesa ({cols}) VALUES ({placeholders})",
            list(voto.values())
        )
        if cur.rowcount:
            v_count += 1

    # PDFs metadata
    for pdf in data.get("pdfs", []):
        cols = ", ".join(pdf.keys())
        placeholders = ", ".join("?" * len(pdf))
        cur.execute(
            f"INSERT OR IGNORE INTO pdfs ({cols}) VALUES ({placeholders})",
            list(pdf.values())
        )
        if cur.rowcount:
            p_count += 1

    conn.commit()
    return a_count, v_count, p_count


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
    total_a = total_v = total_p = 0

    for jf in json_files:
        a, v, p = merge_file(conn, jf)
        logger.info("%s → %d actas, %d votos, %d pdfs", jf.name, a, v, p)
        total_a += a
        total_v += v
        total_p += p

    conn.close()
    print(f"\n✓ Merge completo: {total_a} actas, {total_v} votos, {total_p} pdfs")
    print(f"  Procesados: {len(json_files)} archivos")


if __name__ == "__main__":
    main()
