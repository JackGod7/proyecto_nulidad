"""Merger v2: NDJSON+gzip por distrito (sync/export/{SLUG}/) -> forensic.db.

Idempotente (INSERT OR REPLACE). Verifica manifest SHA-256 antes de mergear.
"""
import gzip
import hashlib
import json
import logging
import sqlite3
import sys
from pathlib import Path

logger = logging.getLogger(__name__)
ROOT = Path(__file__).parent.parent.parent
EXPORT_DIR = ROOT / "sync" / "export"
DB_FILE = ROOT / "data" / "forensic.db"

NDJSON_TO_TABLE = {
    "actas": "actas",
    "votos": "votos_por_mesa",
    "pdfs": "pdfs",
    "instalaciones": "instalaciones",
    "acta_estado_historial": "acta_estado_historial",
}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def verify_manifest(distrito_dir: Path) -> bool:
    manifest_path = distrito_dir / "manifest.json"
    if not manifest_path.exists():
        logger.error("No manifest in %s", distrito_dir.name)
        return False
    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)
    for fname, meta in manifest.get("files", {}).items():
        p = distrito_dir / fname
        if not p.exists():
            logger.warning("%s missing: %s", distrito_dir.name, fname)
            continue
        expected = meta.get("sha256")
        actual = sha256_file(p)
        if expected and expected != actual:
            logger.error("%s/%s hash mismatch", distrito_dir.name, fname)
            return False
    return True


def iter_ndjson(path: Path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def table_columns(cur: sqlite3.Cursor, table: str) -> set[str]:
    cur.execute(f"PRAGMA table_info({table})")
    return {r[1] for r in cur.fetchall()}


def merge_table(cur: sqlite3.Cursor, table: str, rows) -> int:
    valid_cols = table_columns(cur, table)
    n = 0
    for row in rows:
        if "id" in row and table in ("votos_por_mesa", "acta_estado_historial"):
            row = {k: v for k, v in row.items() if k != "id"}
        row = {k: v for k, v in row.items() if k in valid_cols}
        if not row:
            continue
        cols = ", ".join(row.keys())
        placeholders = ", ".join("?" * len(row))
        try:
            cur.execute(
                f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({placeholders})",
                list(row.values()),
            )
            n += 1
        except sqlite3.OperationalError as e:
            logger.warning("Skip %s row: %s", table, e)
        except sqlite3.IntegrityError as e:
            logger.warning("Skip %s row (integrity): %s", table, e)
    return n


def merge_distrito(conn: sqlite3.Connection, distrito_dir: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    cur = conn.cursor()
    for ndjson_name, table in NDJSON_TO_TABLE.items():
        f = distrito_dir / f"{ndjson_name}.ndjson.gz"
        if not f.exists():
            continue
        n = merge_table(cur, table, iter_ndjson(f))
        counts[table] = n
    conn.commit()
    return counts


def main(distritos_filtro: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if not EXPORT_DIR.exists():
        logger.error("sync/export/ no existe")
        return
    dirs = sorted([d for d in EXPORT_DIR.iterdir() if d.is_dir()])
    if distritos_filtro:
        slugs = {s.upper().replace(" ", "_") for s in distritos_filtro}
        dirs = [d for d in dirs if d.name in slugs]

    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    totales: dict[str, int] = {}
    for d in dirs:
        if not verify_manifest(d):
            logger.error("SKIP %s (manifest KO)", d.name)
            continue
        c = merge_distrito(conn, d)
        logger.info("%s -> %s", d.name, c)
        for k, v in c.items():
            totales[k] = totales.get(k, 0) + v
    conn.close()
    print(f"\nMerge v2 OK: {totales}")


if __name__ == "__main__":
    filtro = sys.argv[1:] if len(sys.argv) > 1 else None
    main(filtro)
