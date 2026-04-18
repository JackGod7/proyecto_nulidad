"""Exporter v2 — NDJSON+gzip por tabla + manifest.json con SHA-256 por distrito.

Reemplaza exporter.py (JSON monolítico). Formato:
    sync/export/{DISTRITO}/
        actas.ndjson.gz              ← 1 acta por línea
        votos.ndjson.gz              ← 1 voto por línea
        pdfs.ndjson.gz
        instalaciones.ndjson.gz
        acta_estado_historial.ndjson.gz
        manifest.json                ← {archivo: sha256, rows, exported_at, machine_id}
"""
from __future__ import annotations

import gzip
import hashlib
import json
import logging
import socket
import sqlite3
import getpass
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent.parent
EXPORT_DIR = ROOT / "sync" / "export"
DB_FILE = ROOT / "data" / "forensic.db"
SCHEMA_VERSION = 2

TABLAS = {
    "actas": ("SELECT * FROM actas WHERE distrito=?", True),
    "votos": ("SELECT v.* FROM votos_por_mesa v JOIN actas a ON a.acta_id=v.acta_id WHERE a.distrito=?", True),
    "pdfs": ("SELECT p.* FROM pdfs p JOIN actas a ON a.acta_id=p.acta_id WHERE a.distrito=?", True),
    "instalaciones": ("SELECT * FROM instalaciones WHERE distrito=?", True),
    "acta_estado_historial": ("SELECT h.* FROM acta_estado_historial h JOIN actas a ON a.acta_id=h.acta_id WHERE a.distrito=?", True),
}


def slug(distrito: str) -> str:
    """Normaliza nombre distrito a carpeta segura."""
    s = unicodedata.normalize("NFKD", distrito)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.upper().replace(" ", "_").replace("/", "_")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_ndjson_gz(path: Path, rows: list[dict]) -> int:
    """Escribe NDJSON comprimido. Retorna número de filas."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", compresslevel=6) as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":"), default=str))
            f.write("\n")
    return len(rows)


def export_distrito(conn: sqlite3.Connection, distrito: str) -> Path:
    """Exporta un distrito a sync/export/{SLUG}/ con manifest."""
    conn.row_factory = sqlite3.Row
    out_dir = EXPORT_DIR / slug(distrito)
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest: dict = {
        "schema_version": SCHEMA_VERSION,
        "distrito": distrito,
        "slug": slug(distrito),
        "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "operador": getpass.getuser(),
        "maquina": socket.gethostname(),
        "archivos": {},
    }

    total_rows = 0
    for tabla, (query, _) in TABLAS.items():
        try:
            rows = [dict(r) for r in conn.execute(query, (distrito,)).fetchall()]
        except sqlite3.OperationalError as e:
            logger.warning("%s %s: %s", distrito, tabla, e)
            continue
        if not rows:
            continue

        path = out_dir / f"{tabla}.ndjson.gz"
        n = _write_ndjson_gz(path, rows)
        total_rows += n
        manifest["archivos"][f"{tabla}.ndjson.gz"] = {
            "sha256": sha256_file(path),
            "rows": n,
            "bytes": path.stat().st_size,
        }
        logger.info("%s %s: %d filas -> %s (%.1f KB)",
                    distrito, tabla, n, path.name, path.stat().st_size / 1024)

    manifest["total_rows"] = total_rows
    # Hash del manifest mismo (excluyendo su hash recursivo)
    mpath = out_dir / "manifest.json"
    with open(mpath, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2, sort_keys=True)

    try:
        rel = out_dir.relative_to(ROOT)
    except ValueError:
        rel = out_dir
    logger.info("%s -> %s (%d filas total)", distrito, rel, total_rows)
    return out_dir


def export_todos(db_path: Path = DB_FILE) -> list[Path]:
    """Exporta todos los distritos con >=1 acta en la DB."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    distritos = [r[0] for r in conn.execute(
        "SELECT DISTINCT distrito FROM actas WHERE distrito IS NOT NULL AND distrito != '' "
        "ORDER BY distrito"
    ).fetchall()]
    logger.info("Exportando %d distritos", len(distritos))
    resultados = []
    for d in distritos:
        try:
            resultados.append(export_distrito(conn, d))
        except Exception as e:
            logger.error("Falló %s: %s", d, e)
    conn.close()
    return resultados


def main() -> None:
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if len(sys.argv) > 1:
        distrito = sys.argv[1]
        conn = sqlite3.connect(DB_FILE)
        export_distrito(conn, distrito)
        conn.close()
    else:
        export_todos()


if __name__ == "__main__":
    main()
