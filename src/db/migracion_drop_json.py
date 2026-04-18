"""Migracion: normaliza JSON blobs a tablas.

Accion:
  1. Crea tabla `acta_estado_historial` (normaliza api_response_raw.lineaTiempo)
  2. Puebla la nueva tabla
  3. Dropea columnas JSON obsoletas:
     - actas.api_response_raw     (163 MB - redundante, todos los campos ya estan)
     - actas.votos_todos_json     (11 MB - redundante con votos_por_mesa)
     - instalaciones.gemini_raw   (0.8 MB - redundante con columnas)

Principio Musk: menos lineas para Claude Code, menos contexto desperdiciado.

Integridad forense preservada:
  - actas.api_response_hash (SHA-256) queda intacto como prueba
  - votos_por_mesa mantiene voto completo
  - instalaciones mantiene hora_instalacion_min + observaciones

Idempotente: puede re-ejecutarse.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

DB_PATH = Path("data/forensic.db")


SCHEMA_ESTADO_HIST = """
CREATE TABLE IF NOT EXISTS acta_estado_historial (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    acta_id INTEGER NOT NULL,
    codigo_estado TEXT,
    descripcion_estado TEXT,
    descripcion_resolucion TEXT,
    fecha_registro_ms INTEGER,
    fecha_registro_iso TEXT,
    FOREIGN KEY (acta_id) REFERENCES actas(acta_id)
)
"""

IDX_ESTADO_HIST = [
    "CREATE INDEX IF NOT EXISTS idx_estado_hist_acta ON acta_estado_historial(acta_id)",
    "CREATE INDEX IF NOT EXISTS idx_estado_hist_cod ON acta_estado_historial(codigo_estado)",
]


def _ms_a_iso(ms: int | None) -> str | None:
    if not ms:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()
    except Exception:
        return None


def crear_tabla(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(SCHEMA_ESTADO_HIST)
    for idx in IDX_ESTADO_HIST:
        cur.execute(idx)
    conn.commit()
    log.info("OK tabla acta_estado_historial creada.")


def poblar_historial(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM acta_estado_historial")
    if cur.fetchone()[0] > 0:
        log.info("SKIP acta_estado_historial ya tiene datos.")
        return 0

    cur.execute(
        "SELECT acta_id, api_response_raw FROM actas "
        "WHERE api_response_raw IS NOT NULL"
    )
    filas = cur.fetchall()
    log.info("Procesando %d api_response_raw...", len(filas))

    insertadas = 0
    errores = 0
    for acta_id, raw in filas:
        try:
            data = json.loads(raw).get("data", {})
            linea = data.get("lineaTiempo") or []
            for item in linea:
                cur.execute("""
                    INSERT INTO acta_estado_historial
                    (acta_id, codigo_estado, descripcion_estado,
                     descripcion_resolucion, fecha_registro_ms,
                     fecha_registro_iso)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    acta_id,
                    item.get("codigoEstadoActa"),
                    item.get("descripcionEstadoActa"),
                    item.get("descripcionEstadoActaResolucion"),
                    item.get("fechaRegistro"),
                    _ms_a_iso(item.get("fechaRegistro")),
                ))
                insertadas += 1
        except Exception as e:
            errores += 1
            if errores < 5:
                log.warning("Error acta %s: %s", acta_id, e)

    conn.commit()
    log.info("OK %d filas insertadas en acta_estado_historial (%d errores).",
             insertadas, errores)
    return insertadas


def dropear_columna(conn: sqlite3.Connection, tabla: str, columna: str) -> None:
    """SQLite 3.35+ soporta DROP COLUMN directo."""
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({tabla})")
    cols = [r[1] for r in cur.fetchall()]
    if columna not in cols:
        log.info("SKIP %s.%s ya no existe.", tabla, columna)
        return
    log.info("DROP %s.%s ...", tabla, columna)
    cur.execute(f"ALTER TABLE {tabla} DROP COLUMN {columna}")
    conn.commit()
    log.info("OK %s.%s eliminada.", tabla, columna)


def vacuum(conn: sqlite3.Connection) -> None:
    log.info("VACUUM en curso (compacta DB)...")
    conn.execute("VACUUM")
    log.info("OK VACUUM completado.")


def main() -> None:
    if not DB_PATH.exists():
        log.error("DB no encontrada: %s", DB_PATH)
        return

    tam_antes = DB_PATH.stat().st_size / (1024 * 1024)
    log.info("Tamano DB antes: %.1f MB", tam_antes)

    conn = sqlite3.connect(DB_PATH)
    try:
        crear_tabla(conn)
        poblar_historial(conn)

        dropear_columna(conn, "actas", "api_response_raw")
        dropear_columna(conn, "actas", "votos_todos_json")
        dropear_columna(conn, "instalaciones", "gemini_raw")

        vacuum(conn)
    finally:
        conn.close()

    tam_despues = DB_PATH.stat().st_size / (1024 * 1024)
    log.info("Tamano DB despues: %.1f MB (-%.1f MB)",
             tam_despues, tam_antes - tam_despues)


if __name__ == "__main__":
    main()
