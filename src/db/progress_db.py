"""SQLite progress tracker — resume-safe scraping."""
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import DATA_DIR

DB_PATH = DATA_DIR / "progress.db"


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Crea tablas si no existen."""
    conn = _connect()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS distritos (
            ubigeo TEXT PRIMARY KEY,
            nombre TEXT NOT NULL,
            total_actas INTEGER DEFAULT 0,
            presidenciales INTEGER DEFAULT 0,
            procesadas INTEGER DEFAULT 0,
            con_datos INTEGER DEFAULT 0,
            sin_pdf INTEGER DEFAULT 0,
            pdfs_descargados INTEGER DEFAULT 0,
            estado TEXT DEFAULT 'pendiente',
            inicio_at TEXT,
            fin_at TEXT,
            error TEXT
        );

        CREATE TABLE IF NOT EXISTS actas (
            acta_id INTEGER PRIMARY KEY,
            mesa TEXT NOT NULL,
            ubigeo TEXT NOT NULL,
            distrito TEXT NOT NULL,
            estado_acta TEXT,
            tiene_datos INTEGER DEFAULT 0,
            votos_json TEXT,
            pdf_escrutinio INTEGER DEFAULT 0,
            pdf_instalacion INTEGER DEFAULT 0,
            pdf_sufragio INTEGER DEFAULT 0,
            procesada_at TEXT,
            error TEXT
        );

        CREATE TABLE IF NOT EXISTS pdfs (
            archivo_id TEXT PRIMARY KEY,
            acta_id INTEGER NOT NULL,
            mesa TEXT NOT NULL,
            distrito TEXT NOT NULL,
            tipo INTEGER NOT NULL,
            nombre_destino TEXT NOT NULL,
            descargado INTEGER DEFAULT 0,
            tamano_bytes INTEGER DEFAULT 0,
            descarga_at TEXT,
            error TEXT,
            FOREIGN KEY (acta_id) REFERENCES actas(acta_id)
        );

        CREATE INDEX IF NOT EXISTS idx_actas_ubigeo ON actas(ubigeo);
        CREATE INDEX IF NOT EXISTS idx_actas_distrito ON actas(distrito);
        CREATE INDEX IF NOT EXISTS idx_pdfs_distrito ON pdfs(distrito);
        CREATE INDEX IF NOT EXISTS idx_pdfs_descargado ON pdfs(descargado);
    """)
    conn.commit()
    conn.close()


# --- Distritos ---

def registrar_distrito(ubigeo: str, nombre: str) -> None:
    conn = _connect()
    conn.execute(
        "INSERT OR IGNORE INTO distritos (ubigeo, nombre) VALUES (?, ?)",
        (ubigeo, nombre),
    )
    conn.commit()
    conn.close()


def distrito_estado(ubigeo: str) -> str | None:
    """Retorna estado: pendiente | en_progreso | completado | error."""
    conn = _connect()
    row = conn.execute("SELECT estado FROM distritos WHERE ubigeo = ?", (ubigeo,)).fetchone()
    conn.close()
    return row["estado"] if row else None


def iniciar_distrito(ubigeo: str, total_actas: int, presidenciales: int) -> None:
    conn = _connect()
    conn.execute("""
        UPDATE distritos SET
            total_actas = ?, presidenciales = ?, procesadas = 0,
            estado = 'en_progreso', inicio_at = ?, error = NULL
        WHERE ubigeo = ?
    """, (total_actas, presidenciales, _now(), ubigeo))
    conn.commit()
    conn.close()


def completar_distrito(ubigeo: str, con_datos: int, sin_pdf: int, pdfs: int) -> None:
    conn = _connect()
    conn.execute("""
        UPDATE distritos SET
            con_datos = ?, sin_pdf = ?, pdfs_descargados = ?,
            estado = 'completado', fin_at = ?
        WHERE ubigeo = ?
    """, (con_datos, sin_pdf, pdfs, _now(), ubigeo))
    conn.commit()
    conn.close()


def error_distrito(ubigeo: str, error: str) -> None:
    conn = _connect()
    conn.execute(
        "UPDATE distritos SET estado = 'error', error = ?, fin_at = ? WHERE ubigeo = ?",
        (error, _now(), ubigeo),
    )
    conn.commit()
    conn.close()


def incrementar_procesadas(ubigeo: str) -> None:
    conn = _connect()
    conn.execute("UPDATE distritos SET procesadas = procesadas + 1 WHERE ubigeo = ?", (ubigeo,))
    conn.commit()
    conn.close()


# --- Actas ---

def acta_ya_procesada(acta_id: int) -> bool:
    conn = _connect()
    row = conn.execute("SELECT 1 FROM actas WHERE acta_id = ? AND tiene_datos = 1", (acta_id,)).fetchone()
    conn.close()
    return row is not None


def registrar_acta(
    acta_id: int,
    mesa: str,
    ubigeo: str,
    distrito: str,
    estado_acta: str,
    votos: dict[str, Any] | None = None,
    tiene_datos: bool = False,
) -> None:
    conn = _connect()
    conn.execute("""
        INSERT OR REPLACE INTO actas
            (acta_id, mesa, ubigeo, distrito, estado_acta, tiene_datos, votos_json, procesada_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        acta_id, mesa, ubigeo, distrito, estado_acta,
        1 if tiene_datos else 0,
        json.dumps(votos, ensure_ascii=False) if votos else None,
        _now(),
    ))
    conn.commit()
    conn.close()


def error_acta(acta_id: int, mesa: str, ubigeo: str, distrito: str, error: str) -> None:
    conn = _connect()
    conn.execute("""
        INSERT OR REPLACE INTO actas (acta_id, mesa, ubigeo, distrito, error, procesada_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (acta_id, mesa, ubigeo, distrito, error, _now()))
    conn.commit()
    conn.close()


# --- PDFs ---

def pdf_ya_descargado(archivo_id: str) -> bool:
    conn = _connect()
    row = conn.execute("SELECT 1 FROM pdfs WHERE archivo_id = ? AND descargado = 1", (archivo_id,)).fetchone()
    conn.close()
    return row is not None


def registrar_pdf(
    archivo_id: str,
    acta_id: int,
    mesa: str,
    distrito: str,
    tipo: int,
    nombre_destino: str,
    descargado: bool = False,
    tamano: int = 0,
) -> None:
    conn = _connect()
    conn.execute("""
        INSERT OR REPLACE INTO pdfs
            (archivo_id, acta_id, mesa, distrito, tipo, nombre_destino, descargado, tamano_bytes, descarga_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        archivo_id, acta_id, mesa, distrito, tipo, nombre_destino,
        1 if descargado else 0, tamano, _now() if descargado else None,
    ))
    conn.commit()
    conn.close()


def error_pdf(archivo_id: str, error: str) -> None:
    conn = _connect()
    conn.execute("UPDATE pdfs SET error = ? WHERE archivo_id = ?", (error, archivo_id))
    conn.commit()
    conn.close()


# --- Consultas de progreso ---

def resumen_progreso() -> dict[str, Any]:
    """Resumen global del scraping."""
    conn = _connect()
    d = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN estado = 'completado' THEN 1 ELSE 0 END) as completados,
            SUM(CASE WHEN estado = 'en_progreso' THEN 1 ELSE 0 END) as en_progreso,
            SUM(CASE WHEN estado = 'error' THEN 1 ELSE 0 END) as errores,
            SUM(CASE WHEN estado = 'pendiente' THEN 1 ELSE 0 END) as pendientes
        FROM distritos
    """).fetchone()

    a = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(tiene_datos) as con_datos
        FROM actas
    """).fetchone()

    p = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(descargado) as descargados,
            SUM(tamano_bytes) as bytes_total
        FROM pdfs
    """).fetchone()

    conn.close()
    return {
        "distritos": dict(d),
        "actas": dict(a),
        "pdfs": {**dict(p), "mb_total": round((p["bytes_total"] or 0) / 1024 / 1024, 1)},
    }


def distritos_pendientes() -> list[dict]:
    """Distritos que faltan o tuvieron error."""
    conn = _connect()
    rows = conn.execute(
        "SELECT ubigeo, nombre, estado, error FROM distritos WHERE estado IN ('pendiente', 'error', 'en_progreso') ORDER BY nombre"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def actas_fallidas() -> list[dict]:
    """Actas con error para reintentar."""
    conn = _connect()
    rows = conn.execute(
        "SELECT acta_id, mesa, distrito, error FROM actas WHERE error IS NOT NULL ORDER BY distrito, mesa"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def pdfs_pendientes(distrito: str | None = None) -> list[dict]:
    """PDFs no descargados."""
    conn = _connect()
    query = "SELECT archivo_id, mesa, distrito, tipo, nombre_destino FROM pdfs WHERE descargado = 0"
    params: tuple = ()
    if distrito:
        query += " AND distrito = ?"
        params = (distrito,)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
