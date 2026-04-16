"""Schema v2 forense + migración desde v1."""
import hashlib
import json
import getpass
import logging
import socket
import sqlite3

logger = logging.getLogger(__name__)
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import DATA_DIR

FORENSIC_DB = DATA_DIR / "forensic.db"
V1_DB = DATA_DIR / "progress.db"
SOFTWARE_VERSION = "2.0.0"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_conn(db_path: Path | None = None) -> sqlite3.Connection:
    path = db_path or FORENSIC_DB
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_forensic_db() -> sqlite3.Connection:
    """Crea todas las tablas del schema v2 forense."""
    conn = get_conn()
    conn.executescript("""
        -- Distritos
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

        -- Actas v2 (completa)
        CREATE TABLE IF NOT EXISTS actas (
            acta_id INTEGER PRIMARY KEY,
            mesa TEXT NOT NULL,
            ubigeo TEXT NOT NULL,
            distrito TEXT NOT NULL,
            departamento TEXT,
            provincia TEXT,
            local_votacion TEXT,
            codigo_local_votacion INTEGER,

            -- Estado completo
            estado_acta TEXT,
            codigo_estado_acta TEXT,
            estado_acta_resolucion TEXT,
            estado_descripcion_resolucion TEXT,
            sub_estado_acta TEXT,
            estado_computo TEXT,
            solucion_tecnologica TEXT,

            -- Totales
            total_electores INTEGER,
            total_votantes INTEGER,
            votos_emitidos INTEGER,
            votos_validos INTEGER,
            participacion_pct REAL,

            -- Votos ALL partidos
            votos_todos_json TEXT,
            votos_blanco INTEGER,
            votos_nulos INTEGER,
            votos_impugnados INTEGER,

            -- PDFs
            tiene_pdf_escrutinio INTEGER DEFAULT 0,
            tiene_pdf_instalacion INTEGER DEFAULT 0,
            tiene_pdf_sufragio INTEGER DEFAULT 0,

            -- Raw API (evidencia legal)
            api_response_raw TEXT,
            api_response_hash TEXT,

            -- Trazabilidad
            tiene_datos INTEGER DEFAULT 0,
            captura_version TEXT DEFAULT '2.0.0',
            operador TEXT,
            maquina TEXT,
            capturado_at TEXT,
            error TEXT
        );

        -- Votos normalizados (ALL partidos)
        CREATE TABLE IF NOT EXISTS votos_por_mesa (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            acta_id INTEGER NOT NULL,
            partido_nombre TEXT NOT NULL,
            partido_codigo TEXT,
            candidato_nombre TEXT,
            candidato_documento TEXT,
            votos INTEGER,
            porcentaje_validos REAL,
            porcentaje_emitidos REAL,
            posicion_cedula INTEGER,
            fuente TEXT DEFAULT 'api',
            FOREIGN KEY (acta_id) REFERENCES actas(acta_id),
            UNIQUE(acta_id, partido_nombre, fuente)
        );

        -- PDFs v2
        CREATE TABLE IF NOT EXISTS pdfs (
            archivo_id TEXT PRIMARY KEY,
            acta_id INTEGER NOT NULL,
            mesa TEXT NOT NULL,
            distrito TEXT NOT NULL,
            tipo INTEGER NOT NULL,
            nombre_destino TEXT NOT NULL,
            descargado INTEGER DEFAULT 0,
            tamano_bytes INTEGER DEFAULT 0,

            -- Integridad
            sha256_hash TEXT,
            hash_calculado_at TEXT,
            archivo_en_disco INTEGER DEFAULT 0,

            -- Gemini AI
            gemini_extraido INTEGER DEFAULT 0,
            gemini_votos_json TEXT,
            gemini_confianza REAL,
            gemini_hora_inicio TEXT,
            gemini_hora_fin TEXT,
            gemini_total_votaron INTEGER,
            gemini_observaciones TEXT,
            gemini_raw_response TEXT,
            gemini_extraido_at TEXT,

            descarga_at TEXT,
            error TEXT,
            FOREIGN KEY (acta_id) REFERENCES actas(acta_id)
        );

        -- Snapshots temporales
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            acta_id INTEGER NOT NULL,
            snapshot_at TEXT NOT NULL,
            estado_acta TEXT,
            votos_json TEXT,
            api_response_hash TEXT,
            cambio_detectado INTEGER DEFAULT 0,
            diff_descripcion TEXT,
            FOREIGN KEY (acta_id) REFERENCES actas(acta_id)
        );

        -- Discrepancias API vs PDF
        CREATE TABLE IF NOT EXISTS discrepancias (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            acta_id INTEGER NOT NULL,
            mesa TEXT NOT NULL,
            distrito TEXT NOT NULL,
            tipo TEXT NOT NULL,
            campo TEXT,
            valor_api TEXT,
            valor_pdf TEXT,
            diferencia INTEGER,
            severidad TEXT,
            verificado_por TEXT,
            verificado_at TEXT,
            notas TEXT,
            FOREIGN KEY (acta_id) REFERENCES actas(acta_id)
        );

        -- Cadena de custodia
        CREATE TABLE IF NOT EXISTS cadena_custodia (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            accion TEXT NOT NULL,
            entidad_tipo TEXT,
            entidad_id TEXT,
            operador TEXT,
            maquina TEXT,
            version_software TEXT,
            detalle TEXT
        );

        -- Indices
        CREATE INDEX IF NOT EXISTS idx_actas_distrito ON actas(distrito);
        CREATE INDEX IF NOT EXISTS idx_actas_estado ON actas(estado_acta);
        CREATE INDEX IF NOT EXISTS idx_actas_ubigeo ON actas(ubigeo);
        CREATE INDEX IF NOT EXISTS idx_votos_acta ON votos_por_mesa(acta_id);
        CREATE INDEX IF NOT EXISTS idx_votos_partido ON votos_por_mesa(partido_nombre);
        CREATE INDEX IF NOT EXISTS idx_votos_fuente ON votos_por_mesa(fuente);
        CREATE INDEX IF NOT EXISTS idx_pdfs_acta ON pdfs(acta_id);
        CREATE INDEX IF NOT EXISTS idx_pdfs_distrito ON pdfs(distrito);
        CREATE INDEX IF NOT EXISTS idx_pdfs_descargado ON pdfs(descargado);
        CREATE INDEX IF NOT EXISTS idx_pdfs_gemini ON pdfs(gemini_extraido);
        CREATE INDEX IF NOT EXISTS idx_snapshots_acta ON snapshots(acta_id);
        CREATE INDEX IF NOT EXISTS idx_discrepancias_acta ON discrepancias(acta_id);
        CREATE INDEX IF NOT EXISTS idx_discrepancias_tipo ON discrepancias(tipo);
        CREATE INDEX IF NOT EXISTS idx_custodia_ts ON cadena_custodia(timestamp);
    """)
    conn.commit()
    return conn


def log_custodia(
    conn: sqlite3.Connection,
    accion: str,
    entidad_tipo: str = "",
    entidad_id: str = "",
    detalle: dict | None = None,
) -> None:
    conn.execute(
        """INSERT INTO cadena_custodia
           (timestamp, accion, entidad_tipo, entidad_id, operador, maquina, version_software, detalle)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (_now(), accion, entidad_tipo, entidad_id,
         getpass.getuser(), socket.gethostname(), SOFTWARE_VERSION,
         json.dumps(detalle or {}, ensure_ascii=False)),
    )
    conn.commit()


def migrate_v1_to_v2() -> None:
    """Migra datos de progress.db (v1) a forensic.db (v2)."""
    if not V1_DB.exists():
        return

    v1 = sqlite3.connect(str(V1_DB))
    v1.row_factory = sqlite3.Row
    v2 = init_forensic_db()

    log_custodia(v2, "MIGRACION_INICIO", detalle={"v1_db": str(V1_DB)})

    # Migrar distritos
    rows = v1.execute("SELECT * FROM distritos").fetchall()
    for r in rows:
        v2.execute(
            "INSERT OR IGNORE INTO distritos (ubigeo, nombre, total_actas, presidenciales, procesadas, con_datos, sin_pdf, pdfs_descargados, estado, inicio_at, fin_at, error) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (r["ubigeo"], r["nombre"], r["total_actas"], r["presidenciales"],
             r["procesadas"], r["con_datos"], r["sin_pdf"], r["pdfs_descargados"],
             r["estado"], r["inicio_at"], r["fin_at"], r["error"]),
        )

    # Migrar actas
    rows = v1.execute("SELECT * FROM actas").fetchall()
    for r in rows:
        votos_json = r["votos_json"]
        votos_dict = {}
        if votos_json:
            try:
                votos_dict = json.loads(votos_json)
            except json.JSONDecodeError:
                pass

        v2.execute(
            """INSERT OR IGNORE INTO actas
               (acta_id, mesa, ubigeo, distrito, estado_acta, tiene_datos,
                total_electores, total_votantes, votos_emitidos, votos_validos,
                participacion_pct, captura_version, capturado_at, error)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (r["acta_id"], r["mesa"], r["ubigeo"], r["distrito"],
             r["estado_acta"], r["tiene_datos"],
             votos_dict.get("TOTAL_ELECTORES"),
             votos_dict.get("TOTAL_VOTANTES"),
             votos_dict.get("VOTOS_EMITIDOS"),
             votos_dict.get("VOTOS_VALIDOS"),
             votos_dict.get("PARTICIPACION_PCT"),
             "v1_migrated", r["procesada_at"], r["error"]),
        )

    # Migrar PDFs (solo si el acta existe en v2)
    rows = v1.execute("SELECT * FROM pdfs").fetchall()
    actas_v2 = {r[0] for r in v2.execute("SELECT acta_id FROM actas").fetchall()}
    skipped_pdfs = 0
    for r in rows:
        if r["acta_id"] not in actas_v2:
            skipped_pdfs += 1
            continue
        v2.execute(
            """INSERT OR IGNORE INTO pdfs
               (archivo_id, acta_id, mesa, distrito, tipo, nombre_destino,
                descargado, tamano_bytes, descarga_at, error)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (r["archivo_id"], r["acta_id"], r["mesa"], r["distrito"],
             r["tipo"], r["nombre_destino"], r["descargado"],
             r["tamano_bytes"], r["descarga_at"], r["error"]),
        )
    if skipped_pdfs:
        logger.warning("PDFs sin acta parent: %d skipped", skipped_pdfs)

    v2.commit()
    migrated = {
        "distritos": v2.execute("SELECT COUNT(*) FROM distritos").fetchone()[0],
        "actas": v2.execute("SELECT COUNT(*) FROM actas").fetchone()[0],
        "pdfs": v2.execute("SELECT COUNT(*) FROM pdfs").fetchone()[0],
    }
    log_custodia(v2, "MIGRACION_COMPLETA", detalle=migrated)

    v1.close()
    v2.close()


if __name__ == "__main__":
    init_forensic_db()
    migrate_v1_to_v2()
    conn = get_conn()
    for table in ["distritos", "actas", "pdfs", "cadena_custodia"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count}")
    conn.close()
