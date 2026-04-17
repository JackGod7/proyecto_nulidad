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
    conn.execute("PRAGMA busy_timeout=30000")
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

        -- ==============================================
        -- CAPA DE AUDITORÍA (Supabase-compatible)
        -- ==============================================

        -- Partidos (catálogo)
        CREATE TABLE IF NOT EXISTS partidos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL UNIQUE,
            codigo TEXT,
            candidato TEXT,
            candidato_documento TEXT
        );

        -- Locales de votación
        CREATE TABLE IF NOT EXISTS locales_votacion (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo INTEGER UNIQUE,
            nombre TEXT NOT NULL,
            ubigeo TEXT NOT NULL,
            distrito TEXT NOT NULL,
            direccion TEXT,
            total_mesas INTEGER DEFAULT 0
        );

        -- Auditoría por mesa (vista materializada con flags)
        CREATE TABLE IF NOT EXISTS auditoria_mesa (
            acta_id INTEGER PRIMARY KEY,
            mesa TEXT NOT NULL,
            distrito TEXT NOT NULL,
            ubigeo TEXT NOT NULL,
            local_votacion TEXT,
            codigo_local INTEGER,

            -- Estado
            estado_acta TEXT,
            codigo_estado_acta TEXT,
            motivo_jee TEXT,

            -- Totales
            electores_habiles INTEGER,
            asistieron INTEGER,
            ausentismo_pct REAL,
            votos_emitidos INTEGER,
            votos_validos INTEGER,

            -- Hora instalación (Gemini)
            hora_instalacion TEXT,
            tramo_hora TEXT,

            -- Votos principales
            votos_rafael INTEGER DEFAULT 0,
            votos_keiko INTEGER DEFAULT 0,
            votos_nieto INTEGER DEFAULT 0,
            votos_belmont INTEGER DEFAULT 0,
            votos_blanco INTEGER DEFAULT 0,
            votos_nulos INTEGER DEFAULT 0,
            votos_impugnados INTEGER DEFAULT 0,

            -- Porcentajes
            pct_rafael REAL,
            pct_keiko REAL,
            pct_participacion REAL,

            -- PDFs
            tiene_pdf_escrutinio INTEGER DEFAULT 0,
            tiene_pdf_instalacion INTEGER DEFAULT 0,
            tiene_pdf_sufragio INTEGER DEFAULT 0,

            -- Flags de anomalía
            flag_hora_anomala INTEGER DEFAULT 0,
            flag_ausentismo_alto INTEGER DEFAULT 0,
            flag_sin_pdf INTEGER DEFAULT 0,
            flag_jee INTEGER DEFAULT 0,
            flag_observacion INTEGER DEFAULT 0,
            flag_votos_nulos_alto INTEGER DEFAULT 0,
            flag_error_aritmetico INTEGER DEFAULT 0,
            flag_cambio_temporal INTEGER DEFAULT 0,
            total_flags INTEGER DEFAULT 0,

            -- Integridad
            api_response_hash TEXT,
            tiene_raw_json INTEGER DEFAULT 0,
            capturado_at TEXT,
            auditado_at TEXT,

            FOREIGN KEY (acta_id) REFERENCES actas(acta_id)
        );

        -- Resumen auditoría por distrito
        CREATE TABLE IF NOT EXISTS auditoria_distrito (
            ubigeo TEXT PRIMARY KEY,
            distrito TEXT NOT NULL,
            total_mesas INTEGER DEFAULT 0,
            mesas_contabilizadas INTEGER DEFAULT 0,
            mesas_observadas INTEGER DEFAULT 0,
            mesas_anuladas INTEGER DEFAULT 0,
            mesas_sin_pdf INTEGER DEFAULT 0,
            mesas_con_flags INTEGER DEFAULT 0,

            total_electores INTEGER DEFAULT 0,
            total_asistieron INTEGER DEFAULT 0,
            ausentismo_pct REAL,

            votos_rafael INTEGER DEFAULT 0,
            votos_keiko INTEGER DEFAULT 0,
            pct_rafael REAL,
            pct_keiko REAL,

            total_flags INTEGER DEFAULT 0,
            flags_hora_anomala INTEGER DEFAULT 0,
            flags_ausentismo INTEGER DEFAULT 0,
            flags_sin_pdf INTEGER DEFAULT 0,
            flags_jee INTEGER DEFAULT 0,
            flags_nulos_alto INTEGER DEFAULT 0,
            flags_error_aritmetico INTEGER DEFAULT 0,

            snapshots_count INTEGER DEFAULT 0,
            cambios_detectados INTEGER DEFAULT 0,

            actualizado_at TEXT
        );

        -- Fechas ONPE (tracking temporal)
        CREATE TABLE IF NOT EXISTS fecha_onpe (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha_proceso TEXT,
            capturado_at TEXT NOT NULL,
            fuente TEXT DEFAULT 'api'
        );

        -- Indices auditoría
        CREATE INDEX IF NOT EXISTS idx_auditoria_distrito ON auditoria_mesa(distrito);
        CREATE INDEX IF NOT EXISTS idx_auditoria_flags ON auditoria_mesa(total_flags);
        CREATE INDEX IF NOT EXISTS idx_auditoria_estado ON auditoria_mesa(estado_acta);
        CREATE INDEX IF NOT EXISTS idx_locales_ubigeo ON locales_votacion(ubigeo);
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


def poblar_auditoria() -> dict:
    """Puebla auditoria_mesa y auditoria_distrito desde datos existentes."""
    conn = get_conn()
    now = _now()

    # 1. Poblar catálogo de partidos
    partidos = conn.execute(
        "SELECT DISTINCT partido_nombre, partido_codigo, candidato_nombre, candidato_documento "
        "FROM votos_por_mesa WHERE fuente='api'"
    ).fetchall()
    for p in partidos:
        conn.execute(
            "INSERT OR IGNORE INTO partidos (nombre, codigo, candidato, candidato_documento) VALUES (?,?,?,?)",
            (p["partido_nombre"], p["partido_codigo"], p["candidato_nombre"], p["candidato_documento"]),
        )

    # 2. Poblar auditoria_mesa
    actas = conn.execute("""
        SELECT acta_id, mesa, distrito, ubigeo, local_votacion, codigo_local_votacion,
               estado_acta, codigo_estado_acta, estado_acta_resolucion,
               total_electores, total_votantes, votos_emitidos, votos_validos,
               participacion_pct, votos_blanco, votos_nulos, votos_impugnados,
               tiene_pdf_escrutinio, tiene_pdf_instalacion, tiene_pdf_sufragio,
               api_response_hash, api_response_raw, capturado_at
        FROM actas WHERE tiene_datos=1
    """).fetchall()

    count = 0
    for a in actas:
        acta_id = a["acta_id"]

        # Votos principales
        votos = {}
        for v in conn.execute(
            "SELECT partido_nombre, votos FROM votos_por_mesa WHERE acta_id=? AND fuente='api'",
            (acta_id,)
        ).fetchall():
            votos[v["partido_nombre"]] = v["votos"] or 0

        rafael = votos.get("RENOVACIÓN POPULAR", votos.get("RENOVACION POPULAR", 0))
        keiko = votos.get("FUERZA POPULAR", 0)
        nieto = votos.get("PARTIDO DEL BUEN GOBIERNO", 0)
        belmont = 0
        for k, v in votos.items():
            if "BELMONT" in k.upper():
                belmont = v
                break

        total_validos = a["votos_validos"] or 0
        pct_rafael = (rafael / total_validos * 100) if total_validos > 0 else 0
        pct_keiko = (keiko / total_validos * 100) if total_validos > 0 else 0

        electores = a["total_electores"] or 0
        asistieron = a["total_votantes"] or 0
        ausentismo = ((electores - asistieron) / electores * 100) if electores > 0 else 0

        # Hora instalación (Gemini)
        hora = None
        tramo = None
        gemini = conn.execute(
            "SELECT gemini_hora_inicio FROM pdfs WHERE acta_id=? AND tipo=3 AND gemini_extraido=1",
            (acta_id,)
        ).fetchone()
        if gemini and gemini["gemini_hora_inicio"]:
            hora = gemini["gemini_hora_inicio"]
            try:
                h = int(hora.split(":")[0])
                if h < 7:
                    tramo = "MADRUGADA"
                elif h < 8:
                    tramo = "TEMPRANO"
                elif h < 9:
                    tramo = "NORMAL"
                else:
                    tramo = "TARDIO"
            except (ValueError, IndexError):
                tramo = "INVALIDO"

        # Flags
        flag_hora = 1 if tramo in ("MADRUGADA", "TARDIO", "INVALIDO") else 0
        flag_ausentismo = 1 if ausentismo > 40 else 0
        flag_sin_pdf = 1 if not (a["tiene_pdf_escrutinio"] or a["tiene_pdf_instalacion"]) else 0
        flag_jee = 1 if a["codigo_estado_acta"] in ("3", "4", "5") else 0  # observada/anulada/etc
        flag_obs = 1 if a["estado_acta_resolucion"] else 0
        v_nulos = a["votos_nulos"] or 0
        v_emitidos = a["votos_emitidos"] or 1
        flag_nulos = 1 if (v_nulos / v_emitidos * 100) > 10 else 0

        # Error aritmético: emitidos != validos + blanco + nulos + impugnados
        v_blanco = a["votos_blanco"] or 0
        v_imp = a["votos_impugnados"] or 0
        suma = total_validos + v_blanco + v_nulos + v_imp
        flag_error = 1 if (a["votos_emitidos"] or 0) > 0 and suma != (a["votos_emitidos"] or 0) else 0

        # Cambio temporal
        cambio = conn.execute(
            "SELECT COUNT(*) FROM snapshots WHERE acta_id=? AND cambio_detectado=1",
            (acta_id,)
        ).fetchone()[0]
        flag_cambio = 1 if cambio > 0 else 0

        total_flags = flag_hora + flag_ausentismo + flag_sin_pdf + flag_jee + flag_obs + flag_nulos + flag_error + flag_cambio

        conn.execute("""
            INSERT OR REPLACE INTO auditoria_mesa
            (acta_id, mesa, distrito, ubigeo, local_votacion, codigo_local,
             estado_acta, codigo_estado_acta, motivo_jee,
             electores_habiles, asistieron, ausentismo_pct, votos_emitidos, votos_validos,
             hora_instalacion, tramo_hora,
             votos_rafael, votos_keiko, votos_nieto, votos_belmont,
             votos_blanco, votos_nulos, votos_impugnados,
             pct_rafael, pct_keiko, pct_participacion,
             tiene_pdf_escrutinio, tiene_pdf_instalacion, tiene_pdf_sufragio,
             flag_hora_anomala, flag_ausentismo_alto, flag_sin_pdf, flag_jee,
             flag_observacion, flag_votos_nulos_alto, flag_error_aritmetico, flag_cambio_temporal,
             total_flags, api_response_hash, tiene_raw_json, capturado_at, auditado_at)
            VALUES (?,?,?,?,?,?, ?,?,?, ?,?,?,?,?, ?,?, ?,?,?,?, ?,?,?, ?,?,?, ?,?,?, ?,?,?,?, ?,?,?,?, ?,?,?,?,?)
        """, (
            acta_id, a["mesa"], a["distrito"], a["ubigeo"],
            a["local_votacion"], a["codigo_local_votacion"],
            a["estado_acta"], a["codigo_estado_acta"], a["estado_acta_resolucion"],
            electores, asistieron, round(ausentismo, 2),
            a["votos_emitidos"], total_validos,
            hora, tramo,
            rafael, keiko, nieto, belmont,
            v_blanco, v_nulos, v_imp,
            round(pct_rafael, 2), round(pct_keiko, 2),
            round(a["participacion_pct"] or 0, 2),
            a["tiene_pdf_escrutinio"], a["tiene_pdf_instalacion"], a["tiene_pdf_sufragio"],
            flag_hora, flag_ausentismo, flag_sin_pdf, flag_jee,
            flag_obs, flag_nulos, flag_error, flag_cambio,
            total_flags, a["api_response_hash"],
            1 if a["api_response_raw"] else 0,
            a["capturado_at"], now,
        ))
        count += 1

    conn.commit()

    # 3. Poblar auditoria_distrito
    distritos = conn.execute("SELECT DISTINCT ubigeo, distrito FROM auditoria_mesa").fetchall()
    for d in distritos:
        ub, nombre = d["ubigeo"], d["distrito"]
        r = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN codigo_estado_acta='1' THEN 1 ELSE 0 END) as contabilizadas,
                SUM(CASE WHEN codigo_estado_acta IN ('3','4','5') THEN 1 ELSE 0 END) as observadas,
                SUM(CASE WHEN codigo_estado_acta='6' THEN 1 ELSE 0 END) as anuladas,
                SUM(flag_sin_pdf) as sin_pdf,
                SUM(CASE WHEN total_flags>0 THEN 1 ELSE 0 END) as con_flags,
                SUM(electores_habiles) as electores,
                SUM(asistieron) as asistieron,
                SUM(votos_rafael) as rafael,
                SUM(votos_keiko) as keiko,
                SUM(total_flags) as flags_total,
                SUM(flag_hora_anomala) as f_hora,
                SUM(flag_ausentismo_alto) as f_ausen,
                SUM(flag_sin_pdf) as f_pdf,
                SUM(flag_jee) as f_jee,
                SUM(flag_votos_nulos_alto) as f_nulos,
                SUM(flag_error_aritmetico) as f_error
            FROM auditoria_mesa WHERE ubigeo=?
        """, (ub,)).fetchone()

        total_el = r["electores"] or 0
        total_as = r["asistieron"] or 0
        aus_pct = ((total_el - total_as) / total_el * 100) if total_el > 0 else 0
        total_v = r["rafael"] + r["keiko"] if r["rafael"] and r["keiko"] else 0

        snaps = conn.execute(
            "SELECT COUNT(*) FROM snapshots s JOIN actas a ON s.acta_id=a.acta_id WHERE a.ubigeo=?", (ub,)
        ).fetchone()[0]
        cambios = conn.execute(
            "SELECT COUNT(*) FROM snapshots s JOIN actas a ON s.acta_id=a.acta_id WHERE a.ubigeo=? AND s.cambio_detectado=1", (ub,)
        ).fetchone()[0]

        conn.execute("""
            INSERT OR REPLACE INTO auditoria_distrito
            (ubigeo, distrito, total_mesas, mesas_contabilizadas, mesas_observadas, mesas_anuladas,
             mesas_sin_pdf, mesas_con_flags,
             total_electores, total_asistieron, ausentismo_pct,
             votos_rafael, votos_keiko, pct_rafael, pct_keiko,
             total_flags, flags_hora_anomala, flags_ausentismo, flags_sin_pdf,
             flags_jee, flags_nulos_alto, flags_error_aritmetico,
             snapshots_count, cambios_detectados, actualizado_at)
            VALUES (?,?,?,?,?,?, ?,?, ?,?,?, ?,?,?,?, ?,?,?,?, ?,?,?, ?,?,?)
        """, (
            ub, nombre, r["total"], r["contabilizadas"], r["observadas"], r["anuladas"],
            r["sin_pdf"], r["con_flags"],
            total_el, total_as, round(aus_pct, 2),
            r["rafael"], r["keiko"],
            round((r["rafael"] or 0) / max(total_v, 1) * 100, 2) if total_v else 0,
            round((r["keiko"] or 0) / max(total_v, 1) * 100, 2) if total_v else 0,
            r["flags_total"], r["f_hora"], r["f_ausen"], r["f_pdf"],
            r["f_jee"], r["f_nulos"], r["f_error"],
            snaps, cambios, now,
        ))

    conn.commit()

    result = {
        "auditoria_mesa": conn.execute("SELECT COUNT(*) FROM auditoria_mesa").fetchone()[0],
        "auditoria_distrito": conn.execute("SELECT COUNT(*) FROM auditoria_distrito").fetchone()[0],
        "partidos": conn.execute("SELECT COUNT(*) FROM partidos").fetchone()[0],
        "mesas_con_flags": conn.execute("SELECT COUNT(*) FROM auditoria_mesa WHERE total_flags>0").fetchone()[0],
    }

    log_custodia(conn, "AUDITORIA_POBLADA", detalle=result)
    conn.close()
    logger.info("Auditoría poblada: %s", result)
    return result


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
