"""Normaliza votos_todos_json -> votos_por_mesa para actas pendientes."""
from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = Path("data/forensic.db")


def actas_pendientes(cur: sqlite3.Cursor) -> list[tuple[int, str, str]]:
    cur.execute("""
        SELECT a.acta_id, a.distrito, a.votos_todos_json
        FROM actas a
        WHERE a.votos_todos_json IS NOT NULL
          AND a.votos_todos_json != ''
          AND a.votos_todos_json != '{}'
          AND NOT EXISTS (
              SELECT 1 FROM votos_por_mesa v WHERE v.acta_id = a.acta_id
          )
    """)
    return cur.fetchall()


def normalizar_acta(cur: sqlite3.Cursor, acta_id: int, votos_json: str) -> int:
    try:
        votos = json.loads(votos_json)
    except json.JSONDecodeError:
        logger.warning("acta_id=%s json invalido", acta_id)
        return 0
    filas = 0
    for partido, n in votos.items():
        if not isinstance(n, int):
            continue
        cur.execute("""
            INSERT INTO votos_por_mesa (acta_id, partido_nombre, votos, fuente)
            VALUES (?, ?, ?, 'api')
        """, (acta_id, partido, n))
        filas += 1
    return filas


def log_custodia(cur: sqlite3.Cursor, resumen: dict) -> None:
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).isoformat()
    cur.execute("""
        INSERT INTO cadena_custodia (timestamp, accion, entidad_tipo, entidad_id, operador, maquina, detalle)
        VALUES (?, 'normalizar_votos_por_mesa', 'sistema', 'batch', 'claude', 'maquina-1', ?)
    """, (ts, json.dumps(resumen, ensure_ascii=False)))


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"DB no existe: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        pendientes = actas_pendientes(cur)
        logger.info("Actas pendientes de normalizar: %d", len(pendientes))

        por_distrito: dict[str, int] = {}
        filas_totales = 0
        for acta_id, distrito, json_str in pendientes:
            n = normalizar_acta(cur, acta_id, json_str)
            filas_totales += n
            por_distrito[distrito] = por_distrito.get(distrito, 0) + 1

        resumen = {
            "actas_normalizadas": len(pendientes),
            "filas_insertadas": filas_totales,
            "por_distrito": por_distrito,
        }
        log_custodia(cur, resumen)
        conn.commit()

        logger.info("=== RESUMEN ===")
        logger.info("Actas normalizadas: %d", len(pendientes))
        logger.info("Filas votos_por_mesa insertadas: %d", filas_totales)
        for d, n in sorted(por_distrito.items()):
            logger.info("  %-35s %d actas", d, n)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
