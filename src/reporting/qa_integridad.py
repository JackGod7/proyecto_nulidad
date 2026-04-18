"""QA de integridad forensic.db — consultas de validacion pre-entrega estadistico."""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

DB_PATH = Path("data/forensic.db")


def _row(cur: sqlite3.Cursor, sql: str, params: tuple = ()) -> tuple:
    cur.execute(sql, params)
    return cur.fetchone()


def _all(cur: sqlite3.Cursor, sql: str, params: tuple = ()) -> list[tuple]:
    cur.execute(sql, params)
    return cur.fetchall()


def seccion_conteos(cur: sqlite3.Cursor) -> None:
    logger.info("=" * 70)
    logger.info("1. CONTEOS GLOBALES")
    logger.info("=" * 70)
    tablas = [
        "distritos", "actas", "votos_por_mesa", "pdfs",
        "instalaciones", "cadena_custodia", "auditoria_distrito",
    ]
    for t in tablas:
        (n,) = _row(cur, f"SELECT COUNT(*) FROM {t}")
        logger.info(f"  {t:25} {n:>10,}")


def seccion_cobertura(cur: sqlite3.Cursor) -> None:
    logger.info("\n" + "=" * 70)
    logger.info("2. COBERTURA POR DISTRITO (actas con datos vs total)")
    logger.info("=" * 70)
    logger.info(f"{'DISTRITO':35}{'TOTAL':>8}{'CON_DATOS':>12}{'%':>8}")
    rows = _all(cur, """
        SELECT distrito,
               COUNT(*) AS total,
               SUM(CASE WHEN tiene_datos=1 THEN 1 ELSE 0 END) AS con_datos
        FROM actas
        GROUP BY distrito
        ORDER BY distrito
    """)
    for distrito, total, con_datos in rows:
        pct = (con_datos / total * 100) if total else 0
        marca = "" if pct >= 99 else "  <--"
        logger.info(f"{distrito:35}{total:>8,}{con_datos:>12,}{pct:>7.1f}%{marca}")


def seccion_actas_faltantes(cur: sqlite3.Cursor) -> None:
    logger.info("\n" + "=" * 70)
    logger.info("3. MESAS SIN ACTA ESCRUTINIO PRESIDENCIAL (votos sin respaldo)")
    logger.info("=" * 70)
    (mesas, votos) = _row(cur, """
        SELECT COUNT(*), COALESCE(SUM(total_votantes),0)
        FROM actas
        WHERE flag_sin_acta = 1
    """)
    logger.info(f"  Total mesas sin acta: {mesas:,}")
    logger.info(f"  Votos sin respaldo:   {votos:,}")
    logger.info("")
    logger.info(f"  {'DISTRITO':35}{'MESAS':>10}{'VOTOS':>12}")
    rows = _all(cur, """
        SELECT distrito, COUNT(*) AS mesas, COALESCE(SUM(total_votantes),0) AS votos
        FROM actas
        WHERE flag_sin_acta = 1
        GROUP BY distrito
        ORDER BY mesas DESC
        LIMIT 15
    """)
    for distrito, mesas, votos in rows:
        logger.info(f"  {distrito:35}{mesas:>10,}{votos:>12,}")


def seccion_votos_consistencia(cur: sqlite3.Cursor) -> None:
    logger.info("\n" + "=" * 70)
    logger.info("4. CONSISTENCIA VOTOS (votos_por_mesa vs actas.votos_emitidos)")
    logger.info("=" * 70)
    rows = _all(cur, """
        SELECT a.distrito,
               COUNT(*) AS actas,
               COALESCE(SUM(a.votos_emitidos),0) AS suma_emitidos,
               COALESCE(SUM(a.votos_validos),0) AS suma_validos,
               COALESCE(SUM(a.votos_blanco),0)+COALESCE(SUM(a.votos_nulos),0)+COALESCE(SUM(a.votos_impugnados),0) AS suma_otros
        FROM actas a
        WHERE a.tiene_datos = 1
        GROUP BY a.distrito
    """)
    por_distrito_mesa = dict(_all(cur, """
        SELECT a.distrito, COALESCE(SUM(v.votos),0)
        FROM votos_por_mesa v
        JOIN actas a ON a.acta_id = v.acta_id
        GROUP BY a.distrito
    """))
    con_votos_distrito = dict(_all(cur, """
        SELECT a.distrito, COUNT(DISTINCT v.acta_id)
        FROM votos_por_mesa v
        JOIN actas a ON a.acta_id = v.acta_id
        GROUP BY a.distrito
    """))

    logger.info("  SUM_MESA = votos_por_mesa (solo partidos = validos)")
    logger.info("  SUM_VAL  = actas.votos_validos  |  SUM_EMI = emitidos")
    logger.info("")
    logger.info(
        f"  {'DISTRITO':35}{'ACTAS':>8}{'CON_VOT':>10}"
        f"{'SUM_MESA':>12}{'SUM_VAL':>12}{'DIFF_VAL':>10}"
    )
    for distrito, actas, suma_emit, suma_val, suma_otros in rows:
        cv = con_votos_distrito.get(distrito, 0)
        sm = por_distrito_mesa.get(distrito, 0)
        diff = sm - suma_val
        marca = ""
        if cv == 0:
            marca = "  <-- sin_votos"
        elif cv < actas * 0.5:
            marca = "  <-- cobert<50%"
        elif suma_val and abs(diff) > suma_val * 0.005:
            marca = "  <-- diff>0.5%"
        logger.info(
            f"  {distrito:35}{actas:>8,}{cv:>10,}"
            f"{sm:>12,}{suma_val:>12,}{diff:>10,}{marca}"
        )


def seccion_pdfs(cur: sqlite3.Cursor) -> None:
    logger.info("\n" + "=" * 70)
    logger.info("5. PDFs DESCARGADOS POR TIPO")
    logger.info("=" * 70)
    tipo_nombre = {1: "ESCRUTINIO", 3: "INSTALACION", 4: "SUFRAGIO"}
    rows = _all(cur, """
        SELECT tipo,
               COUNT(*) AS total,
               SUM(CASE WHEN archivo_en_disco=1 THEN 1 ELSE 0 END) AS en_disco,
               SUM(CASE WHEN sha256_hash IS NOT NULL THEN 1 ELSE 0 END) AS con_hash,
               SUM(CASE WHEN gemini_extraido=1 THEN 1 ELSE 0 END) AS con_gemini
        FROM pdfs
        GROUP BY tipo
        ORDER BY tipo
    """)
    logger.info(
        f"  {'TIPO':15}{'TOTAL':>10}{'EN_DISCO':>12}{'CON_HASH':>12}{'CON_GEMINI':>12}"
    )
    for tipo, total, disco, hash_, gem in rows:
        nombre = tipo_nombre.get(tipo, f"tipo_{tipo}")
        logger.info(f"  {nombre:15}{total:>10,}{disco:>12,}{hash_:>12,}{gem:>12,}")


def seccion_instalaciones(cur: sqlite3.Cursor) -> None:
    logger.info("\n" + "=" * 70)
    logger.info("6. INSTALACIONES — EXTRACCION HORAS GEMINI")
    logger.info("=" * 70)
    (total,) = _row(cur, "SELECT COUNT(*) FROM instalaciones")
    (con_hora,) = _row(
        cur, "SELECT COUNT(*) FROM instalaciones WHERE hora_instalacion_raw IS NOT NULL"
    )
    logger.info(f"  Total registros:        {total:,}")
    logger.info(f"  Con hora extraida:      {con_hora:,}")
    if total:
        logger.info(f"  Pendientes:             {total - con_hora:,}")

    rows = _all(cur, """
        SELECT a.distrito,
               COUNT(i.acta_id) AS total,
               SUM(CASE WHEN i.hora_instalacion_raw IS NOT NULL THEN 1 ELSE 0 END) AS con_hora
        FROM instalaciones i
        JOIN actas a ON a.acta_id = i.acta_id
        GROUP BY a.distrito
        ORDER BY a.distrito
    """)
    logger.info(f"\n  {'DISTRITO':35}{'TOTAL':>10}{'CON_HORA':>12}")
    for d, t, h in rows:
        logger.info(f"  {d:35}{t:>10,}{h:>12,}")


def seccion_duplicados(cur: sqlite3.Cursor) -> None:
    logger.info("\n" + "=" * 70)
    logger.info("7. DUPLICADOS (mesa debe ser unica por distrito)")
    logger.info("=" * 70)
    rows = _all(cur, """
        SELECT distrito, mesa, COUNT(*) AS n
        FROM actas
        WHERE mesa IS NOT NULL AND mesa != ''
        GROUP BY distrito, mesa
        HAVING n > 1
        ORDER BY n DESC
        LIMIT 10
    """)
    (huecos,) = _row(cur, "SELECT COUNT(*) FROM actas WHERE mesa IS NULL OR mesa=''")
    logger.info(f"  Actas con mesa vacia (fetch fail): {huecos}")
    if not rows:
        logger.info("  OK -- sin duplicados reales")
    else:
        for d, m, n in rows:
            logger.info(f"  DUP: {d:25} mesa={m!r} x{n}")


def seccion_encoding(cur: sqlite3.Cursor) -> None:
    logger.info("\n" + "=" * 70)
    logger.info("8. ENCODING (detectar mojibake residual)")
    logger.info("=" * 70)
    rows = _all(cur, """
        SELECT DISTINCT distrito
        FROM actas
        WHERE distrito LIKE '%?%' OR distrito LIKE '%�%'
           OR distrito LIKE '%Ã%' OR distrito LIKE '%â%'
    """)
    if not rows:
        logger.info("  OK — sin caracteres corruptos")
    else:
        for (d,) in rows:
            logger.info(f"  CORRUPTO: {d!r}")


def seccion_custodia(cur: sqlite3.Cursor) -> None:
    logger.info("\n" + "=" * 70)
    logger.info("9. CADENA DE CUSTODIA — resumen")
    logger.info("=" * 70)
    rows = _all(cur, """
        SELECT accion, COUNT(*) AS n
        FROM cadena_custodia
        GROUP BY accion
        ORDER BY n DESC
        LIMIT 15
    """)
    for accion, n in rows:
        logger.info(f"  {accion:40}{n:>8,}")


def main() -> None:
    if not DB_PATH.exists():
        logger.error("DB no existe: %s", DB_PATH)
        raise SystemExit(1)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        seccion_conteos(cur)
        seccion_cobertura(cur)
        seccion_actas_faltantes(cur)
        seccion_votos_consistencia(cur)
        seccion_pdfs(cur)
        seccion_instalaciones(cur)
        seccion_duplicados(cur)
        seccion_encoding(cur)
        seccion_custodia(cur)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
