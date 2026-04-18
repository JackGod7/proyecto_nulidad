"""Calculo de umbrales legales LOE para nulidad electoral.

Base normativa:
  Art. 363 LOE — nulidad de MESA (causales: instalacion irregular, violencia,
                 coaccion, etc). Efecto: se excluye la mesa del computo.
  Art. 364 LOE — nulidad de la ELECCION cuando
                 (votos_blancos + votos_nulos) > 2/3 de votos emitidos.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

DB_PATH = Path("data/forensic.db")


@dataclass(frozen=True)
class UmbralDistrito:
    distrito: str
    total_mesas: int
    mesas_tardias: int
    pct_tardias: float
    total_emitidos: int
    total_blanco: int
    total_nulos: int
    pct_blanco_nulos: float
    mesas_sin_acta: int
    votos_sin_acta: int
    pct_votos_sin_acta: float
    aplica_art_363: bool
    aplica_art_364: bool
    causal_propuesta: str


def _row(cur: sqlite3.Cursor, sql: str, params: tuple = ()) -> tuple:
    cur.execute(sql, params)
    return cur.fetchone()


def calcular(distrito: str, umbral_hora: float = 9.0) -> UmbralDistrito:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        (total_mesas,) = _row(cur,
            "SELECT COUNT(*) FROM actas WHERE distrito = ? AND tiene_datos = 1",
            (distrito,))

        (emitidos, blanco, nulos) = _row(cur, """
            SELECT
                COALESCE(SUM(votos_emitidos), 0),
                COALESCE(SUM(votos_blanco), 0),
                COALESCE(SUM(votos_nulos), 0)
            FROM actas
            WHERE distrito = ? AND tiene_datos = 1
        """, (distrito,))

        (mesas_tardias,) = _row(cur, """
            SELECT COUNT(DISTINCT i.acta_id)
            FROM instalaciones i
            JOIN actas a ON a.acta_id = i.acta_id
            WHERE a.distrito = ? AND i.hora_instalacion_min > ?
        """, (distrito, int(umbral_hora * 60)))

        (mesas_sin_acta, votos_sin_acta) = _row(cur, """
            SELECT
                COALESCE(SUM(flag_sin_acta), 0),
                COALESCE(SUM(CASE WHEN flag_sin_acta=1 THEN total_votantes ELSE 0 END), 0)
            FROM actas
            WHERE distrito = ?
        """, (distrito,))
    finally:
        conn.close()

    pct_tardias = (mesas_tardias / total_mesas * 100) if total_mesas else 0.0
    pct_blanco_nulos = ((blanco + nulos) / emitidos * 100) if emitidos else 0.0
    pct_votos_sin_acta = (votos_sin_acta / emitidos * 100) if emitidos else 0.0

    aplica_363 = mesas_tardias > 0 or mesas_sin_acta > 0
    aplica_364 = pct_blanco_nulos > (2 / 3 * 100)

    if aplica_364:
        causal = "Nulidad de la eleccion (art. 364 LOE)"
    elif aplica_363:
        causal = "Nulidad parcial de mesas (art. 363 LOE)"
    else:
        causal = "No se configura causal directa"

    return UmbralDistrito(
        distrito=distrito,
        total_mesas=total_mesas,
        mesas_tardias=mesas_tardias,
        pct_tardias=round(pct_tardias, 2),
        total_emitidos=emitidos,
        total_blanco=blanco,
        total_nulos=nulos,
        pct_blanco_nulos=round(pct_blanco_nulos, 2),
        mesas_sin_acta=mesas_sin_acta,
        votos_sin_acta=votos_sin_acta,
        pct_votos_sin_acta=round(pct_votos_sin_acta, 2),
        aplica_art_363=aplica_363,
        aplica_art_364=aplica_364,
        causal_propuesta=causal,
    )


def main() -> None:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    log = logging.getLogger(__name__)

    distritos = [
        "MIRAFLORES", "SAN JUAN DE MIRAFLORES", "SANTIAGO DE SURCO",
        "VILLA EL SALVADOR", "SAN ISIDRO", "PUCUSANA", "MAGDALENA DEL MAR",
    ]
    log.info(f"{'DISTRITO':25}{'MESAS':>8}{'TARD':>6}{'%T':>6}"
             f"{'SIN_ACTA':>10}{'%B+N':>8}  CAUSAL")
    for d in distritos:
        u = calcular(d)
        log.info(f"{u.distrito:25}{u.total_mesas:>8}{u.mesas_tardias:>6}"
                 f"{u.pct_tardias:>6.1f}{u.mesas_sin_acta:>10}"
                 f"{u.pct_blanco_nulos:>7.1f}%  {u.causal_propuesta}")


if __name__ == "__main__":
    main()
