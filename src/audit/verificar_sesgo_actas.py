"""Verificacion empirica: mesas sin acta vs con acta.

Preguntas a responder:
  P1. ¿Las mesas sin acta tienen votos reportados en el portal?
      (ONPE contabiliza pero no publica el acta)
  P2. ¿Las mesas sin acta tienen hora tardia de instalacion?
      (correlacion entre flag_sin_acta y hora)
  P3. ¿Cual es la diferencia de media de hora_instalacion entre grupos?
      (test de Welch)

Output: tabla por distrito + test estadistico.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
from scipy.stats import ttest_ind

DB_PATH = Path("data/forensic.db")

DISTRITOS = [
    "MIRAFLORES", "SAN JUAN DE MIRAFLORES", "SANTIAGO DE SURCO",
    "VILLA EL SALVADOR", "SAN ISIDRO", "PUCUSANA", "MAGDALENA DEL MAR",
]


def cargar(distrito: str) -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT
            a.mesa,
            a.distrito,
            a.flag_sin_acta,
            a.votos_emitidos,
            a.votos_validos,
            a.total_votantes,
            a.estado_acta,
            i.hora_instalacion_min
        FROM actas a
        LEFT JOIN instalaciones i ON i.acta_id = a.acta_id
        WHERE a.distrito = ? AND a.tiene_datos = 1
    """, conn, params=(distrito,))
    conn.close()
    return df


def analizar(distrito: str) -> dict:
    df = cargar(distrito)
    total = len(df)
    sin_acta = df[df["flag_sin_acta"] == 1]
    con_acta = df[df["flag_sin_acta"] == 0]

    sin_acta_con_votos = sin_acta[sin_acta["votos_emitidos"] > 0]
    sin_acta_sin_votos = sin_acta[sin_acta["votos_emitidos"].isna() | (sin_acta["votos_emitidos"] == 0)]

    sin_hora = sin_acta["hora_instalacion_min"].dropna()
    con_hora = con_acta["hora_instalacion_min"].dropna()

    if len(sin_hora) >= 2 and len(con_hora) >= 2:
        t_stat, p_val = ttest_ind(sin_hora, con_hora, equal_var=False)
    else:
        t_stat, p_val = float("nan"), float("nan")

    return {
        "distrito": distrito,
        "total_mesas": total,
        "con_acta": len(con_acta),
        "sin_acta": len(sin_acta),
        "sin_acta_con_votos_portal": len(sin_acta_con_votos),
        "sin_acta_sin_votos_portal": len(sin_acta_sin_votos),
        "votos_sin_acta_total": int(sin_acta_con_votos["votos_emitidos"].sum() or 0),
        "hora_media_sin_acta_min": round(float(sin_hora.mean()), 1) if len(sin_hora) else None,
        "hora_media_con_acta_min": round(float(con_hora.mean()), 1) if len(con_hora) else None,
        "diferencia_min": (
            round(float(sin_hora.mean() - con_hora.mean()), 1)
            if len(sin_hora) and len(con_hora) else None
        ),
        "t_stat": round(float(t_stat), 3) if pd.notna(t_stat) else None,
        "p_value": float(p_val) if pd.notna(p_val) else None,
        "n_sin_hora_disponible": len(sin_hora),
        "n_con_hora_disponible": len(con_hora),
    }


def main() -> None:
    rows = []
    for d in DISTRITOS:
        rows.append(analizar(d))

    print("\n=== P1. Mesas SIN acta pero CON votos en el portal ===")
    print(f"{'DISTRITO':25}{'TOTAL':>8}{'CON_ACTA':>10}{'SIN_ACTA':>10}"
          f"{'CON_VOTOS':>11}{'VOTOS':>10}")
    print("-" * 75)
    for r in rows:
        print(f"{r['distrito']:25}{r['total_mesas']:>8}{r['con_acta']:>10}"
              f"{r['sin_acta']:>10}{r['sin_acta_con_votos_portal']:>11}"
              f"{r['votos_sin_acta_total']:>10,}")

    print("\n=== P2. Hora instalacion: mesas sin acta vs con acta ===")
    print(f"{'DISTRITO':25}{'HORA_SIN':>10}{'HORA_CON':>10}{'DIF(min)':>10}"
          f"{'T_STAT':>9}{'P_VALUE':>12}{'SIGN':>6}")
    print("-" * 85)
    for r in rows:
        h_sin = f"{r['hora_media_sin_acta_min']:.1f}" if r['hora_media_sin_acta_min'] else "n/d"
        h_con = f"{r['hora_media_con_acta_min']:.1f}" if r['hora_media_con_acta_min'] else "n/d"
        diff = f"{r['diferencia_min']:+.1f}" if r['diferencia_min'] is not None else "n/d"
        t = f"{r['t_stat']:+.3f}" if r['t_stat'] is not None else "n/d"
        p = f"{r['p_value']:.4g}" if r['p_value'] is not None else "n/d"
        sig = "***" if r['p_value'] and r['p_value'] < 0.001 else (
              "**" if r['p_value'] and r['p_value'] < 0.01 else (
              "*" if r['p_value'] and r['p_value'] < 0.05 else ""))
        print(f"{r['distrito']:25}{h_sin:>10}{h_con:>10}{diff:>10}{t:>9}{p:>12}{sig:>6}")

    print("\nInterpretacion:")
    print("  DIF > 0  => mesas sin acta abrieron MAS TARDE (sesgo de seleccion presente)")
    print("  DIF < 0  => mesas sin acta abrieron MAS TEMPRANO")
    print("  p<0.05   => diferencia estadisticamente significativa")
    print("  n/d      => no hay acta de instalacion para esa mesa")

    out = pd.DataFrame(rows)
    out_path = Path("data/ENTREGA_ESTADISTICO/VERIFICACION_SESGO_ACTAS.csv")
    out.to_csv(out_path, sep=";", index=False, encoding="utf-8-sig")
    print(f"\nGuardado: {out_path}")


if __name__ == "__main__":
    main()
