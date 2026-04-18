"""Replica exacta del metodo del estadistico asesor (SJM_analysis.ipynb).

Pasos (identicos al notebook):
  1. Limpiar tipos + excluir 'Para envio al JEE'
  2. Dicotomizar en Antes_9am / Despues_9am (hora_decimal <= 9)
  3. Media de ausentismo_pct por grupo
  4. Welch t-test (equal_var=False)
  5. OLS univariado: ausentismo_pct ~ hora_decimal
  6. Contrafactual: exceso de ausentes vs baseline Antes_9am
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd
import statsmodels.api as sm
from scipy.stats import ttest_ind

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

BASE = Path("data/ENTREGA_ESTADISTICO")


def cargar(distrito_dir: str) -> pd.DataFrame:
    csv = BASE / distrito_dir / f"{distrito_dir}_horas_y_votos.csv"
    if not csv.exists():
        raise FileNotFoundError(csv)
    df = pd.read_csv(csv, sep=";")
    df["hora_decimal"] = pd.to_numeric(df["hora_decimal"], errors="coerce")
    df["ausentismo_pct"] = pd.to_numeric(df["ausentismo_pct"], errors="coerce")
    df["asistieron"] = pd.to_numeric(df["asistieron"], errors="coerce").fillna(0).astype(int)
    df["electores_ausentes"] = pd.to_numeric(df["electores_ausentes"], errors="coerce").fillna(0).astype(int)
    df = df[df["estado_acta"] != "Para envío al JEE"].copy()
    df["grupo_hora"] = df["hora_decimal"].apply(lambda x: "Antes_9am" if x <= 9 else "Despues_9am")
    return df.dropna(subset=["hora_decimal", "ausentismo_pct"])


def analisis(df: pd.DataFrame) -> dict:
    medias = df.groupby("grupo_hora")["ausentismo_pct"].mean()
    antes = df[df["grupo_hora"] == "Antes_9am"]["ausentismo_pct"]
    despues = df[df["grupo_hora"] == "Despues_9am"]["ausentismo_pct"]

    # Welch t-test
    if len(antes) > 1 and len(despues) > 1:
        t_stat, p_val = ttest_ind(antes, despues, equal_var=False)
    else:
        t_stat, p_val = float("nan"), float("nan")

    # OLS
    X = sm.add_constant(df["hora_decimal"])
    modelo = sm.OLS(df["ausentismo_pct"], X).fit()

    # Contrafactual
    baseline = medias.get("Antes_9am", df["ausentismo_pct"].mean())
    df2 = df.copy()
    df2["esperados_ausentes"] = (baseline / 100) * df2["electores_ausentes"].astype(float)
    # replica exacta: usa electores_habiles (no ausentes)
    habiles_col = "electores_habiles" if "electores_habiles" in df2.columns else None
    if habiles_col:
        df2["esperados_ausentes"] = (baseline / 100) * df2[habiles_col]
        df2["exceso_ausentes"] = (df2["electores_ausentes"] - df2["esperados_ausentes"]).clip(lower=0)
        tardias = df2[df2["grupo_hora"] == "Despues_9am"]
        afectados = int(round(tardias["exceso_ausentes"].sum()))
    else:
        afectados = None

    return {
        "n_total": len(df),
        "n_antes": len(antes),
        "n_despues": len(despues),
        "media_antes": round(float(medias.get("Antes_9am", float("nan"))), 2),
        "media_despues": round(float(medias.get("Despues_9am", float("nan"))), 2),
        "efecto_pp": round(float(medias.get("Despues_9am", 0) - medias.get("Antes_9am", 0)), 2),
        "t_stat": round(float(t_stat), 4) if pd.notna(t_stat) else None,
        "p_value": float(p_val) if pd.notna(p_val) else None,
        "ols_intercepto": round(float(modelo.params["const"]), 4),
        "ols_pendiente_hora": round(float(modelo.params["hora_decimal"]), 4),
        "ols_p_pendiente": float(modelo.pvalues["hora_decimal"]),
        "ols_r2": round(float(modelo.rsquared), 4),
        "baseline_ausentismo": round(float(baseline), 2),
        "electores_afectados": afectados,
    }


def formato(distrito: str, res: dict) -> str:
    lines = [
        f"=== REPLICA METODO ESTADISTICO — {distrito} ===",
        "",
        f"Muestra: n={res['n_total']}  (antes_9am={res['n_antes']}  despues_9am={res['n_despues']})",
        "",
        "--- Comparacion de medias ---",
        f"  Ausentismo Antes_9am:   {res['media_antes']:>6.2f}%",
        f"  Ausentismo Despues_9am: {res['media_despues']:>6.2f}%",
        f"  Efecto:                 {res['efecto_pp']:+.2f} pp",
        "",
        "--- Welch t-test ---",
        f"  t-stat:  {res['t_stat']}",
        f"  p-value: {res['p_value']:.6g}" if res['p_value'] is not None else "  p-value: n/d",
        "",
        "--- OLS univariado (ausentismo_pct ~ hora_decimal) ---",
        f"  intercepto:    {res['ols_intercepto']}",
        f"  pendiente/hr:  {res['ols_pendiente_hora']} pp",
        f"  p pendiente:   {res['ols_p_pendiente']:.6g}",
        f"  R^2:           {res['ols_r2']}",
        "",
        "--- Contrafactual ---",
        f"  Baseline (Antes_9am): {res['baseline_ausentismo']:.2f}%",
        f"  Electores afectados estimados: {res['electores_afectados']}",
    ]
    return "\n".join(lines)


def distritos_disponibles() -> list[str]:
    return sorted(p.name for p in BASE.iterdir()
                  if p.is_dir() and (p / f"{p.name}_horas_y_votos.csv").exists())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--distrito", help="Nombre carpeta (ej. MIRAFLORES). Si omite, todos.")
    args = ap.parse_args()

    targets = [args.distrito] if args.distrito else distritos_disponibles()
    resumen: list[dict] = []

    for d in targets:
        try:
            df = cargar(d)
            res = analisis(df)
            res["distrito"] = d
            resumen.append(res)
            texto = formato(d, res)
            out = BASE / d / "analisis_replica.txt"
            out.write_text(texto, encoding="utf-8")
            logger.info(texto)
            logger.info("-> %s", out)
        except Exception as e:
            logger.error("%s: %s", d, e)

    if len(resumen) > 1:
        df_r = pd.DataFrame(resumen)[[
            "distrito", "n_total", "n_antes", "n_despues",
            "media_antes", "media_despues", "efecto_pp",
            "p_value", "ols_pendiente_hora", "ols_p_pendiente", "ols_r2",
            "baseline_ausentismo", "electores_afectados",
        ]]
        out = BASE / "RESUMEN_REPLICA_ESTADISTICO.csv"
        df_r.to_csv(out, sep=";", index=False)
        logger.info("\n=== RESUMEN CONSOLIDADO ===")
        logger.info(df_r.to_string(index=False))
        logger.info("-> %s", out)


if __name__ == "__main__":
    main()
