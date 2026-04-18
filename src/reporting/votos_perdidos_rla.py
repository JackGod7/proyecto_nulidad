"""Estimacion de votos perdidos por candidato (foco: Rafael Lopez Aliaga).

Metodo contrafactual: si una mesa tardia hubiera abierto a las 9:00 a.m.
habrian asistido mas electores. Ese exceso de ausentes se convierte en
votos adicionales usando alguna regla de asignacion (% de voto).

Tres metodologias:
  A) Proporcion de la mesa tardia           (asume votantes ausentes votarian
                                              igual que los que si votaron en
                                              esa misma mesa)
  B) Proporcion baseline del distrito       (usa %voto promedio en mesas
                                              oportunas del distrito)
  C) Proporcion del mismo local de votacion (usa %voto promedio en mesas
                                              oportunas del mismo local, si
                                              existen; fallback a B)

Para cada metodo se calcula: votos_perdidos_estimados por candidato en
cada mesa, consolidado por distrito, y total agregado.

Candidatos analizados: Rafael Lopez Aliaga (principal), Keiko Fujimori,
Rafael Belmont, Roberto Chavin Castillo, Nieto (segun columnas CSV).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

BASE_ENTREGA = Path("data/ENTREGA_ESTADISTICO")
OUT_DIR = BASE_ENTREGA / "VOTOS_PERDIDOS_RLA"

CANDIDATOS = {
    "Rafael_Lopez_Aliaga": "Voto_Rafael",
    "Nieto": "Voto_Nieto",
    "Keiko": "Voto_Keiko",
    "Belmont": "Voto_Belmont",
    "Roberto": "Voto_Roberto",
}

UMBRAL_HORA = 9.0


@dataclass(frozen=True)
class ResumenDistrito:
    distrito: str
    n_mesas_total: int
    n_mesas_tardias: int
    baseline_ausentismo_pct: float
    exceso_ausentes_total: int
    rla_perdidos_A: int
    rla_perdidos_B: int
    rla_perdidos_C: int
    todos_perdidos_A: dict
    todos_perdidos_B: dict


def _cargar(distrito_dir: str) -> pd.DataFrame:
    csv = BASE_ENTREGA / distrito_dir / f"{distrito_dir}_horas_y_votos.csv"
    df = pd.read_csv(csv, sep=";")
    num_cols = ["hora_decimal", "ausentismo_pct", "electores_habiles",
                "asistieron", "electores_ausentes"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in CANDIDATOS.values():
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    df = df[df["estado_acta"] != "Para envío al JEE"].copy()
    df = df.dropna(subset=["hora_decimal", "ausentismo_pct", "electores_habiles"])
    df["votos_validos_mesa"] = df[list(CANDIDATOS.values())].sum(axis=1)
    df["grupo_hora"] = df["hora_decimal"].apply(
        lambda x: "oportuna" if x <= UMBRAL_HORA else "tardia"
    )
    return df


def _baseline_ausentismo(df: pd.DataFrame) -> float:
    opor = df[df["grupo_hora"] == "oportuna"]
    if len(opor) == 0:
        return float(df["ausentismo_pct"].mean())
    return float(opor["ausentismo_pct"].mean())


def _pct_distrital(df: pd.DataFrame, col_voto: str) -> float:
    opor = df[df["grupo_hora"] == "oportuna"]
    base = opor if len(opor) > 0 else df
    total_validos = base["votos_validos_mesa"].sum()
    if total_validos == 0:
        return 0.0
    return float(base[col_voto].sum() / total_validos)


def _pct_local(df: pd.DataFrame, col_voto: str, local: str, fallback: float) -> float:
    opor_local = df[(df["grupo_hora"] == "oportuna") & (df["local_votacion"] == local)]
    if len(opor_local) == 0:
        return fallback
    total_validos = opor_local["votos_validos_mesa"].sum()
    if total_validos == 0:
        return fallback
    return float(opor_local[col_voto].sum() / total_validos)


def estimar_distrito(distrito: str, distrito_dir: str) -> tuple[pd.DataFrame, ResumenDistrito]:
    df = _cargar(distrito_dir)

    baseline = _baseline_ausentismo(df)
    df["ausentes_esperados"] = (baseline / 100) * df["electores_habiles"]
    df["exceso_ausentes"] = (df["electores_ausentes"] - df["ausentes_esperados"]).clip(lower=0).round().astype(int)

    tardias = df[df["grupo_hora"] == "tardia"].copy()

    for nombre, col in CANDIDATOS.items():
        tardias[f"pct_mesa_{nombre}"] = tardias.apply(
            lambda r, c=col: (r[c] / r["votos_validos_mesa"]) if r["votos_validos_mesa"] > 0 else 0.0,
            axis=1
        )
        tardias[f"perdidos_A_{nombre}"] = (
            tardias["exceso_ausentes"] * tardias[f"pct_mesa_{nombre}"]
        ).round().astype(int)

        pct_dist = _pct_distrital(df, col)
        tardias[f"perdidos_B_{nombre}"] = (
            tardias["exceso_ausentes"] * pct_dist
        ).round().astype(int)

        def fill_c(r, cc=col, pdist=pct_dist):
            p = _pct_local(df, cc, r["local_votacion"], pdist)
            return int(round(r["exceso_ausentes"] * p))
        tardias[f"perdidos_C_{nombre}"] = tardias.apply(fill_c, axis=1)

    cols_out = [
        "mesa", "distrito", "local_votacion", "hora_instalacion", "hora_decimal",
        "electores_habiles", "asistieron", "electores_ausentes",
        "ausentismo_pct", "ausentes_esperados", "exceso_ausentes",
        "votos_validos_mesa",
    ]
    for col_voto in CANDIDATOS.values():
        cols_out.append(col_voto)
    for nombre in CANDIDATOS:
        cols_out += [
            f"pct_mesa_{nombre}",
            f"perdidos_A_{nombre}",
            f"perdidos_B_{nombre}",
            f"perdidos_C_{nombre}",
        ]
    detalle = tardias[cols_out].copy()
    detalle["ausentes_esperados"] = detalle["ausentes_esperados"].round().astype(int)

    todos_A = {n: int(tardias[f"perdidos_A_{n}"].sum()) for n in CANDIDATOS}
    todos_B = {n: int(tardias[f"perdidos_B_{n}"].sum()) for n in CANDIDATOS}

    resumen = ResumenDistrito(
        distrito=distrito,
        n_mesas_total=len(df),
        n_mesas_tardias=len(tardias),
        baseline_ausentismo_pct=round(baseline, 2),
        exceso_ausentes_total=int(tardias["exceso_ausentes"].sum()),
        rla_perdidos_A=int(tardias["perdidos_A_Rafael_Lopez_Aliaga"].sum()),
        rla_perdidos_B=int(tardias["perdidos_B_Rafael_Lopez_Aliaga"].sum()),
        rla_perdidos_C=int(tardias["perdidos_C_Rafael_Lopez_Aliaga"].sum()),
        todos_perdidos_A=todos_A,
        todos_perdidos_B=todos_B,
    )
    return detalle, resumen


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    mapping = [
        ("MIRAFLORES", "MIRAFLORES"),
        ("SAN JUAN DE MIRAFLORES", "SAN_JUAN_DE_MIRAFLORES"),
        ("SANTIAGO DE SURCO", "SANTIAGO_DE_SURCO"),
        ("VILLA EL SALVADOR", "VILLA_EL_SALVADOR"),
        ("SAN ISIDRO", "SAN_ISIDRO"),
        ("PUCUSANA", "PUCUSANA"),
        ("MAGDALENA DEL MAR", "MAGDALENA_DEL_MAR"),
    ]

    resumenes = []
    for distrito, dir_ in mapping:
        try:
            detalle, r = estimar_distrito(distrito, dir_)
            out_csv = OUT_DIR / f"{dir_}_votos_perdidos.csv"
            detalle.to_csv(out_csv, sep=";", index=False, encoding="utf-8-sig")
            resumenes.append(r)
        except FileNotFoundError as e:
            log.warning("Saltando %s (sin CSV): %s", distrito, e)
        except Exception as e:
            log.error("FALLO %s: %s", distrito, e)

    if not resumenes:
        log.error("Sin resumenes.")
        return

    cols = [
        "distrito", "n_mesas_total", "n_mesas_tardias",
        "baseline_ausentismo_pct", "exceso_ausentes_total",
        "rla_perdidos_A", "rla_perdidos_B", "rla_perdidos_C",
    ]
    rows = []
    for r in resumenes:
        rows.append([
            r.distrito, r.n_mesas_total, r.n_mesas_tardias,
            r.baseline_ausentismo_pct, r.exceso_ausentes_total,
            r.rla_perdidos_A, r.rla_perdidos_B, r.rla_perdidos_C,
        ])
    res_df = pd.DataFrame(rows, columns=cols)
    total_row = pd.DataFrame([[
        "TOTAL_7_DISTRITOS", int(res_df["n_mesas_total"].sum()),
        int(res_df["n_mesas_tardias"].sum()),
        round(float(res_df["baseline_ausentismo_pct"].mean()), 2),
        int(res_df["exceso_ausentes_total"].sum()),
        int(res_df["rla_perdidos_A"].sum()),
        int(res_df["rla_perdidos_B"].sum()),
        int(res_df["rla_perdidos_C"].sum()),
    ]], columns=cols)
    res_df = pd.concat([res_df, total_row], ignore_index=True)
    res_df.to_csv(OUT_DIR / "RESUMEN_VOTOS_PERDIDOS_RLA.csv",
                  sep=";", index=False, encoding="utf-8-sig")

    rows_all = []
    for r in resumenes:
        for cand in CANDIDATOS:
            rows_all.append([r.distrito, cand, r.todos_perdidos_A[cand], r.todos_perdidos_B[cand]])
    all_df = pd.DataFrame(rows_all, columns=["distrito", "candidato", "perdidos_A", "perdidos_B"])
    all_df.to_csv(OUT_DIR / "RESUMEN_TODOS_CANDIDATOS.csv",
                  sep=";", index=False, encoding="utf-8-sig")

    log.info("")
    log.info("%-22s %6s %6s %8s %10s %10s %10s", "DISTRITO", "MESAS",
             "TARD", "BASE%", "RLA_A", "RLA_B", "RLA_C")
    log.info("-" * 80)
    for r in resumenes:
        log.info("%-22s %6d %6d %7.2f%% %10d %10d %10d",
                 r.distrito, r.n_mesas_total, r.n_mesas_tardias,
                 r.baseline_ausentismo_pct,
                 r.rla_perdidos_A, r.rla_perdidos_B, r.rla_perdidos_C)
    log.info("-" * 80)
    total_a = sum(r.rla_perdidos_A for r in resumenes)
    total_b = sum(r.rla_perdidos_B for r in resumenes)
    total_c = sum(r.rla_perdidos_C for r in resumenes)
    log.info("%-22s %6s %6s %8s %10d %10d %10d",
             "TOTAL", "", "", "", total_a, total_b, total_c)
    log.info("")
    log.info("Output: %s", OUT_DIR)


if __name__ == "__main__":
    main()
