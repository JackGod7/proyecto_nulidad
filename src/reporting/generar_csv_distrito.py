"""Genera CSV horas_y_votos por distrito desde forensic.db."""
import json
import sqlite3
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)
ROOT = Path(__file__).parent.parent.parent
DB_FILE = ROOT / "data" / "forensic.db"
ENTREGA = ROOT / "data" / "ENTREGA_ESTADISTICO"

PARTIDOS_COLS = ["Voto_Rafael", "Voto_Nieto", "Voto_Keiko", "Voto_Belmont", "Voto_Roberto"]


def _resolver_distrito(conn: sqlite3.Connection, distrito: str) -> str | None:
    """Busca nombre exacto del distrito en DB (maneja acentos/case)."""
    # Intento exacto
    r = conn.execute("SELECT DISTINCT distrito FROM actas WHERE distrito=?", (distrito,)).fetchone()
    if r:
        return r[0]
    # Intento UPPER
    r = conn.execute("SELECT DISTINCT distrito FROM actas WHERE UPPER(distrito)=UPPER(?)", (distrito,)).fetchone()
    if r:
        return r[0]
    # Intento LIKE con palabras clave
    palabras = distrito.replace("_", " ").split()
    if palabras:
        # Usar la palabra mas larga como filtro
        keyword = max(palabras, key=len)
        r = conn.execute("SELECT DISTINCT distrito FROM actas WHERE distrito LIKE ?", (f"%{keyword}%",)).fetchone()
        if r:
            return r[0]
    return None
# Mapeo flexible: soporta dict {partido: votos} y list [{strNombreElector, intVotos}]
PARTIDOS_MAP_DICT = {
    "RENOVACIÓN POPULAR": "Voto_Rafael",
    "PARTIDO DEL BUEN GOBIERNO": "Voto_Nieto",
    "FUERZA POPULAR": "Voto_Keiko",
    "PARTIDO DEMOCRÁTICO SOMOS PERÚ": "Voto_Belmont",
    "JUNTOS POR EL PERÚ": "Voto_Roberto",
}
PARTIDOS_MAP_LIST = {
    "RAFAEL LOPEZ ALIAGA": "Voto_Rafael",
    "JOSE ANTONIO NIETO MONTESINOS": "Voto_Nieto",
    "KEIKO SOFIA FUJIMORI HIGUCHI": "Voto_Keiko",
    "RICARDO BELMONT CASSINELLI": "Voto_Belmont",
    "ROBERTO SANCHEZ PALOMINO": "Voto_Roberto",
}


def _parse_hora_decimal(hora_min: int | None) -> float | None:
    if hora_min is None:
        return None
    return round(hora_min / 60, 2)


def _tramo_horario(hora_min: int | None) -> str:
    if hora_min is None:
        return ""
    h = hora_min // 60
    return f"{h:02d}:00-{h:02d}:59"


def generar_csv_distrito(distrito: str) -> Path | None:
    """Genera CSV filtrado 7am-12pm para un distrito."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row

    # Resolver nombre exacto del distrito en DB
    distrito_db = _resolver_distrito(conn, distrito)
    if not distrito_db:
        logger.warning("Distrito no encontrado en DB: %s", distrito)
        conn.close()
        return None

    # Instalaciones
    inst = conn.execute(
        "SELECT mesa, hora_instalacion_raw, hora_instalacion_min, "
        "total_electores_habiles, material_buen_estado, observaciones "
        "FROM instalaciones WHERE distrito=? AND hora_instalacion_min IS NOT NULL",
        (distrito_db,)
    ).fetchall()

    if not inst:
        logger.warning("Sin instalaciones para %s", distrito)
        conn.close()
        return None

    # Actas (votos leidos desde votos_por_mesa)
    actas = conn.execute(
        "SELECT acta_id, mesa, local_votacion, estado_acta, total_electores, "
        "total_votantes FROM actas WHERE distrito=?",
        (distrito_db,)
    ).fetchall()
    actas_map = {r["mesa"]: dict(r) for r in actas}

    votos_por_acta: dict[int, dict[str, int]] = {}
    for v in conn.execute(
        "SELECT acta_id, partido_nombre, votos FROM votos_por_mesa "
        "WHERE fuente='api' AND acta_id IN ("
        "  SELECT acta_id FROM actas WHERE distrito=?)",
        (distrito_db,)
    ).fetchall():
        votos_por_acta.setdefault(v["acta_id"], {})[v["partido_nombre"]] = v["votos"] or 0

    rows = []
    for r in inst:
        mesa = r["mesa"]
        acta = actas_map.get(mesa, {})
        hora_min = r["hora_instalacion_min"]
        hora_dec = _parse_hora_decimal(hora_min)

        electores = acta.get("total_electores") or r["total_electores_habiles"] or 0
        votantes = acta.get("total_votantes") or 0
        ausentes = electores - votantes if electores else 0
        aus_pct = round(ausentes / electores * 100, 2) if electores else 0

        # Votos por partido (leidos de tabla normalizada)
        votos_partido = {c: 0 for c in PARTIDOS_COLS}
        acta_id = acta.get("acta_id")
        votos_acta = votos_por_acta.get(acta_id, {})
        for partido, votos_val in votos_acta.items():
            col = PARTIDOS_MAP_DICT.get(partido)
            if col:
                votos_partido[col] = int(votos_val) if votos_val else 0

        rows.append({
            "mesa": str(mesa).zfill(6),
            "distrito": distrito,
            "local_votacion": acta.get("local_votacion", ""),
            "hora_instalacion": r["hora_instalacion_raw"] or "",
            "hora_min": hora_min,
            "hora_decimal": hora_dec,
            "electores_habiles": electores,
            "asistieron": votantes,
            "electores_ausentes": ausentes,
            "ausentismo_pct": aus_pct,
            "tramo_horario": _tramo_horario(hora_min),
            "material_buen_estado": r["material_buen_estado"] or "",
            "observaciones": r["observaciones"] or "",
            "estado_acta": acta.get("estado_acta", ""),
            **votos_partido,
        })

    conn.close()
    df = pd.DataFrame(rows)

    # Separar fuera de horario legal (antes 7am = 420min)
    safe_name = distrito.replace(" ", "_").upper()
    out_dir = ENTREGA / safe_name
    out_dir.mkdir(parents=True, exist_ok=True)

    # Mesas fuera de horario (<7am)
    df_fuera = df[df["hora_min"] < 420].copy()
    if not df_fuera.empty:
        fuera_path = out_dir / f"{safe_name}_mesas_fuera_horario_legal.csv"
        df_fuera.to_csv(fuera_path, sep=";", index=False, encoding="utf-8-sig")
        logger.info("%s: %d mesas fuera horario -> %s", distrito, len(df_fuera), fuera_path.name)

    # Filtrar 7am-12pm (420-720 min)
    df_legal = df[(df["hora_min"] >= 420) & (df["hora_min"] <= 720)].copy()
    csv_path = out_dir / f"{safe_name}_horas_y_votos.csv"
    df_legal.to_csv(csv_path, sep=";", index=False, encoding="utf-8-sig")

    logger.info("%s: %d mesas (de %d) en rango legal -> %s",
                distrito, len(df_legal), len(df), csv_path.name)
    return csv_path


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT UPPER(distrito) FROM instalaciones "
        "WHERE hora_instalacion_min IS NOT NULL ORDER BY UPPER(distrito)"
    )
    distritos = [r[0] for r in cur.fetchall()]
    conn.close()

    logger.info("Distritos con instalaciones: %s", distritos)
    for d in distritos:
        generar_csv_distrito(d)


if __name__ == "__main__":
    main()
