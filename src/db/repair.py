"""Reparación integral de forensic.db: encoding, duplicados, re-import."""
import json
import sqlite3
import logging
import unicodedata
from pathlib import Path

logger = logging.getLogger(__name__)
ROOT = Path(__file__).parent.parent.parent
DB_FILE = ROOT / "data" / "forensic.db"
EXPORT_DIR = ROOT / "sync" / "export"

# Mapeo de distritos con encoding corrupto -> nombre correcto
ENCODING_FIXES = {
    "ANC\ufffdN": "ANCÓN",
    "BRE\ufffdA": "BREÑA",
    "JES\ufffdS MAR\ufffdA": "JESÚS MARÍA",
    "LUR\ufffdN": "LURÍN",
    "PACHAC\ufffdMAC": "PACHACÁMAC",
    "R\ufffdMAC": "RÍMAC",
    "SAN MART\ufffdN DE PORRES": "SAN MARTÍN DE PORRES",
    "SANTA MAR\ufffdA DEL MAR": "SANTA MARÍA DEL MAR",
    "VILLA MAR\ufffdA DEL TRIUNFO": "VILLA MARÍA DEL TRIUNFO",
}

TABLES_WITH_DISTRITO = [
    "actas", "instalaciones", "pdfs", "discrepancias",
    "auditoria_mesa", "auditoria_distrito", "locales_votacion",
]

CASE_FIXES = {
    "Miraflores": "MIRAFLORES",
    "San Juan de Miraflores": "SAN JUAN DE MIRAFLORES",
}


def _detect_corrupted(conn: sqlite3.Connection) -> dict[str, str]:
    """Detecta distritos con encoding corrupto en todas las tablas."""
    fixes = {}
    for table in TABLES_WITH_DISTRITO:
        try:
            rows = conn.execute(f"SELECT DISTINCT distrito FROM {table}").fetchall()
        except sqlite3.OperationalError:
            continue
        for (d,) in rows:
            if not d:
                continue
            # Check for replacement char or known corruptions
            if "\ufffd" in d or "?" in d:
                for bad, good in ENCODING_FIXES.items():
                    if bad == d or d.replace("?", "\ufffd") in ENCODING_FIXES:
                        fixes[d] = good
                        break
            # Check case
            if d in CASE_FIXES:
                fixes[d] = CASE_FIXES[d]
    return fixes


def fix_encoding(conn: sqlite3.Connection) -> int:
    """Normaliza encoding de distritos en todas las tablas."""
    fixed = 0

    # Build complete fix map: auto-detect + known
    all_fixes = {**ENCODING_FIXES, **CASE_FIXES}

    # Also detect dynamically
    detected = _detect_corrupted(conn)
    all_fixes.update(detected)

    for table in TABLES_WITH_DISTRITO:
        try:
            rows = conn.execute(f"SELECT DISTINCT distrito FROM {table}").fetchall()
        except sqlite3.OperationalError:
            continue

        for (d,) in rows:
            if not d:
                continue

            correct = all_fixes.get(d)
            if not correct:
                # Try matching by stripping non-ASCII
                ascii_d = d.encode("ascii", "replace").decode("ascii")
                for bad, good in all_fixes.items():
                    ascii_bad = bad.encode("ascii", "replace").decode("ascii")
                    if ascii_d == ascii_bad:
                        correct = good
                        break

            if correct and correct != d:
                conn.execute(
                    f"UPDATE {table} SET distrito=? WHERE distrito=?",
                    (correct, d)
                )
                count = conn.total_changes
                logger.info("  %s: '%s' -> '%s'", table, d, correct)
                fixed += 1

    conn.commit()
    return fixed


def reimport_exports(conn: sqlite3.Connection) -> dict[str, int]:
    """Re-importa TODOS los exports con INSERT OR REPLACE."""
    if not EXPORT_DIR.exists():
        logger.warning("No existe sync/export/")
        return {}

    totals = {"actas": 0, "instalaciones": 0, "votos": 0, "pdfs": 0}

    for jf in sorted(EXPORT_DIR.glob("*.json")):
        try:
            with open(jf, encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error("Error leyendo %s: %s", jf.name, e)
            continue

        tables = [
            ("actas", "actas"),
            ("votos", "votos_por_mesa"),
            ("pdfs", "pdfs"),
            ("instalaciones", "instalaciones"),
        ]

        for json_key, table_name in tables:
            for row in data.get(json_key, []):
                cols = ", ".join(row.keys())
                placeholders = ", ".join("?" * len(row))
                try:
                    conn.execute(
                        f"INSERT OR REPLACE INTO {table_name} ({cols}) VALUES ({placeholders})",
                        list(row.values())
                    )
                    totals[json_key] = totals.get(json_key, 0) + 1
                except (sqlite3.OperationalError, sqlite3.IntegrityError):
                    pass  # Schema mismatch or constraint, skip

        logger.info("Re-imported: %s", jf.name)

    conn.commit()
    return totals


def verify_integrity(conn: sqlite3.Connection) -> dict:
    """Verificación post-reparación."""
    result = {}

    # Distritos con encoding OK
    for table in ["actas", "instalaciones"]:
        try:
            rows = conn.execute(f"SELECT DISTINCT distrito FROM {table} ORDER BY distrito").fetchall()
            bad = [d for (d,) in rows if d and ("\ufffd" in d or "?" in d)]
            result[f"{table}_encoding_bad"] = bad
        except sqlite3.OperationalError:
            pass

    # Case duplicates
    for table in ["actas", "instalaciones"]:
        try:
            rows = conn.execute(
                f"SELECT distrito, COUNT(*) FROM {table} GROUP BY distrito HAVING distrito != UPPER(distrito)"
            ).fetchall()
            result[f"{table}_case_issues"] = [(d, c) for d, c in rows]
        except sqlite3.OperationalError:
            pass

    # Votos coverage
    rows = conn.execute("""
        SELECT distrito, COUNT(*) as total,
               SUM(CASE WHEN votos_todos_json IS NOT NULL THEN 1 ELSE 0 END) as con_votos
        FROM actas GROUP BY distrito ORDER BY distrito
    """).fetchall()
    result["votos_coverage"] = {d: f"{v}/{t}" for d, t, v in rows}

    # Instalaciones coverage
    rows = conn.execute("""
        SELECT distrito, COUNT(*) as total,
               SUM(CASE WHEN hora_instalacion_min IS NOT NULL THEN 1 ELSE 0 END) as con_hora
        FROM instalaciones GROUP BY distrito ORDER BY distrito
    """).fetchall()
    result["instalaciones_coverage"] = {d: f"{h}/{t}" for d, t, h in rows}

    return result


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    conn = sqlite3.connect(DB_FILE)

    print("=" * 60)
    print("REPARACIÓN INTEGRAL forensic.db")
    print("=" * 60)

    # 1. Fix encoding
    print("\n[1/3] Normalizando encoding de distritos...")
    fixed = fix_encoding(conn)
    print(f"  -> {fixed} correcciones aplicadas")

    # 2. Re-import all exports con REPLACE
    print("\n[2/3] Re-importando exports con INSERT OR REPLACE...")
    totals = reimport_exports(conn)
    for k, v in totals.items():
        print(f"  -> {k}: {v} rows")

    # 3. Fix encoding again (exports may have brought bad encoding)
    print("\n[2.5/3] Re-normalizando encoding post-import...")
    fixed2 = fix_encoding(conn)
    print(f"  -> {fixed2} correcciones adicionales")

    # 4. Verify
    print("\n[3/3] Verificación de integridad...")
    result = verify_integrity(conn)

    bad_enc = result.get("actas_encoding_bad", [])
    if bad_enc:
        print(f"  WARN Encoding malo restante en actas: {bad_enc}")
    else:
        print("  OK Encoding OK en actas")

    bad_enc_i = result.get("instalaciones_encoding_bad", [])
    if bad_enc_i:
        print(f"  WARN Encoding malo restante en instalaciones: {bad_enc_i}")
    else:
        print("  OK Encoding OK en instalaciones")

    case_a = result.get("actas_case_issues", [])
    if case_a:
        print(f"  WARN Case issues en actas: {case_a}")
    else:
        print("  OK Case OK en actas")

    case_i = result.get("instalaciones_case_issues", [])
    if case_i:
        print(f"  WARN Case issues en instalaciones: {case_i}")
    else:
        print("  OK Case OK en instalaciones")

    print("\n--- Votos por distrito ---")
    for d, cov in sorted(result.get("votos_coverage", {}).items()):
        t, v = cov.split("/")
        status = "OK" if t == v else "FALTA"
        print(f"  {status} {d:40s} {cov}")

    print("\n--- Instalaciones con hora ---")
    for d, cov in sorted(result.get("instalaciones_coverage", {}).items()):
        print(f"  {d:40s} {cov}")

    conn.close()
    print("\n" + "=" * 60)
    print("REPARACIÓN COMPLETA")
    print("=" * 60)


if __name__ == "__main__":
    main()
