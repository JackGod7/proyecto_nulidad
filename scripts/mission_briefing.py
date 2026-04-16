#!/usr/bin/env python3
"""SessionStart hook - muestra mision al abrir el proyecto en Claude Code."""
import json
import sqlite3
import sys
import io
from pathlib import Path

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).parent.parent
CONFIG_FILE = ROOT / "machine_config.json"
DB_FILE = ROOT / "forensic.db"

SEPARATOR = "=" * 60


def load_config() -> dict:
    if not CONFIG_FILE.exists():
        return {}
    with open(CONFIG_FILE, encoding="utf-8") as f:
        return json.load(f)


def get_progress(distritos: list[str]) -> dict[str, dict]:
    """Returns per-district progress from local DB."""
    if not DB_FILE.exists():
        return {}
    try:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        progress = {}
        for d in distritos:
            cur.execute(
                "SELECT COUNT(*), SUM(CASE WHEN estado='completada' THEN 1 ELSE 0 END) "
                "FROM actas WHERE distrito=?", (d,)
            )
            row = cur.fetchone()
            total, done = (row[0] or 0), (row[1] or 0)
            progress[d] = {"total": total, "done": done}
        conn.close()
        return progress
    except Exception:
        return {}


def get_extraction_progress(distritos: list[str]) -> tuple[int, int]:
    """Returns (extracted, total_pdfs) for assigned districts."""
    if not DB_FILE.exists():
        return 0, 0
    try:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        placeholders = ",".join("?" * len(distritos))
        cur.execute(
            f"SELECT COUNT(*), SUM(CASE WHEN hora_instalacion IS NOT NULL THEN 1 ELSE 0 END) "
            f"FROM pdfs p JOIN actas a ON a.id=p.acta_id "
            f"WHERE a.distrito IN ({placeholders}) AND p.tipo_acta='INSTALACION'",
            distritos
        )
        row = cur.fetchone()
        conn.close()
        return (row[1] or 0), (row[0] or 0)
    except Exception:
        return 0, 0


def check_sync_ready() -> tuple[int, int]:
    """Returns (files_ready, files_pending) in sync/export/."""
    export_dir = ROOT / "sync" / "export"
    if not export_dir.exists():
        return 0, 0
    files = list(export_dir.glob("*.json"))
    ready = sum(1 for f in files if not f.name.endswith("_pending.json"))
    return ready, len(files) - ready


def print_briefing(config: dict) -> None:
    machine_id = config.get("machine_id", "SIN CONFIGURAR")
    distritos = config.get("distritos", [])
    rol = config.get("rol", "worker")

    print(SEPARATOR)
    print(f"  PROYECTO NULIDAD — AUDITORIA FORENSE ONPE 2026")
    print(f"  Maquina : {machine_id}  |  Rol: {rol.upper()}")
    print(SEPARATOR)

    if not distritos:
        print()
        print("  ⚠  NO CONFIGURADO")
        print("  Copia machine_config.example.json → machine_config.json")
        print("  Edita machine_id y distritos asignados")
        print()
        print(SEPARATOR)
        return

    print(f"\n  MISION: {len(distritos)} distritos asignados")
    print()

    # --- Fase 1: scraping ---
    progress = get_progress(distritos)
    total_actas = sum(v["total"] for v in progress.values())
    done_actas = sum(v["done"] for v in progress.values())
    pct_scraping = int(done_actas / total_actas * 100) if total_actas else 0

    print(f"  FASE 1 — Scraping actas")
    print(f"  {done_actas}/{total_actas} actas completadas ({pct_scraping}%)")

    pending = [d for d in distritos if progress.get(d, {}).get("total", 0) == 0]
    in_progress = [
        d for d in distritos
        if progress.get(d, {}).get("total", 0) > 0
        and progress.get(d, {}).get("done", 0) < progress.get(d, {}).get("total", 0)
    ]
    if in_progress:
        print(f"  En progreso : {', '.join(in_progress)}")
    if pending:
        print(f"  Pendientes  : {', '.join(pending[:5])}{'...' if len(pending) > 5 else ''}")

    # --- Fase 2: extracción entidades ---
    extracted, total_pdfs = get_extraction_progress(distritos)
    pct_ext = int(extracted / total_pdfs * 100) if total_pdfs else 0
    print()
    print(f"  FASE 2 — Extraccion actas instalacion (Gemini)")
    print(f"  {extracted}/{total_pdfs} PDFs extraidos ({pct_ext}%)")

    # --- Fase 3: sync ---
    ready, pending_sync = check_sync_ready()
    print()
    print(f"  FASE 3 — Sync export")
    print(f"  {ready} archivos listos para sincronizar")
    if pending_sync:
        print(f"  {pending_sync} pendientes de exportar")

    # --- Siguiente accion ---
    print()
    print("  SIGUIENTE ACCION:")
    if pending:
        next_dist = pending[0]
        print(f"  → Scraping: uv run python -c \"from src.scraping.browser_scraper import main; import asyncio; asyncio.run(main(fase=1, workers=5, filtro_distritos=['{next_dist}']))\"")
    elif in_progress:
        next_dist = in_progress[0]
        print(f"  → Continuar scraping: {next_dist}")
        print(f"  → Luego PDFs: fase=2")
    elif total_pdfs > extracted:
        print(f"  → Extraccion Gemini: uv run python src/extraction/instalacion_extractor.py")
    elif ready > 0:
        print(f"  → Exportar sync: uv run python src/sync/exporter.py")
    else:
        print(f"  ✓ TODO COMPLETADO — ejecuta sync/exporter.py")

    print()
    print(SEPARATOR)


def main() -> None:
    config = load_config()
    if not config:
        print(SEPARATOR)
        print("  PROYECTO NULIDAD — sin machine_config.json")
        print("  Copia machine_config.example.json → machine_config.json")
        print(SEPARATOR)
        return
    print_briefing(config)


if __name__ == "__main__":
    main()
