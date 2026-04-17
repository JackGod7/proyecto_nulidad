"""Loop que reinicia el scraper fase2 hasta completar todos los PDFs."""
import subprocess
import sys
import time

import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

DISTRITO = sys.argv[1] if len(sys.argv) > 1 else "VILLA EL SALVADOR"
TARGET = int(sys.argv[2]) if len(sys.argv) > 2 else 2514


def count_downloaded() -> int:
    import sqlite3
    conn = sqlite3.connect("data/forensic.db")
    n = conn.execute(
        "SELECT COUNT(*) FROM pdfs WHERE distrito=? AND descargado=1 AND archivo_en_disco=1",
        (DISTRITO,),
    ).fetchone()[0]
    conn.close()
    return n


run = 0
while True:
    current = count_downloaded()
    print(f"\n[LOOP run={run}] {current}/{TARGET} descargados", flush=True)

    if current >= TARGET:
        print("COMPLETADO.", flush=True)
        break

    cmd = [
        "uv", "run", "python", "-c",
        f"from src.scraping.browser_scraper import main; import asyncio; asyncio.run(main(fase=2, workers=1, filtro_distritos=['{DISTRITO}']))"
    ]
    env = {**os.environ, "NODE_OPTIONS": "--max-old-space-size=4096"}

    print(f"Iniciando scraper (pendientes: {TARGET - current})...", flush=True)
    try:
        subprocess.run(cmd, env=env, timeout=600)  # max 10 min por run
    except subprocess.TimeoutExpired:
        print("Timeout 10min — reiniciando proceso...", flush=True)
    except Exception as e:
        print(f"Error: {e}", flush=True)

    after = count_downloaded()
    print(f"Run {run} completado: {current} -> {after} (+{after - current})", flush=True)

    if after == current:
        print("Sin progreso en este run. Esperando 10s y reintentando...", flush=True)
        time.sleep(10)

    run += 1
    time.sleep(3)  # pequeña pausa entre runs

print("FIN.", flush=True)
