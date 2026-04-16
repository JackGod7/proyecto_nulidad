"""CLI para consultar progreso del scraping."""
import argparse
import json

from src.progress_db import (
    actas_fallidas,
    distritos_pendientes,
    init_db,
    pdfs_pendientes,
    resumen_progreso,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Progreso scraping ONPE")
    parser.add_argument("--pendientes", action="store_true", help="Distritos pendientes")
    parser.add_argument("--errores", action="store_true", help="Actas con error")
    parser.add_argument("--pdfs", action="store_true", help="PDFs no descargados")
    parser.add_argument("--distrito", type=str, help="Filtrar por distrito")
    args = parser.parse_args()

    init_db()

    if args.pendientes:
        pend = distritos_pendientes()
        for d in pend:
            print(f"  {d['nombre']:20s} [{d['estado']}] {d.get('error', '')}")
        print(f"\nTotal pendientes: {len(pend)}")
        return

    if args.errores:
        errs = actas_fallidas()
        for e in errs:
            print(f"  Mesa {e['mesa']} ({e['distrito']}): {e['error']}")
        print(f"\nTotal errores: {len(errs)}")
        return

    if args.pdfs:
        pend = pdfs_pendientes(args.distrito)
        for p in pend:
            print(f"  {p['distrito']}/{p['nombre_destino']}")
        print(f"\nTotal PDFs pendientes: {len(pend)}")
        return

    # Default: resumen general
    r = resumen_progreso()
    print(json.dumps(r, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
