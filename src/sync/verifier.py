"""Verifier — valida manifest.json y SHA-256 de NDJSON+gzip por distrito.

Uso:
    python -m src.sync.verifier                        # verifica TODOS los distritos
    python -m src.sync.verifier VILLA_MARÍA_DEL_TRIUNFO
    python -m src.sync.verifier --changed              # solo los modificados en git diff

Exit codes:
    0  OK
    1  Error de verificación (hash mismatch, manifest corrupto, archivo falta)
    2  Error de uso (args, IO)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger(__name__)
ROOT = Path(__file__).parent.parent.parent
EXPORT_DIR = ROOT / "sync" / "export"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def verify_distrito(dir_: Path) -> tuple[bool, list[str]]:
    """Verifica un directorio de distrito. Retorna (ok, errores)."""
    errors: list[str] = []
    manifest_path = dir_ / "manifest.json"
    if not manifest_path.exists():
        return False, [f"{dir_.name}: manifest.json FALTA"]

    try:
        with open(manifest_path, encoding="utf-8") as f:
            manifest = json.load(f)
    except Exception as e:
        return False, [f"{dir_.name}: manifest.json corrupto: {e}"]

    archivos = manifest.get("archivos", {})
    if not archivos:
        errors.append(f"{dir_.name}: manifest sin archivos declarados")

    for nombre, meta in archivos.items():
        path = dir_ / nombre
        if not path.exists():
            errors.append(f"{dir_.name}/{nombre}: FALTA en disco")
            continue
        actual_hash = sha256_file(path)
        expected = meta.get("sha256")
        if actual_hash != expected:
            errors.append(f"{dir_.name}/{nombre}: HASH MISMATCH (esp={expected[:12]}.. got={actual_hash[:12]}..)")
        actual_bytes = path.stat().st_size
        if meta.get("bytes") and actual_bytes != meta["bytes"]:
            errors.append(f"{dir_.name}/{nombre}: TAMAÑO DIFIERE (esp={meta['bytes']} got={actual_bytes})")

    # Detectar archivos extra no declarados
    declarados = set(archivos.keys()) | {"manifest.json"}
    en_disco = {p.name for p in dir_.iterdir() if p.is_file()}
    extra = en_disco - declarados
    if extra:
        errors.append(f"{dir_.name}: archivos NO declarados en manifest: {sorted(extra)}")

    return (len(errors) == 0), errors


def verify_all(subset: list[str] | None = None) -> tuple[int, int, list[str]]:
    """Verifica todos los distritos en sync/export (o subconjunto)."""
    if not EXPORT_DIR.exists():
        return 0, 0, ["sync/export/ no existe"]

    dirs = [d for d in EXPORT_DIR.iterdir() if d.is_dir()]
    if subset:
        dirs = [d for d in dirs if d.name in subset]

    ok = 0
    fail = 0
    all_errors: list[str] = []
    for d in sorted(dirs):
        passed, errs = verify_distrito(d)
        if passed:
            ok += 1
            logger.info("OK %s", d.name)
        else:
            fail += 1
            for e in errs:
                all_errors.append(e)
                logger.error(e)
    return ok, fail, all_errors


def _changed_dirs_from_git() -> list[str]:
    """Directorios de sync/export/ modificados según git (staged + unstaged)."""
    try:
        out = subprocess.check_output(
            ["git", "diff", "--name-only", "HEAD", "--", "sync/export/"],
            cwd=ROOT, text=True, stderr=subprocess.DEVNULL,
        )
        staged = subprocess.check_output(
            ["git", "diff", "--name-only", "--cached", "--", "sync/export/"],
            cwd=ROOT, text=True, stderr=subprocess.DEVNULL,
        )
        untracked = subprocess.check_output(
            ["git", "ls-files", "--others", "--exclude-standard", "--", "sync/export/"],
            cwd=ROOT, text=True, stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        return []

    dirs = set()
    for path in (out + staged + untracked).splitlines():
        path = path.strip()
        if not path:
            continue
        # sync/export/{DISTRITO}/archivo
        parts = path.split("/")
        if len(parts) >= 3 and parts[0] == "sync" and parts[1] == "export":
            dirs.add(parts[2])
    return sorted(dirs)


def main() -> int:
    parser = argparse.ArgumentParser(description="Verifica integridad manifest+hashes sync/export")
    parser.add_argument("distrito", nargs="?", help="Slug distrito específico (opcional)")
    parser.add_argument("--changed", action="store_true", help="Solo distritos modificados en git")
    parser.add_argument("--quiet", action="store_true", help="Silencia OKs")
    args = parser.parse_args()

    level = logging.WARNING if args.quiet else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s %(message)s")

    subset: list[str] | None = None
    if args.distrito:
        subset = [args.distrito]
    elif args.changed:
        subset = _changed_dirs_from_git()
        if not subset:
            logger.info("No hay distritos modificados")
            return 0

    ok, fail, errors = verify_all(subset)
    print(f"\nVerificados: {ok} OK, {fail} FALLAS", file=sys.stderr)
    if fail:
        print(f"\nERRORES:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
