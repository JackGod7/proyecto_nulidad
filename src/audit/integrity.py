"""Integridad de PDFs — SHA-256 hashing y detección de tampering."""
import hashlib
from pathlib import Path


def sha256_file(file_path: Path) -> str:
    """SHA-256 de un archivo en disco."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_bytes(content: bytes) -> str:
    """SHA-256 de bytes en memoria."""
    return hashlib.sha256(content).hexdigest()


def verify_integrity(file_path: Path, expected_hash: str) -> bool:
    """Verifica que un archivo coincida con su hash registrado."""
    if not file_path.exists():
        return False
    return sha256_file(file_path) == expected_hash
