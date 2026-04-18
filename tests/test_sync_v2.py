"""Tests unitarios + integración exporter_v2 + verifier.

Cubre:
- slug() normaliza nombres con acentos / espacios / barras
- sha256_file() hash estable
- _write_ndjson_gz() escribe archivo válido
- export_distrito() → verifier.verify_distrito() round-trip OK
- detecta corrupción (byte flip)
- detecta archivo faltante
- detecta archivo extra no declarado
- detecta manifest corrupto
"""
from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from src.sync import exporter_v2, verifier


# ---------- fixtures ----------

@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    """DB SQLite mínima con 2 distritos para testing."""
    db = tmp_path / "test.db"
    conn = sqlite3.connect(db)
    conn.executescript("""
        CREATE TABLE actas (
            acta_id INTEGER PRIMARY KEY,
            mesa TEXT, distrito TEXT,
            total_electores INTEGER,
            api_response_hash TEXT
        );
        CREATE TABLE votos_por_mesa (
            id INTEGER PRIMARY KEY,
            acta_id INTEGER, partido_nombre TEXT, votos INTEGER
        );
        CREATE TABLE pdfs (
            archivo_id TEXT PRIMARY KEY,
            acta_id INTEGER, mesa TEXT, distrito TEXT, tipo INTEGER,
            nombre_destino TEXT, descargado INTEGER, sha256_hash TEXT
        );
        CREATE TABLE instalaciones (
            mesa TEXT PRIMARY KEY,
            acta_id INTEGER, archivo_id TEXT,
            distrito TEXT, hora_instalacion_min INTEGER
        );
        CREATE TABLE acta_estado_historial (
            id INTEGER PRIMARY KEY,
            acta_id INTEGER, codigo_estado TEXT, descripcion_estado TEXT
        );

        INSERT INTO actas VALUES
            (1, '045001', 'MIRAFLORES', 300, 'hash_a'),
            (2, '045002', 'MIRAFLORES', 280, 'hash_b'),
            (3, '050001', 'LURÍN', 320, 'hash_c');
        INSERT INTO votos_por_mesa (acta_id, partido_nombre, votos) VALUES
            (1, 'FUERZA POPULAR', 120),
            (1, 'RENOVACIÓN POPULAR', 85),
            (2, 'FUERZA POPULAR', 110);
        INSERT INTO pdfs VALUES
            ('f1', 1, '045001', 'MIRAFLORES', 3, '045001_INSTALACION.pdf', 1, 'sha1'),
            ('f2', 2, '045002', 'MIRAFLORES', 3, '045002_INSTALACION.pdf', 1, 'sha2');
        INSERT INTO instalaciones VALUES
            ('045001', 1, 'f1', 'MIRAFLORES', 480);
    """)
    conn.commit()
    conn.close()
    return db


@pytest.fixture
def tmp_export(tmp_path: Path, monkeypatch) -> Path:
    """Redirige EXPORT_DIR a tmp para no ensuciar sync/export real."""
    export_dir = tmp_path / "export"
    export_dir.mkdir()
    monkeypatch.setattr(exporter_v2, "EXPORT_DIR", export_dir)
    monkeypatch.setattr(verifier, "EXPORT_DIR", export_dir)
    return export_dir


# ---------- unit tests ----------

class TestSlug:
    def test_basic(self):
        assert exporter_v2.slug("MIRAFLORES") == "MIRAFLORES"

    def test_with_accents(self):
        assert exporter_v2.slug("LURÍN") == "LURIN"
        assert exporter_v2.slug("PACHACÁMAC") == "PACHACAMAC"
        assert exporter_v2.slug("VILLA MARÍA DEL TRIUNFO") == "VILLA_MARIA_DEL_TRIUNFO"

    def test_with_slash(self):
        assert exporter_v2.slug("A/B") == "A_B"


class TestSha256:
    def test_hash_stable(self, tmp_path: Path):
        p = tmp_path / "f.bin"
        p.write_bytes(b"hello world")
        h1 = exporter_v2.sha256_file(p)
        h2 = exporter_v2.sha256_file(p)
        assert h1 == h2
        assert h1 == hashlib.sha256(b"hello world").hexdigest()

    def test_hash_changes_on_flip(self, tmp_path: Path):
        p = tmp_path / "f.bin"
        p.write_bytes(b"hello world")
        h1 = exporter_v2.sha256_file(p)
        p.write_bytes(b"hello world!")
        h2 = exporter_v2.sha256_file(p)
        assert h1 != h2


class TestNDJsonGz:
    def test_writes_and_reads(self, tmp_path: Path):
        p = tmp_path / "x.ndjson.gz"
        rows = [{"a": 1, "b": "foo"}, {"a": 2, "b": "bar"}]
        n = exporter_v2._write_ndjson_gz(p, rows)
        assert n == 2
        with gzip.open(p, "rt", encoding="utf-8") as f:
            lines = [json.loads(l) for l in f]
        assert lines == rows

    def test_empty_list(self, tmp_path: Path):
        p = tmp_path / "empty.ndjson.gz"
        n = exporter_v2._write_ndjson_gz(p, [])
        assert n == 0
        assert p.exists()  # archivo vacío válido


# ---------- integration tests ----------

class TestExportDistrito:
    def test_creates_files_and_manifest(self, tmp_db: Path, tmp_export: Path):
        conn = sqlite3.connect(tmp_db)
        out = exporter_v2.export_distrito(conn, "MIRAFLORES")
        conn.close()

        assert out.name == "MIRAFLORES"
        assert (out / "manifest.json").exists()
        assert (out / "actas.ndjson.gz").exists()
        assert (out / "votos.ndjson.gz").exists()
        assert (out / "pdfs.ndjson.gz").exists()

        with open(out / "manifest.json", encoding="utf-8") as f:
            m = json.load(f)
        assert m["distrito"] == "MIRAFLORES"
        assert m["schema_version"] == 2
        assert "exported_at" in m
        assert m["archivos"]["actas.ndjson.gz"]["rows"] == 2
        assert m["archivos"]["votos.ndjson.gz"]["rows"] == 3

    def test_handles_accented_distrito(self, tmp_db: Path, tmp_export: Path):
        conn = sqlite3.connect(tmp_db)
        out = exporter_v2.export_distrito(conn, "LURÍN")
        conn.close()
        assert out.name == "LURIN"
        assert (out / "manifest.json").exists()
        with open(out / "manifest.json", encoding="utf-8") as f:
            m = json.load(f)
        assert m["distrito"] == "LURÍN"  # conserva nombre original
        assert m["slug"] == "LURIN"

    def test_only_includes_distrito_rows(self, tmp_db: Path, tmp_export: Path):
        conn = sqlite3.connect(tmp_db)
        out = exporter_v2.export_distrito(conn, "MIRAFLORES")
        conn.close()

        with gzip.open(out / "actas.ndjson.gz", "rt", encoding="utf-8") as f:
            actas = [json.loads(l) for l in f]
        assert len(actas) == 2
        assert all(a["distrito"] == "MIRAFLORES" for a in actas)


class TestVerifierRoundTrip:
    def test_export_then_verify_ok(self, tmp_db: Path, tmp_export: Path):
        conn = sqlite3.connect(tmp_db)
        exporter_v2.export_distrito(conn, "MIRAFLORES")
        conn.close()

        ok, errs = verifier.verify_distrito(tmp_export / "MIRAFLORES")
        assert ok, f"errors: {errs}"
        assert errs == []

    def test_detect_byte_flip_corruption(self, tmp_db: Path, tmp_export: Path):
        conn = sqlite3.connect(tmp_db)
        exporter_v2.export_distrito(conn, "MIRAFLORES")
        conn.close()

        # Corrompe el archivo
        target = tmp_export / "MIRAFLORES" / "actas.ndjson.gz"
        data = target.read_bytes()
        target.write_bytes(data[:-1] + bytes([data[-1] ^ 0xFF]))

        ok, errs = verifier.verify_distrito(tmp_export / "MIRAFLORES")
        assert not ok
        assert any("HASH MISMATCH" in e for e in errs)

    def test_detect_missing_file(self, tmp_db: Path, tmp_export: Path):
        conn = sqlite3.connect(tmp_db)
        exporter_v2.export_distrito(conn, "MIRAFLORES")
        conn.close()

        (tmp_export / "MIRAFLORES" / "actas.ndjson.gz").unlink()
        ok, errs = verifier.verify_distrito(tmp_export / "MIRAFLORES")
        assert not ok
        assert any("FALTA" in e for e in errs)

    def test_detect_extra_file(self, tmp_db: Path, tmp_export: Path):
        conn = sqlite3.connect(tmp_db)
        exporter_v2.export_distrito(conn, "MIRAFLORES")
        conn.close()

        (tmp_export / "MIRAFLORES" / "rogue.txt").write_text("malicious")
        ok, errs = verifier.verify_distrito(tmp_export / "MIRAFLORES")
        assert not ok
        assert any("NO declarados" in e for e in errs)

    def test_detect_manifest_missing(self, tmp_db: Path, tmp_export: Path):
        conn = sqlite3.connect(tmp_db)
        exporter_v2.export_distrito(conn, "MIRAFLORES")
        conn.close()

        (tmp_export / "MIRAFLORES" / "manifest.json").unlink()
        ok, errs = verifier.verify_distrito(tmp_export / "MIRAFLORES")
        assert not ok
        assert any("manifest.json FALTA" in e for e in errs)

    def test_detect_manifest_corrupt(self, tmp_db: Path, tmp_export: Path):
        conn = sqlite3.connect(tmp_db)
        exporter_v2.export_distrito(conn, "MIRAFLORES")
        conn.close()

        (tmp_export / "MIRAFLORES" / "manifest.json").write_text("{invalid json")
        ok, errs = verifier.verify_distrito(tmp_export / "MIRAFLORES")
        assert not ok
        assert any("corrupto" in e for e in errs)


class TestIdempotency:
    def test_second_export_same_hash(self, tmp_db: Path, tmp_export: Path):
        """Dos exports seguidos producen mismos hashes de contenido."""
        conn = sqlite3.connect(tmp_db)
        exporter_v2.export_distrito(conn, "MIRAFLORES")
        with open(tmp_export / "MIRAFLORES" / "manifest.json", encoding="utf-8") as f:
            m1 = json.load(f)
        hashes1 = {k: v["sha256"] for k, v in m1["archivos"].items()}

        exporter_v2.export_distrito(conn, "MIRAFLORES")
        conn.close()
        with open(tmp_export / "MIRAFLORES" / "manifest.json", encoding="utf-8") as f:
            m2 = json.load(f)
        hashes2 = {k: v["sha256"] for k, v in m2["archivos"].items()}

        assert hashes1 == hashes2, "exports idempotentes fallaron"
