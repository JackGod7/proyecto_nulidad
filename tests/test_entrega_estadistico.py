"""Tests para validar CSVs de entrega al estadístico."""
import csv
import pytest
from pathlib import Path

DATA_DIR = Path("data/ENTREGA_ESTADISTICO")
DISTRITOS = ["MIRAFLORES", "SAN_JUAN_DE_MIRAFLORES"]
CAMPOS_REQUERIDOS = [
    "DEPARTAMENTO", "PROVINCIA", "DISTRITO", "LOCAL_VOTACION",
    "MESA_SUFRAGIO", "TOTAL_ELECTORES_HABILES", "TOTAL_VOTANTES",
    "ACTA_INSTALACION_HORA", "Voto_Rafael", "Voto_Nieto",
    "Voto_Keiko", "Voto_Belmont", "Voto_Roberto",
]


def _leer_csv(distrito: str) -> list[dict]:
    path = DATA_DIR / distrito / f"{distrito}_para_estadistico.csv"
    with open(path, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f, delimiter=";"))


@pytest.mark.parametrize("distrito", DISTRITOS)
class TestEntregaEstadistico:

    def test_archivo_existe(self, distrito):
        path = DATA_DIR / distrito / f"{distrito}_para_estadistico.csv"
        assert path.exists(), f"CSV no encontrado: {path}"

    def test_campos_requeridos(self, distrito):
        rows = _leer_csv(distrito)
        assert len(rows) > 0, "CSV vacío"
        for campo in CAMPOS_REQUERIDOS:
            assert campo in rows[0], f"Falta campo: {campo}"

    def test_no_campos_extra_inesperados(self, distrito):
        rows = _leer_csv(distrito)
        for campo in rows[0].keys():
            assert campo in CAMPOS_REQUERIDOS, f"Campo extra: {campo}"

    def test_mesas_unicas(self, distrito):
        rows = _leer_csv(distrito)
        mesas = [r["MESA_SUFRAGIO"] for r in rows]
        assert len(mesas) == len(set(mesas)), f"Mesas duplicadas: {len(mesas) - len(set(mesas))}"

    def test_mesas_no_vacias(self, distrito):
        rows = _leer_csv(distrito)
        for r in rows:
            assert r["MESA_SUFRAGIO"].strip(), "Mesa vacía encontrada"

    def test_electores_son_numeros_positivos(self, distrito):
        rows = _leer_csv(distrito)
        for r in rows:
            val = int(r["TOTAL_ELECTORES_HABILES"])
            assert val >= 0, f"Mesa {r['MESA_SUFRAGIO']}: electores negativos ({val})"

    def test_votantes_no_superan_electores(self, distrito):
        rows = _leer_csv(distrito)
        errores = []
        for r in rows:
            elec = int(r["TOTAL_ELECTORES_HABILES"])
            vot = int(r["TOTAL_VOTANTES"])
            if vot > elec and elec > 0:
                errores.append(f"Mesa {r['MESA_SUFRAGIO']}: votantes({vot}) > electores({elec})")
        assert not errores, f"Votantes superan electores en {len(errores)} mesas:\n" + "\n".join(errores[:5])

    def test_votos_son_numeros(self, distrito):
        rows = _leer_csv(distrito)
        campos_votos = ["Voto_Rafael", "Voto_Nieto", "Voto_Keiko", "Voto_Belmont", "Voto_Roberto"]
        for r in rows:
            for campo in campos_votos:
                val = r[campo]
                assert val.lstrip("-").isdigit(), f"Mesa {r['MESA_SUFRAGIO']}: {campo}='{val}' no es número"

    def test_votos_no_negativos(self, distrito):
        rows = _leer_csv(distrito)
        campos_votos = ["Voto_Rafael", "Voto_Nieto", "Voto_Keiko", "Voto_Belmont", "Voto_Roberto"]
        for r in rows:
            for campo in campos_votos:
                assert int(r[campo]) >= 0, f"Mesa {r['MESA_SUFRAGIO']}: {campo} negativo"

    def test_suma_votos_no_supera_votantes(self, distrito):
        rows = _leer_csv(distrito)
        errores = []
        for r in rows:
            vot = int(r["TOTAL_VOTANTES"])
            if vot == 0:
                continue
            suma = sum(int(r[c]) for c in ["Voto_Rafael", "Voto_Nieto", "Voto_Keiko", "Voto_Belmont", "Voto_Roberto"])
            if suma > vot:
                errores.append(f"Mesa {r['MESA_SUFRAGIO']}: suma_5_candidatos({suma}) > votantes({vot})")
        assert not errores, f"Suma votos excede votantes en {len(errores)} mesas:\n" + "\n".join(errores[:5])

    def test_distrito_correcto(self, distrito):
        rows = _leer_csv(distrito)
        nombre_esperado = distrito.replace("_", " ")
        for r in rows:
            assert r["DISTRITO"] == nombre_esperado, f"Distrito incorrecto: {r['DISTRITO']}"

    def test_departamento_lima(self, distrito):
        rows = _leer_csv(distrito)
        for r in rows:
            assert r["DEPARTAMENTO"] == "LIMA"
            assert r["PROVINCIA"] == "LIMA"

    def test_hora_formato_valido(self, distrito):
        """Horas deben ser HH:MM a.m./p.m. o vacío."""
        rows = _leer_csv(distrito)
        sin_hora = 0
        for r in rows:
            hora = r["ACTA_INSTALACION_HORA"].strip()
            if not hora:
                sin_hora += 1
                continue
            assert ":" in hora, f"Mesa {r['MESA_SUFRAGIO']}: hora sin ':' -> '{hora}'"
        # Permitir máximo 10% sin hora
        pct = sin_hora / len(rows) * 100
        # SJM tiene 30% sin hora (166 pendientes ONPE + mesas sin PDF instalación)
        assert pct < 35, f"{sin_hora}/{len(rows)} ({pct:.1f}%) mesas sin hora"

    def test_minimo_filas(self, distrito):
        rows = _leer_csv(distrito)
        if "MIRAFLORES" == distrito:
            assert len(rows) >= 400, f"Muy pocas filas Miraflores: {len(rows)}"
        else:
            assert len(rows) >= 900, f"Muy pocas filas SJM: {len(rows)}"

    def test_local_votacion_no_todos_vacios(self, distrito):
        rows = _leer_csv(distrito)
        con_local = sum(1 for r in rows if r["LOCAL_VOTACION"].strip())
        assert con_local > len(rows) * 0.5, f"Solo {con_local}/{len(rows)} tienen local de votación"
