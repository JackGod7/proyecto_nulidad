"""Tests para el extractor v2 de datos ONPE (ALL partidos)."""
from src.extraction.extractor import (
    extraer_archivos,
    extraer_fila_completa,
    extraer_todos_los_votos,
    extraer_votos_normalizados,
)


DETALLE_SAMPLE = [
    {
        "descripcion": "FUERZA POPULAR",
        "ccodigo": "FP",
        "nvotos": 35,
        "candidato": [{"apellidoPaterno": "FUJIMORI", "apellidoMaterno": "HIGUCHI", "nombres": "KEIKO", "cdocumentoIdentidad": "1"}],
    },
    {
        "descripcion": "RENOVACIÓN POPULAR",
        "ccodigo": "RP",
        "nvotos": 15,
        "candidato": [{"apellidoPaterno": "LÓPEZ ALIAGA", "apellidoMaterno": "CAZORLA", "nombres": "RAFAEL", "cdocumentoIdentidad": "2"}],
    },
    {
        "descripcion": "PARTIDO DEL BUEN GOBIERNO",
        "ccodigo": "PBG",
        "nvotos": 14,
        "candidato": [{"apellidoPaterno": "NIETO", "apellidoMaterno": "MONTESINOS", "nombres": "JORGE", "cdocumentoIdentidad": "3"}],
    },
    {"descripcion": "VOTOS EN BLANCO", "nvotos": 25, "candidato": []},
    {"descripcion": "VOTOS NULOS", "nvotos": 9, "candidato": []},
    {"descripcion": "VOTOS IMPUGNADOS", "nvotos": 2, "candidato": []},
]


def test_extraer_todos_los_votos():
    votos = extraer_todos_los_votos(DETALLE_SAMPLE)
    assert votos["blanco"] == 25
    assert votos["nulos"] == 9
    assert votos["impugnados"] == 2
    assert votos["partidos"]["FUERZA POPULAR"] == 35
    assert votos["partidos"]["RENOVACIÓN POPULAR"] == 15
    assert "VOTOS EN BLANCO" not in votos["partidos"]


def test_extraer_votos_normalizados():
    rows = extraer_votos_normalizados(DETALLE_SAMPLE)
    # Excluye blanco/nulos/impugnados
    assert len(rows) == 3
    assert rows[0]["partido_nombre"] == "FUERZA POPULAR"
    assert rows[0]["partido_codigo"] == "FP"
    assert rows[0]["votos"] == 35
    assert "FUJIMORI" in rows[0]["candidato_nombre"]


def test_extraer_fila_completa():
    api_response = {
        "data": {
            "codigoMesa": "036999",
            "ubigeoNivel01": "LIMA",
            "ubigeoNivel02": "LIMA",
            "ubigeoNivel03": "ANCÓN",
            "nombreLocalVotacion": "IE 3069",
            "totalElectoresHabiles": 230,
            "totalAsistentes": 174,
            "totalVotosEmitidos": 174,
            "totalVotosValidos": 140,
            "porcentajeParticipacionCiudadana": 75.652,
            "descripcionEstadoActa": "Contabilizada",
            "descripcionSolucionTecnologica": "STAE",
            "detalle": DETALLE_SAMPLE,
            "archivos": [
                {"id": "abc", "tipo": 1, "nombre": "a.pdf", "descripcion": "ESCRUTINIO"},
                {"id": "def", "tipo": 3, "nombre": "b.pdf", "descripcion": "INSTALACIÓN"},
            ],
        }
    }
    fila = extraer_fila_completa(api_response)
    assert fila["mesa"] == "036999"
    assert fila["distrito"] == "ANCÓN"
    assert fila["votos_blanco"] == 25
    assert fila["votos_nulos"] == 9
    assert fila["votos_impugnados"] == 2
    assert fila["tiene_pdf_escrutinio"] == 1
    assert fila["tiene_pdf_instalacion"] == 1
    assert fila["tiene_pdf_sufragio"] == 0
    assert fila["tiene_datos"] == 1
    assert "api_response_hash" in fila
    assert len(fila["api_response_hash"]) == 64  # SHA-256 hex
    assert fila["partidos_detalle"]["FUERZA POPULAR"] == 35


def test_extraer_archivos():
    acta = {
        "codigoMesa": "036999",
        "archivos": [
            {"id": "abc", "tipo": 1, "nombre": "uuid1.pdf", "descripcion": "ACTA DE ESCRUTINIO"},
            {"id": "def", "tipo": 3, "nombre": "uuid2.pdf", "descripcion": "ACTA DE INSTALACIÓN"},
            {"id": "ghi", "tipo": 4, "nombre": "uuid3.pdf", "descripcion": "ACTA DE SUFRAGIO"},
        ],
    }
    archivos = extraer_archivos(acta)
    assert len(archivos) == 3
    assert archivos[0]["nombre_destino"] == "036999_ESCRUTINIO.pdf"
    assert archivos[1]["nombre_destino"] == "036999_INSTALACION.pdf"
    assert archivos[2]["nombre_destino"] == "036999_SUFRAGIO.pdf"


def test_extraer_archivos_vacio():
    acta = {"codigoMesa": "037000", "archivos": []}
    assert extraer_archivos(acta) == []


def test_api_response_hash_deterministico():
    """Mismo input → mismo hash (idempotencia forense)."""
    api = {"data": {"codigoMesa": "001", "detalle": DETALLE_SAMPLE, "archivos": []}}
    h1 = extraer_fila_completa(api)["api_response_hash"]
    h2 = extraer_fila_completa(api)["api_response_hash"]
    assert h1 == h2


def test_api_response_hash_cambia_con_input():
    api1 = {"data": {"codigoMesa": "001", "detalle": DETALLE_SAMPLE, "archivos": []}}
    api2 = {"data": {"codigoMesa": "002", "detalle": DETALLE_SAMPLE, "archivos": []}}
    h1 = extraer_fila_completa(api1)["api_response_hash"]
    h2 = extraer_fila_completa(api2)["api_response_hash"]
    assert h1 != h2
