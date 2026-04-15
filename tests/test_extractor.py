"""Tests para el extractor de datos ONPE."""
from src.extractor import extraer_archivos, extraer_fila_mesa, extraer_votos_top5


DETALLE_SAMPLE = [
    {
        "descripcion": "FUERZA POPULAR",
        "nvotos": 35,
        "candidato": [{"apellidoPaterno": "FUJIMORI", "apellidoMaterno": "HIGUCHI", "nombres": "KEIKO"}],
    },
    {
        "descripcion": "RENOVACIÓN POPULAR",
        "nvotos": 15,
        "candidato": [{"apellidoPaterno": "LÓPEZ ALIAGA", "apellidoMaterno": "CAZORLA", "nombres": "RAFAEL"}],
    },
    {
        "descripcion": "PARTIDO DEL BUEN GOBIERNO",
        "nvotos": 14,
        "candidato": [{"apellidoPaterno": "NIETO", "apellidoMaterno": "MONTESINOS", "nombres": "JORGE"}],
    },
    {
        "descripcion": "PARTIDO CÍVICO OBRAS",
        "nvotos": 17,
        "candidato": [{"apellidoPaterno": "BELMONT", "apellidoMaterno": "CASSINELLI", "nombres": "RICARDO"}],
    },
    {
        "descripcion": "JUNTOS POR EL PERÚ",
        "nvotos": 10,
        "candidato": [{"apellidoPaterno": "SÁNCHEZ PALOMINO", "apellidoMaterno": "", "nombres": "ROBERTO"}],
    },
    {"descripcion": "VOTOS EN BLANCO", "nvotos": 25, "candidato": []},
    {"descripcion": "VOTOS NULOS", "nvotos": 9, "candidato": []},
]


def test_extraer_votos_top5():
    votos = extraer_votos_top5(DETALLE_SAMPLE)
    assert votos["Voto_Keiko"] == 35
    assert votos["Voto_Rafael"] == 15
    assert votos["Voto_Nieto"] == 14
    assert votos["Voto_Belmont"] == 17
    assert votos["Voto_Roberto"] == 10


def test_extraer_fila_mesa():
    acta = {
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
    fila = extraer_fila_mesa(acta)
    assert fila["MESA"] == "036999"
    assert fila["DISTRITO"] == "ANCÓN"
    assert fila["Voto_Keiko"] == 35
    assert fila["TIENE_ACTA_INSTALACION"] is True
    assert fila["TIENE_ACTA_SUFRAGIO"] is False
    assert fila["VOTOS_BLANCO"] == 25
    assert fila["VOTOS_NULOS"] == 9


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
