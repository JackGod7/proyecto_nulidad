"""Extrae datos estructurados de las respuestas API ONPE."""
import logging
from typing import Any

from src.config import CANDIDATOS_TOP5, TIPO_NOMBRES

logger = logging.getLogger(__name__)


def extraer_votos_top5(detalle: list[dict]) -> dict[str, int | None]:
    """Extrae votos de los 5 candidatos del detalle de un acta."""
    votos = {v: None for v in CANDIDATOS_TOP5.values()}

    for partido in detalle:
        candidatos = partido.get("candidato", [])
        for cand in candidatos:
            apellido = cand.get("apellidoPaterno", "")
            apellido_m = cand.get("apellidoMaterno", "")
            nombre_completo = f"{apellido} {apellido_m}".strip()

            for key_busqueda, col_csv in CANDIDATOS_TOP5.items():
                if key_busqueda in nombre_completo.upper() or key_busqueda in apellido.upper():
                    votos[col_csv] = partido.get("nvotos")
                    break

    return votos


def extraer_fila_mesa(acta: dict[str, Any]) -> dict[str, Any]:
    """Convierte el detalle de un acta en una fila para el CSV."""
    archivos = acta.get("archivos", [])
    tipos_presentes = {a["tipo"] for a in archivos}

    fila = {
        "DEPARTAMENTO": acta.get("ubigeoNivel01", ""),
        "PROVINCIA": acta.get("ubigeoNivel02", ""),
        "DISTRITO": acta.get("ubigeoNivel03", ""),
        "LOCAL_VOTACION": acta.get("nombreLocalVotacion", ""),
        "MESA": acta.get("codigoMesa", ""),
        "TOTAL_ELECTORES": acta.get("totalElectoresHabiles"),
        "TOTAL_VOTANTES": acta.get("totalAsistentes"),
        "VOTOS_EMITIDOS": acta.get("totalVotosEmitidos"),
        "VOTOS_VALIDOS": acta.get("totalVotosValidos"),
        "PARTICIPACION_PCT": acta.get("porcentajeParticipacionCiudadana"),
        "ESTADO_ACTA": acta.get("descripcionEstadoActa", ""),
        "SOLUCION_TECNOLOGICA": acta.get("descripcionSolucionTecnologica", ""),
        "TIENE_ACTA_ESCRUTINIO": 1 in tipos_presentes,
        "TIENE_ACTA_INSTALACION": 3 in tipos_presentes,
        "TIENE_ACTA_SUFRAGIO": 4 in tipos_presentes,
    }

    # Votos top 5
    detalle = acta.get("detalle", [])
    if detalle:
        votos = extraer_votos_top5(detalle)
        fila.update(votos)
    else:
        for col in CANDIDATOS_TOP5.values():
            fila[col] = None

    # Votos especiales
    for partido in detalle:
        desc = partido.get("descripcion", "")
        if desc == "VOTOS EN BLANCO":
            fila["VOTOS_BLANCO"] = partido.get("nvotos")
        elif desc == "VOTOS NULOS":
            fila["VOTOS_NULOS"] = partido.get("nvotos")
        elif desc == "VOTOS IMPUGNADOS":
            fila["VOTOS_IMPUGNADOS"] = partido.get("nvotos")

    return fila


def extraer_archivos(acta: dict[str, Any]) -> list[dict[str, str]]:
    """Extrae info de archivos para descarga."""
    archivos = acta.get("archivos", [])
    mesa = acta.get("codigoMesa", "000000")
    resultado = []

    for arch in archivos:
        tipo = arch.get("tipo")
        nombre_tipo = TIPO_NOMBRES.get(tipo)
        if nombre_tipo:
            resultado.append({
                "archivo_id": arch["id"],
                "nombre_original": arch["nombre"],
                "nombre_destino": f"{mesa}_{nombre_tipo}.pdf",
                "tipo": tipo,
                "descripcion": arch.get("descripcion", ""),
            })

    return resultado
