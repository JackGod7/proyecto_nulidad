"""Extractor v2 — captura TODOS los partidos y ALL campos de la API ONPE."""
import hashlib
import json
import getpass
import logging
import socket
from typing import Any

logger = logging.getLogger(__name__)

TIPO_NOMBRES = {1: "ESCRUTINIO", 3: "INSTALACION", 4: "SUFRAGIO"}


def _hash_json(data: Any) -> str:
    raw = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def extraer_todos_los_votos(detalle: list[dict]) -> dict[str, Any]:
    """Extrae votos de TODOS los partidos + votos especiales."""
    partidos = {}
    blanco = None
    nulos = None
    impugnados = None

    for p in detalle:
        desc = p.get("descripcion", "").strip()
        nvotos = p.get("nvotos")
        if desc == "VOTOS EN BLANCO":
            blanco = nvotos
        elif desc == "VOTOS NULOS":
            nulos = nvotos
        elif desc == "VOTOS IMPUGNADOS":
            impugnados = nvotos
        else:
            partidos[desc] = nvotos

    return {"partidos": partidos, "blanco": blanco, "nulos": nulos, "impugnados": impugnados}


def extraer_votos_normalizados(detalle: list[dict]) -> list[dict]:
    """Datos normalizados para tabla votos_por_mesa (ALL partidos)."""
    rows = []
    for p in detalle:
        desc = p.get("descripcion", "").strip()
        if desc in ("VOTOS EN BLANCO", "VOTOS NULOS", "VOTOS IMPUGNADOS"):
            continue

        candidatos = p.get("candidato", [])
        cand_nombre = ""
        cand_doc = ""
        if candidatos:
            c = candidatos[0]
            cand_nombre = f"{c.get('nombres', '')} {c.get('apellidoPaterno', '')} {c.get('apellidoMaterno', '')}".strip()
            cand_doc = c.get("cdocumentoIdentidad", "")

        rows.append({
            "partido_nombre": desc,
            "partido_codigo": p.get("ccodigo", ""),
            "candidato_nombre": cand_nombre,
            "candidato_documento": cand_doc,
            "votos": p.get("nvotos"),
            "porcentaje_validos": p.get("nporcentajeVotosValidos"),
            "porcentaje_emitidos": p.get("nporcentajeVotosEmitidos"),
            "posicion_cedula": p.get("nposicion"),
        })
    return rows


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


def extraer_fila_completa(api_response: dict[str, Any]) -> dict[str, Any]:
    """Extrae TODOS los campos — versión forense.

    Recibe el response completo {success, data: {...}}.
    """
    acta = api_response.get("data", api_response)
    detalle = acta.get("detalle", [])
    archivos = acta.get("archivos", [])
    tipos_pdf = {a.get("tipo") for a in archivos}
    votos = extraer_todos_los_votos(detalle)

    return {
        "departamento": acta.get("ubigeoNivel01", ""),
        "provincia": acta.get("ubigeoNivel02", ""),
        "distrito": acta.get("ubigeoNivel03", ""),
        "local_votacion": acta.get("nombreLocalVotacion", ""),
        "codigo_local_votacion": acta.get("codigoLocalVotacion"),
        "mesa": acta.get("codigoMesa", ""),

        "estado_acta": acta.get("descripcionEstadoActa", ""),
        "codigo_estado_acta": acta.get("codigoEstadoActa", ""),
        "estado_acta_resolucion": acta.get("estadoActaResolucion", ""),
        "estado_descripcion_resolucion": acta.get("estadoDescripcionActaResolucion", ""),
        "sub_estado_acta": acta.get("descripcionSubEstadoActa", ""),
        "estado_computo": acta.get("estadoComputo", ""),
        "solucion_tecnologica": acta.get("descripcionSolucionTecnologica", ""),

        "total_electores": acta.get("totalElectoresHabiles"),
        "total_votantes": acta.get("totalAsistentes"),
        "votos_emitidos": acta.get("totalVotosEmitidos"),
        "votos_validos": acta.get("totalVotosValidos"),
        "participacion_pct": acta.get("porcentajeParticipacionCiudadana"),

        "votos_todos_json": json.dumps(votos["partidos"], ensure_ascii=False),
        "votos_blanco": votos["blanco"],
        "votos_nulos": votos["nulos"],
        "votos_impugnados": votos["impugnados"],

        "tiene_pdf_escrutinio": 1 if 1 in tipos_pdf else 0,
        "tiene_pdf_instalacion": 1 if 3 in tipos_pdf else 0,
        "tiene_pdf_sufragio": 1 if 4 in tipos_pdf else 0,

        "api_response_raw": json.dumps(api_response, ensure_ascii=False),
        "api_response_hash": _hash_json(api_response),

        "tiene_datos": 1 if detalle else 0,
        "operador": getpass.getuser(),
        "maquina": socket.gethostname(),
    }


# Backward compat — v1 functions
def extraer_fila_mesa(acta: dict[str, Any]) -> dict[str, Any]:
    """Legacy v1 — solo top 5. Usar extraer_fila_completa() en v2."""
    from src.config import CANDIDATOS_TOP5
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
    detalle = acta.get("detalle", [])
    if detalle:
        from src.extractor import extraer_votos_top5
        fila.update(extraer_votos_top5(detalle))
    else:
        for col in CANDIDATOS_TOP5.values():
            fila[col] = None
    for partido in detalle:
        desc = partido.get("descripcion", "")
        if desc == "VOTOS EN BLANCO":
            fila["VOTOS_BLANCO"] = partido.get("nvotos")
        elif desc == "VOTOS NULOS":
            fila["VOTOS_NULOS"] = partido.get("nvotos")
        elif desc == "VOTOS IMPUGNADOS":
            fila["VOTOS_IMPUGNADOS"] = partido.get("nvotos")
    return fila
