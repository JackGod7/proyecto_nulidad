"""Monitor temporal forense — detecta cambios en datos ONPE."""
import asyncio
import hashlib
import json
import logging
import random
from datetime import datetime, timezone
from pathlib import Path

from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from src.config import ID_AMBITO, ID_ELECCION, UBIGEO_LIMA_PROVINCIA
from src.db.schema import get_conn, log_custodia

logger = logging.getLogger(__name__)

API = "/presentacion-backend"
BASE_URL = "https://resultadoelectoral.onpe.gob.pe"
BATCH_SIZE = 15

stealth = Stealth(
    navigator_languages_override=("es-PE", "es"),
    navigator_platform_override="Win32",
)

# Campos críticos a monitorear
CAMPOS_CRITICOS = [
    "codigoEstadoActa", "descripcionEstadoActa",
    "totalElectoresHabiles", "totalVotosEmitidos",
    "totalVotosValidos", "totalAsistentes",
    "porcentajeParticipacionCiudadana",
    "estadoActaResolucion",
]


def _hash_response(data: dict) -> str:
    raw = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode()).hexdigest()


def _extract_votos(detalle: list) -> dict:
    """Extrae mapa partido->votos del detalle."""
    votos = {}
    for d in detalle:
        nombre = d.get("descripcion", "")
        votos[nombre] = d.get("nvotos", 0)
    return votos


def _diff_acta(anterior: dict, actual: dict) -> list[dict]:
    """Compara dos responses y retorna lista de cambios."""
    cambios = []

    # Campos críticos
    for campo in CAMPOS_CRITICOS:
        v_ant = anterior.get(campo)
        v_act = actual.get(campo)
        if v_ant != v_act:
            cambios.append({
                "campo": campo,
                "anterior": v_ant,
                "actual": v_act,
                "severidad": "CRITICA" if "Votos" in campo or "Estado" in campo else "ALTA",
            })

    # Votos por partido
    votos_ant = _extract_votos(anterior.get("detalle", []))
    votos_act = _extract_votos(actual.get("detalle", []))

    todos_partidos = set(votos_ant.keys()) | set(votos_act.keys())
    for partido in todos_partidos:
        v_a = votos_ant.get(partido, 0)
        v_b = votos_act.get(partido, 0)
        if v_a != v_b:
            cambios.append({
                "campo": f"votos:{partido}",
                "anterior": v_a,
                "actual": v_b,
                "severidad": "CRITICA",
            })

    # Archivos (PDFs que aparecen/desaparecen)
    arch_ant = {a["id"] for a in anterior.get("archivos", [])}
    arch_act = {a["id"] for a in actual.get("archivos", [])}
    if arch_ant != arch_act:
        nuevos = arch_act - arch_ant
        eliminados = arch_ant - arch_act
        if eliminados:
            cambios.append({
                "campo": "archivos_eliminados",
                "anterior": list(eliminados),
                "actual": None,
                "severidad": "CRITICA",
            })
        if nuevos:
            cambios.append({
                "campo": "archivos_nuevos",
                "anterior": None,
                "actual": list(nuevos),
                "severidad": "ALTA",
            })

    return cambios


async def _api_get(page, path: str, params: dict | None = None) -> dict:
    query = ""
    if params:
        query = "?" + "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{API}/{path}{query}"

    for attempt in range(3):
        try:
            result = await page.evaluate(f"""async () => {{
                const r = await fetch("{url}");
                if (!r.ok) return {{__err: true, s: r.status}};
                return await r.json();
            }}""")
            if isinstance(result, dict) and result.get("__err"):
                if result["s"] in (429, 503) and attempt < 2:
                    await asyncio.sleep(2 ** (attempt + 1))
                    continue
                raise RuntimeError(f"HTTP {result['s']} en {path}")
            return result
        except Exception:
            if attempt == 2:
                raise
            await asyncio.sleep(2 ** (attempt + 1))
    raise RuntimeError(f"Max retries: {path}")


async def _batch_detalles(page, acta_ids: list[int]) -> list[dict]:
    ids_json = json.dumps(acta_ids)
    return await page.evaluate(f"""async () => {{
        const ids = {ids_json};
        const results = await Promise.allSettled(
            ids.map(id => fetch("{API}/actas/" + id).then(r => r.json()))
        );
        return results.map((r, i) => ({{
            id: ids[i],
            ok: r.status === 'fulfilled',
            data: r.status === 'fulfilled' ? r.value : null
        }}));
    }}""")


async def monitorear_distrito(page, ubigeo: str, nombre: str) -> dict:
    """Re-scrapea un distrito y detecta cambios vs última captura."""
    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()

    # Obtener actas actuales del API
    actas = []
    pagina = 0
    while True:
        resp = await _api_get(page, "actas", {
            "pagina": pagina, "tamanio": 200,
            "idAmbitoGeografico": ID_AMBITO, "idUbigeo": ubigeo,
        })
        data = resp.get("data", {})
        items = data.get("content", [])
        actas.extend(items)
        if pagina + 1 >= data.get("totalPaginas", 1) or not items:
            break
        pagina += 1
        await asyncio.sleep(random.uniform(0.3, 0.8))

    pres = [a for a in actas if a.get("idEleccion") == ID_ELECCION]
    logger.info("[MONITOR] %s: %d presidenciales", nombre, len(pres))

    # Cargar hashes anteriores (raw ya no se guarda; diff por hash)
    prev_hashes = {}
    for r in conn.execute(
        "SELECT acta_id, api_response_hash FROM actas "
        "WHERE ubigeo=? AND api_response_hash IS NOT NULL",
        (ubigeo,)
    ).fetchall():
        prev_hashes[r["acta_id"]] = {"hash": r["api_response_hash"]}

    # Fetch detalles en batches
    ids = [a["id"] for a in pres]
    total_cambios = 0
    total_nuevas = 0
    cambios_detalle = []

    for batch_start in range(0, len(ids), BATCH_SIZE):
        batch = ids[batch_start:batch_start + BATCH_SIZE]
        results = await _batch_detalles(page, batch)

        for r in results:
            if not r["ok"] or not r["data"]:
                continue

            acta_id = r["id"]
            acta_data = r["data"].get("data", r["data"])
            new_hash = _hash_response(acta_data)

            if acta_id in prev_hashes:
                old_hash = prev_hashes[acta_id]["hash"]
                if old_hash != new_hash:
                    # CAMBIO DETECTADO (diff a alto nivel; raw ya no se guarda)
                    total_cambios += 1
                    diffs = [{"campo": "api_response_hash",
                              "antes": old_hash, "despues": new_hash}]
                    mesa = acta_data.get("codigoMesa", "?")

                    logger.warning(
                        "[CAMBIO] %s mesa %s: %d campos cambiaron",
                        nombre, mesa, len(diffs)
                    )

                    # Guardar snapshot
                    conn.execute("""
                        INSERT INTO snapshots
                        (acta_id, snapshot_at, estado_acta, votos_json,
                         api_response_hash, cambio_detectado, diff_descripcion)
                        VALUES (?,?,?,?,?,1,?)
                    """, (
                        acta_id, now,
                        acta_data.get("codigoEstadoActa"),
                        json.dumps(_extract_votos(acta_data.get("detalle", [])), ensure_ascii=False),
                        new_hash,
                        json.dumps(diffs, ensure_ascii=False),
                    ))

                    # Actualizar acta con nuevos datos (solo hash + campos escalares)
                    conn.execute("""
                        UPDATE actas SET
                            api_response_hash=?,
                            estado_acta=?, codigo_estado_acta=?,
                            total_electores=?, total_votantes=?,
                            votos_emitidos=?, votos_validos=?,
                            participacion_pct=?, capturado_at=?
                        WHERE acta_id=?
                    """, (
                        new_hash,
                        acta_data.get("descripcionEstadoActa"),
                        acta_data.get("codigoEstadoActa"),
                        acta_data.get("totalElectoresHabiles"),
                        acta_data.get("totalAsistentes"),
                        acta_data.get("totalVotosEmitidos"),
                        acta_data.get("totalVotosValidos"),
                        acta_data.get("porcentajeParticipacionCiudadana"),
                        now, acta_id,
                    ))

                    # Log custodia
                    log_custodia(
                        conn, "CAMBIO_DETECTADO",
                        entidad_tipo="acta", entidad_id=str(acta_id),
                        detalle={"mesa": mesa, "distrito": nombre,
                                 "cambios": len(diffs), "diffs": diffs},
                    )

                    cambios_detalle.append({
                        "acta_id": acta_id,
                        "mesa": mesa,
                        "diffs": diffs,
                    })
                else:
                    # Sin cambios — guardar snapshot limpio
                    conn.execute("""
                        INSERT INTO snapshots
                        (acta_id, snapshot_at, estado_acta, api_response_hash, cambio_detectado)
                        VALUES (?,?,?,?,0)
                    """, (acta_id, now, acta_data.get("codigoEstadoActa"), new_hash))
            else:
                # Acta nueva (no existía antes)
                total_nuevas += 1
                logger.info("[NUEVA] %s mesa %s", nombre, acta_data.get("codigoMesa", "?"))

        await asyncio.sleep(random.uniform(0.3, 0.8))

    conn.commit()

    # Guardar timestamp ONPE
    try:
        fecha_resp = await _api_get(page, "fecha/listarFecha")
        fecha_onpe = fecha_resp.get("data", {}).get("fechaProceso")
        if fecha_onpe:
            log_custodia(
                conn, "MONITOR_FECHA_ONPE",
                detalle={"fechaProceso": fecha_onpe, "distrito": nombre},
            )
            conn.commit()
    except Exception as e:
        logger.warning("No se pudo obtener fecha ONPE: %s", e)

    # Log resumen
    log_custodia(
        conn, "MONITOR_COMPLETO",
        entidad_tipo="distrito", entidad_id=ubigeo,
        detalle={
            "nombre": nombre,
            "actas_revisadas": len(ids),
            "cambios": total_cambios,
            "nuevas": total_nuevas,
        },
    )
    conn.commit()
    conn.close()

    resumen = {
        "distrito": nombre,
        "ubigeo": ubigeo,
        "actas_revisadas": len(ids),
        "cambios_detectados": total_cambios,
        "actas_nuevas": total_nuevas,
        "detalle_cambios": cambios_detalle,
        "timestamp": now,
    }

    logger.info(
        "[MONITOR] %s: %d revisadas, %d cambios, %d nuevas",
        nombre, len(ids), total_cambios, total_nuevas
    )

    return resumen


async def monitorear(
    distritos: list[str] | None = None,
    output_json: str | None = None,
) -> list[dict]:
    """Ejecuta monitoreo para distritos especificados o todos los v2."""
    conn = get_conn()

    if distritos:
        rows = conn.execute(
            f"SELECT ubigeo, nombre FROM distritos WHERE nombre IN ({','.join('?' * len(distritos))})",
            [d.upper() for d in distritos]
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT ubigeo, nombre FROM distritos WHERE estado LIKE 'completado%' ORDER BY nombre"
        ).fetchall()

    conn.close()

    if not rows:
        logger.error("No hay distritos para monitorear")
        return []

    logger.info("[MONITOR] Iniciando monitoreo de %d distritos", len(rows))

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="es-PE",
            timezone_id="America/Lima",
        )
        await stealth.apply_stealth_async(ctx)
        page = await ctx.new_page()
        await page.goto(f"{BASE_URL}/", wait_until="networkidle", timeout=60000)
        await asyncio.sleep(random.uniform(1, 2))

        resultados = []
        for row in rows:
            try:
                res = await monitorear_distrito(page, row["ubigeo"], row["nombre"])
                resultados.append(res)
            except Exception as e:
                logger.error("[MONITOR] Error %s: %s", row["nombre"], e)
                resultados.append({
                    "distrito": row["nombre"],
                    "error": str(e),
                })
            await asyncio.sleep(random.uniform(1, 2))

        await browser.close()

    # Resumen
    total_cambios = sum(r.get("cambios_detectados", 0) for r in resultados)
    logger.info("[MONITOR] Completado: %d distritos, %d cambios totales", len(resultados), total_cambios)

    # Exportar JSON si hay cambios
    if output_json or total_cambios > 0:
        out = output_json or f"data/monitor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", encoding="utf-8") as f:
            json.dump(resultados, f, ensure_ascii=False, indent=2)
        logger.info("[MONITOR] Reporte: %s", out)

    return resultados


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    distritos_arg = sys.argv[1:] if len(sys.argv) > 1 else None
    asyncio.run(monitorear(distritos=distritos_arg))
