"""Scraper ONPE v2 — batch fetching, 2 fases, stealth, multi-context."""
import argparse
import asyncio
import json
import logging
import random
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright, BrowserContext, Page
from playwright_stealth import Stealth

from src.config import (
    ACTAS_SIN_PDF,
    DATA_DIR,
    DATASET_CSV,
    ID_AMBITO,
    ID_ELECCION,
    UBIGEO_LIMA_PROVINCIA,
)
from src.extractor import extraer_archivos, extraer_fila_mesa
from src import progress_db as db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

BASE_URL = "https://resultadoelectoral.onpe.gob.pe"
API = "/presentacion-backend"
BATCH_SIZE = 10
MAX_WORKERS = 5
DELAY_MIN = 0.5
DELAY_MAX = 1.5

# Distritos prioritarios (Lima Centro/Sur primero)
PRIORITY = [
    "MIRAFLORES", "SAN ISIDRO", "SAN BORJA", "SURQUILLO", "BARRANCO",
    "LA MOLINA", "SANTIAGO DE SURCO", "LA VICTORIA", "LIMA", "LINCE",
    "JESÚS MARÍA", "MAGDALENA DEL MAR", "PUEBLO LIBRE", "SAN MIGUEL",
    "SAN JUAN DE MIRAFLORES", "VILLA EL SALVADOR", "VILLA MARÍA DEL TRIUNFO",
    "CHORRILLOS", "SAN LUIS", "SAN JUAN DE LURIGANCHO",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
]

stealth = Stealth(
    navigator_languages_override=("es-PE", "es"),
    navigator_platform_override="Win32",
)


async def rdelay(factor: float = 1.0) -> None:
    await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX) * factor)


async def create_context(pw) -> tuple:
    browser = await pw.chromium.launch(headless=True)
    ctx = await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        viewport={"width": 1920, "height": 1080},
        locale="es-PE",
        timezone_id="America/Lima",
    )
    await stealth.apply_stealth_async(ctx)
    return browser, ctx


async def init_page(ctx: BrowserContext) -> Page:
    page = await ctx.new_page()
    await page.goto(f"{BASE_URL}/", wait_until="networkidle", timeout=60000)
    await rdelay(0.3)
    return page


async def api_get(page: Page, path: str, params: dict | None = None) -> dict:
    """Single API fetch."""
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
                s = result["s"]
                if s in (429, 503):
                    await asyncio.sleep(2 ** (attempt + 1) + random.random() * 2)
                    continue
                raise RuntimeError(f"HTTP {s} en {path}")
            return result
        except Exception as e:
            if attempt == 2:
                raise
            await asyncio.sleep(2 ** (attempt + 1))
    raise RuntimeError(f"Max retries: {path}")


async def batch_fetch_detalles(page: Page, acta_ids: list[int]) -> list[dict]:
    """Fetch N detalles de actas en paralelo con Promise.all."""
    ids_json = json.dumps(acta_ids)
    results = await page.evaluate(f"""async () => {{
        const ids = {ids_json};
        const results = await Promise.allSettled(
            ids.map(id => fetch("{API}/actas/" + id).then(r => r.json()))
        );
        return results.map((r, i) => ({{
            id: ids[i],
            ok: r.status === 'fulfilled',
            data: r.status === 'fulfilled' ? r.value : {{error: r.reason?.message || 'failed'}}
        }}));
    }}""")
    return results


async def batch_fetch_signed_urls(page: Page, archivo_ids: list[str]) -> list[dict]:
    """Fetch N URLs firmadas S3 en paralelo."""
    ids_json = json.dumps(archivo_ids)
    results = await page.evaluate(f"""async () => {{
        const ids = {ids_json};
        const results = await Promise.allSettled(
            ids.map(id => fetch("{API}/actas/file?id=" + id).then(r => r.json()))
        );
        return results.map((r, i) => ({{
            id: ids[i],
            ok: r.status === 'fulfilled',
            url: r.status === 'fulfilled' ? (r.value?.data || '') : ''
        }}));
    }}""")
    return results


async def batch_download_pdfs(page: Page, downloads: list[dict]) -> list[dict]:
    """Descarga N PDFs desde S3 en paralelo dentro del browser."""
    urls_json = json.dumps([d["url"] for d in downloads])
    results = await page.evaluate(f"""async () => {{
        const urls = {urls_json};
        const results = await Promise.allSettled(
            urls.map(async url => {{
                if (!url) return null;
                const r = await fetch(url);
                if (!r.ok) return null;
                const buf = await r.arrayBuffer();
                return Array.from(new Uint8Array(buf));
            }})
        );
        return results.map(r => ({{
            ok: r.status === 'fulfilled' && r.value !== null,
            data: r.status === 'fulfilled' ? r.value : null
        }}));
    }}""")

    for i, r in enumerate(results):
        downloads[i]["downloaded"] = r["ok"]
        if r["ok"] and r["data"]:
            destino = Path(downloads[i]["destino"])
            destino.parent.mkdir(parents=True, exist_ok=True)
            destino.write_bytes(bytes(r["data"]))
            downloads[i]["size"] = len(r["data"])
    return downloads


def sort_distritos(distritos: list[dict]) -> list[dict]:
    """Ordena: prioritarios primero, resto alfabético."""
    priority_map = {n: i for i, n in enumerate(PRIORITY)}

    def key(d):
        nombre = d.get("nombre", "")
        if nombre in priority_map:
            return (0, priority_map[nombre])
        return (1, nombre)

    return sorted(distritos, key=key)


async def obtener_distritos(page: Page) -> list[dict]:
    resp = await api_get(page, "ubigeos/distritos", {
        "idEleccion": ID_ELECCION,
        "idAmbitoGeografico": ID_AMBITO,
        "idUbigeoProvincia": UBIGEO_LIMA_PROVINCIA,
    })
    return resp.get("data", [])


async def obtener_actas_distrito(page: Page, ubigeo: str) -> list[dict]:
    """Pagina todas las actas (usa tamanio=200 para menos round-trips)."""
    actas = []
    pagina = 0
    while True:
        resp = await api_get(page, "actas", {
            "pagina": pagina, "tamanio": 200,
            "idAmbitoGeografico": ID_AMBITO, "idUbigeo": ubigeo,
        })
        data = resp.get("data", {})
        items = data.get("content", [])
        actas.extend(items)
        if pagina + 1 >= data.get("totalPaginas", 1) or not items:
            break
        pagina += 1
        await rdelay(0.3)
    return actas


# ─── FASE 1: DATOS (votos + metadata, sin PDFs) ───


async def fase1_distrito(
    page: Page, ubigeo: str, nombre: str, sem: asyncio.Semaphore,
) -> tuple[list[dict], list[dict]]:
    """Fase 1: extrae datos de votos con batch fetching."""
    async with sem:
        estado = db.distrito_estado(ubigeo)
        if estado == "completado":
            logger.info("SKIP %s (completado)", nombre)
            return [], []

        logger.info(">>> F1 %s (ubigeo=%s)", nombre, ubigeo)

        actas = await obtener_actas_distrito(page, ubigeo)
        pres = [a for a in actas if a.get("idEleccion") == ID_ELECCION]
        logger.info("  %s: %d presidenciales", nombre, len(pres))
        db.iniciar_distrito(ubigeo, len(actas), len(pres))

        # Filtrar actas no procesadas
        ids_nuevos = [a["id"] for a in pres if not db.acta_ya_procesada(a["id"])]
        logger.info("  %s: %d nuevas (skip %d ya procesadas)", nombre, len(ids_nuevos), len(pres) - len(ids_nuevos))

        filas = []
        sin_pdf = []

        # Procesar en batches
        for batch_start in range(0, len(ids_nuevos), BATCH_SIZE):
            batch_ids = ids_nuevos[batch_start:batch_start + BATCH_SIZE]

            try:
                results = await batch_fetch_detalles(page, batch_ids)

                for r in results:
                    acta_id = r["id"]
                    mesa = ""
                    try:
                        if not r["ok"]:
                            db.error_acta(acta_id, "", ubigeo, nombre, str(r["data"]))
                            continue

                        detalle = r["data"].get("data", r["data"])
                        mesa = detalle.get("codigoMesa", "")
                        estado_acta = detalle.get("descripcionEstadoActa", "")
                        fila = extraer_fila_mesa(detalle)
                        filas.append(fila)

                        archivos = extraer_archivos(detalle)
                        tiene_datos = bool(detalle.get("detalle"))

                        db.registrar_acta(
                            acta_id=acta_id, mesa=mesa, ubigeo=ubigeo,
                            distrito=nombre, estado_acta=estado_acta,
                            votos=fila, tiene_datos=tiene_datos,
                        )

                        # Registrar archivos para fase 2 (sin descargar)
                        if not archivos:
                            sin_pdf.append({
                                "MESA": mesa, "DISTRITO": nombre,
                                "ESTADO": estado_acta, "ACTA_ID": acta_id,
                            })
                        else:
                            for arch in archivos:
                                if not db.pdf_ya_descargado(arch["archivo_id"]):
                                    db.registrar_pdf(
                                        archivo_id=arch["archivo_id"],
                                        acta_id=acta_id, mesa=mesa,
                                        distrito=nombre, tipo=arch["tipo"],
                                        nombre_destino=arch["nombre_destino"],
                                        descargado=False, tamano=0,
                                    )

                        db.incrementar_procesadas(ubigeo)

                    except Exception as e:
                        logger.error("  Error acta %d: %s", acta_id, e)
                        db.error_acta(acta_id, mesa, ubigeo, nombre, str(e))

            except Exception as e:
                logger.error("  Batch error en %s: %s", nombre, e)

            processed = min(batch_start + BATCH_SIZE, len(ids_nuevos))
            if processed % 50 == 0 or processed == len(ids_nuevos):
                logger.info("  %s: %d/%d", nombre, processed, len(ids_nuevos))

            await rdelay(0.5)

        db.completar_distrito(ubigeo, len(filas), len(sin_pdf), 0)
        logger.info("<<< F1 %s DONE: datos=%d sin_pdf=%d", nombre, len(filas), len(sin_pdf))
        return filas, sin_pdf


# ─── FASE 2: PDFs (descarga masiva desde S3) ───


async def fase2_distrito(
    page: Page, nombre: str, sem: asyncio.Semaphore,
) -> int:
    """Fase 2: descarga PDFs pendientes de un distrito."""
    async with sem:
        pendientes = db.pdfs_pendientes(nombre)
        if not pendientes:
            return 0

        logger.info(">>> F2 %s: %d PDFs pendientes", nombre, len(pendientes))
        distrito_dir = DATA_DIR / nombre
        distrito_dir.mkdir(parents=True, exist_ok=True)

        descargados = 0

        # Batch: obtener URLs firmadas
        for batch_start in range(0, len(pendientes), BATCH_SIZE):
            batch = pendientes[batch_start:batch_start + BATCH_SIZE]
            arch_ids = [p["archivo_id"] for p in batch]

            try:
                urls = await batch_fetch_signed_urls(page, arch_ids)

                # Preparar downloads
                downloads = []
                for i, u in enumerate(urls):
                    if u["ok"] and u["url"]:
                        downloads.append({
                            "url": u["url"],
                            "destino": str(distrito_dir / batch[i]["nombre_destino"]),
                            "archivo_id": batch[i]["archivo_id"],
                            "acta_id": batch[i].get("acta_id", 0),
                            "mesa": batch[i]["mesa"],
                            "tipo": batch[i].get("tipo", 0),
                            "nombre_destino": batch[i]["nombre_destino"],
                        })

                if downloads:
                    results = await batch_download_pdfs(page, downloads)
                    for d in results:
                        db.registrar_pdf(
                            archivo_id=d["archivo_id"],
                            acta_id=d.get("acta_id", 0),
                            mesa=d["mesa"],
                            distrito=nombre,
                            tipo=d.get("tipo", 0),
                            nombre_destino=d["nombre_destino"],
                            descargado=d.get("downloaded", False),
                            tamano=d.get("size", 0),
                        )
                        if d.get("downloaded"):
                            descargados += 1

            except Exception as e:
                logger.error("  F2 batch error %s: %s", nombre, e)

            await rdelay(0.3)

        logger.info("<<< F2 %s: %d/%d descargados", nombre, descargados, len(pendientes))
        return descargados


# ─── MAIN ───


async def main(
    test_mode: bool = False,
    workers: int = MAX_WORKERS,
    fase: int = 0,
    filtro_distritos: list[str] | None = None,
) -> None:
    """
    fase=0: ambas fases
    fase=1: solo datos (rápido)
    fase=2: solo PDFs (pesado)
    filtro_distritos: lista de nombres para procesar solo esos
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    db.init_db()

    sem = asyncio.Semaphore(workers)

    async with async_playwright() as pw:
        browser, ctx = await create_context(pw)
        main_page = await init_page(ctx)

        # Obtener y ordenar distritos
        distritos = sort_distritos(await obtener_distritos(main_page))
        logger.info("Distritos: %d (prioritarios primero)", len(distritos))

        if filtro_distritos:
            filtro_upper = [f.upper() for f in filtro_distritos]
            distritos = [d for d in distritos if d["nombre"] in filtro_upper]
            logger.info("FILTRO: %s", [d["nombre"] for d in distritos])
        elif test_mode:
            distritos = distritos[:2]
            logger.info("TEST: %s", [d["nombre"] for d in distritos])

        for d in distritos:
            db.registrar_distrito(d["ubigeo"], d["nombre"])

        # Crear pages para workers
        pages = [main_page]
        for _ in range(min(workers - 1, len(distritos) - 1)):
            p = await ctx.new_page()
            await p.goto(f"{BASE_URL}/", wait_until="networkidle", timeout=60000)
            pages.append(p)
            await rdelay(0.2)
        logger.info("Workers: %d pages", len(pages))

        # ── FASE 1: DATOS ──
        if fase in (0, 1):
            logger.info("═══ FASE 1: EXTRACCIÓN DE DATOS ═══")
            tasks = []
            for i, d in enumerate(distritos):
                page = pages[i % len(pages)]
                tasks.append(fase1_distrito(page, d["ubigeo"], d["nombre"], sem))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            todas_filas = []
            todos_sin_pdf = []
            for r in results:
                if isinstance(r, Exception):
                    logger.error("F1 error: %s", r)
                    continue
                todas_filas.extend(r[0])
                todos_sin_pdf.extend(r[1])

            if todas_filas:
                pd.DataFrame(todas_filas).to_csv(
                    DATASET_CSV, index=False, encoding="utf-8-sig"
                )
                logger.info("Dataset: %d filas -> %s", len(todas_filas), DATASET_CSV)

            if todos_sin_pdf:
                pd.DataFrame(todos_sin_pdf).to_csv(
                    ACTAS_SIN_PDF, index=False, encoding="utf-8-sig"
                )
                logger.info("Sin PDF: %d -> %s", len(todos_sin_pdf), ACTAS_SIN_PDF)

        # ── FASE 2: PDFs ──
        if fase in (0, 2):
            logger.info("═══ FASE 2: DESCARGA DE PDFs ═══")
            nombres = [d["nombre"] for d in distritos]
            tasks2 = []
            for i, nombre in enumerate(nombres):
                page = pages[i % len(pages)]
                tasks2.append(fase2_distrito(page, nombre, sem))

            results2 = await asyncio.gather(*tasks2, return_exceptions=True)
            total_pdfs = sum(r for r in results2 if isinstance(r, int))
            logger.info("PDFs descargados: %d", total_pdfs)

        await browser.close()

    progreso = db.resumen_progreso()
    logger.info("RESUMEN: %s", json.dumps(progreso, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper ONPE v2")
    parser.add_argument("--test", action="store_true", help="Solo 2 distritos")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    parser.add_argument("--fase", type=int, default=0, choices=[0, 1, 2],
                        help="0=ambas, 1=solo datos, 2=solo PDFs")
    parser.add_argument("--distritos", nargs="+", help="Distritos específicos")
    args = parser.parse_args()
    asyncio.run(main(
        test_mode=args.test, workers=args.workers,
        fase=args.fase, filtro_distritos=args.distritos,
    ))
