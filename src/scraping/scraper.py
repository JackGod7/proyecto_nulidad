"""Scraper principal ONPE — Lima Provincia mesa por mesa."""
import argparse
import asyncio
import json
import logging
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from src.scraping.api_client import OnpeClient
from src.config import (
    ACTAS_SIN_PDF,
    CHECKPOINT_DIR,
    DATA_DIR,
    DATASET_CSV,
    ID_AMBITO,
    ID_ELECCION,
    PAGE_SIZE,
    UBIGEO_LIMA_PROVINCIA,
)
from src.extraction.extractor import extraer_archivos, extraer_fila_mesa

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def obtener_distritos(client: OnpeClient) -> list[dict]:
    """Lista todos los distritos de Lima Provincia."""
    data = await client.get_json(
        "ubigeos/distritos",
        params={
            "idEleccion": ID_ELECCION,
            "idAmbitoGeografico": ID_AMBITO,
            "idUbigeoProvincia": UBIGEO_LIMA_PROVINCIA,
        },
    )
    distritos = data if isinstance(data, list) else data.get("data", data)
    logger.info("Distritos encontrados: %d", len(distritos))
    return distritos


async def obtener_actas_distrito(client: OnpeClient, ubigeo: str) -> list[dict]:
    """Pagina TODAS las actas presidenciales de un distrito."""
    actas = []
    pagina = 0

    while True:
        data = await client.get_json(
            "actas",
            params={
                "pagina": pagina,
                "tamanio": PAGE_SIZE,
                "idAmbitoGeografico": ID_AMBITO,
                "idUbigeo": ubigeo,
            },
        )
        contenido = data.get("data", data)
        items = contenido.get("content", [])
        actas.extend(items)

        total_paginas = contenido.get("totalPaginas", 1)
        if pagina + 1 >= total_paginas or not items:
            break
        pagina += 1

    return actas


async def procesar_acta(
    client: OnpeClient,
    acta_id: int,
    distrito_dir: Path,
) -> tuple[dict | None, list[dict]]:
    """Obtiene detalle de un acta, extrae datos y descarga PDFs."""
    resp = await client.get_json(f"actas/{acta_id}")
    detalle = resp.get("data", resp)

    fila = extraer_fila_mesa(detalle)
    archivos = extraer_archivos(detalle)
    sin_pdf = []

    if not archivos:
        sin_pdf.append({
            "MESA": detalle.get("codigoMesa", ""),
            "DISTRITO": detalle.get("ubigeoNivel03", ""),
            "ESTADO": detalle.get("descripcionEstadoActa", ""),
            "ACTA_ID": acta_id,
        })
        return fila, sin_pdf

    for arch in archivos:
        destino = distrito_dir / arch["nombre_destino"]
        if destino.exists():
            continue
        try:
            signed_url = await client.get_signed_url(arch["archivo_id"])
            pdf_bytes = await client.download_pdf(signed_url)
            destino.write_bytes(pdf_bytes)
        except Exception as e:
            logger.error("Error descargando %s: %s", arch["nombre_destino"], e)

    return fila, sin_pdf


def distrito_ya_procesado(nombre_distrito: str) -> bool:
    """Verifica si un distrito ya fue procesado (checkpoint)."""
    checkpoint = CHECKPOINT_DIR / f"{nombre_distrito}.done"
    return checkpoint.exists()


def marcar_distrito_procesado(nombre_distrito: str, resumen: dict) -> None:
    """Marca distrito como completado."""
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    checkpoint = CHECKPOINT_DIR / f"{nombre_distrito}.done"
    checkpoint.write_text(json.dumps(resumen, ensure_ascii=False))


async def procesar_distrito(
    client: OnpeClient,
    ubigeo: str,
    nombre_distrito: str,
) -> tuple[list[dict], list[dict]]:
    """Procesa todas las actas de un distrito."""
    if distrito_ya_procesado(nombre_distrito):
        logger.info("SKIP %s (ya procesado)", nombre_distrito)
        return [], []

    logger.info("Procesando %s (ubigeo=%s)", nombre_distrito, ubigeo)
    distrito_dir = DATA_DIR / nombre_distrito
    distrito_dir.mkdir(parents=True, exist_ok=True)

    # Obtener lista de actas
    actas_lista = await obtener_actas_distrito(client, ubigeo)
    logger.info("  %s: %d actas encontradas", nombre_distrito, len(actas_lista))

    # Filtrar solo presidenciales (idEleccion=10)
    presidenciales = [a for a in actas_lista if a.get("idEleccion") == ID_ELECCION]
    logger.info("  %s: %d actas presidenciales", nombre_distrito, len(presidenciales))

    filas = []
    sin_pdf_total = []

    for acta in tqdm(presidenciales, desc=nombre_distrito, leave=False):
        fila, sin_pdf = await procesar_acta(client, acta["id"], distrito_dir)
        if fila:
            filas.append(fila)
        sin_pdf_total.extend(sin_pdf)

    resumen = {
        "total_actas": len(actas_lista),
        "presidenciales": len(presidenciales),
        "con_datos": len(filas),
        "sin_pdf": len(sin_pdf_total),
    }
    marcar_distrito_procesado(nombre_distrito, resumen)
    logger.info("  %s: OK — %s", nombre_distrito, resumen)

    return filas, sin_pdf_total


async def main(test_mode: bool = False) -> None:
    """Scraper principal."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    async with OnpeClient() as client:
        distritos = await obtener_distritos(client)

        if test_mode:
            distritos = distritos[:1]
            logger.info("MODO TEST: solo %s", distritos[0])

        todas_filas = []
        todos_sin_pdf = []

        for dist in tqdm(distritos, desc="Distritos"):
            ubigeo = str(dist.get("idUbigeo", dist.get("id", "")))
            nombre = dist.get("nombreUbigeo", dist.get("nombre", ubigeo))

            filas, sin_pdf = await procesar_distrito(client, ubigeo, nombre)
            todas_filas.extend(filas)
            todos_sin_pdf.extend(sin_pdf)

            # Guardar incrementalmente
            if todas_filas:
                df = pd.DataFrame(todas_filas)
                df.to_csv(DATASET_CSV, index=False, encoding="utf-8-sig")

            if todos_sin_pdf:
                df_sin = pd.DataFrame(todos_sin_pdf)
                df_sin.to_csv(ACTAS_SIN_PDF, index=False, encoding="utf-8-sig")

    logger.info("COMPLETADO: %d filas, %d sin PDF", len(todas_filas), len(todos_sin_pdf))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper ONPE Lima Provincia")
    parser.add_argument("--test", action="store_true", help="Solo 1 distrito")
    args = parser.parse_args()
    asyncio.run(main(test_mode=args.test))
