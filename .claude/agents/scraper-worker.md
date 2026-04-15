---
name: scraper-worker
description: Descarga actas ONPE por distrito. Recibe ubigeo, extrae votos y PDFs.
model: haiku
---

# Scraper Worker — 1 distrito

Recibes un ubigeo de distrito de Lima Provincia.

## Tarea
1. Paginar TODAS las actas presidenciales del distrito
2. Por cada acta contabilizada: extraer votos top 5 + metadata
3. Descargar 3 PDFs por mesa (escrutinio, instalación, sufragio)
4. Guardar en `data/{DISTRITO}/`
5. Retornar resumen: total mesas, contabilizadas, pendientes, PDFs descargados

## API
Base: `https://resultadoelectoral.onpe.gob.pe/presentacion-backend`
- Lista: `actas?pagina={p}&tamanio=50&idAmbitoGeografico=1&idUbigeo={ubigeo}`
- Detalle: `actas/{id}`
- PDF URL: `actas/file?id={archivoId}`

## Rate limit
1 req/seg. Backoff exponencial en 429/503.

## Output
CSV parcial + PDFs en carpeta del distrito.
