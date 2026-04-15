# Reglas Scraper ONPE

## Modo Cavernicola (OBLIGATORIO)
- Max 3-5 palabras por punto en respuestas
- 0 cortesía, 0 preámbulo, 0 relleno
- Código > explicación. Solo mostrar errores.
- RTK para TODO comando bash

## Python
- httpx async con rate limit 1 req/seg
- Type hints en TODA firma
- logging, NUNCA print
- Archivos < 400 líneas
- Immutable: return new dict, no mutar
- utf-8-sig para CSV Windows

## Scraping
- Paginar con tamanio=50 (max API)
- Reintentar 3x con backoff exponencial en 429/503
- Guardar progreso parcial (checkpoint por distrito)
- Actas "Pendiente" → log en actas_sin_pdf.csv
- Rate limit: 1 req/seg mínimo. Servidor público, no saturar.

## PDFs
- Descargar 3 tipos: ESCRUTINIO(1), INSTALACION(3), SUFRAGIO(4)
- Naming: `{mesa}_{TIPO}.pdf` en carpeta `data/{DISTRITO}/`
- URL firmada S3 expira ~15min, obtener justo antes de descargar
- Verificar Content-Type: application/pdf antes de guardar

## Dataset
- CSV principal: dataset_lima_provincia.csv
- Columnas: DEPARTAMENTO, PROVINCIA, DISTRITO, LOCAL_VOTACION, MESA, TOTAL_ELECTORES, TOTAL_VOTANTES, ESTADO_ACTA, Voto_Keiko, Voto_Rafael, Voto_Nieto, Voto_Belmont, Voto_Roberto, TIENE_ACTA_INSTALACION, TIENE_ACTA_ESCRUTINIO, TIENE_ACTA_SUFRAGIO
- actas_sin_pdf.csv: mesas pendientes/sin archivos
- horas_instalacion.csv: output del OCR
