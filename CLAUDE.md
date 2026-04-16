# Proyecto Nulidad — Auditoría Forense Electoral ONPE 2026

## Objetivo
Sistema forense para sustentar nulidad de elecciones peruanas 2026.
Captura completa de datos electorales con trazabilidad, integridad y cadena de custodia.
Foco: Lima Metropolitana, distrito por distrito.

## Stack
- **Python 3.12+** (uv)
- **Playwright** — scraping stealth (API vía browser context)
- **SQLite** — forensic.db (schema v2 forense)
- **Gemini 2.5 Pro** — lectura AI de PDFs escaneados
- **pandas + openpyxl** — reportes

## Estructura
```
src/
├── config.py                    # URLs, constantes, rutas
├── scraping/                    # Captura de datos ONPE
│   └── browser_scraper.py       # Playwright stealth + batch fetching
├── extraction/                  # Extracción de datos
│   ├── extractor.py             # v2: ALL partidos, ALL campos
│   └── gemini_extractor.py      # AI lectura PDFs
├── db/                          # Base de datos
│   ├── schema.py                # Schema v2 forense + migración
│   └── custody.py               # Cadena de custodia
├── audit/                       # Auditoría
│   ├── integrity.py             # SHA-256 hashing PDFs
│   ├── cross_validator.py       # API vs PDF discrepancias
│   └── temporal_monitor.py      # Re-scrape + detección cambios
├── reporting/                   # Reportes
│   ├── gen_reporte.py           # Excel de avance
│   └── legal_report.py         # Reporte grado legal
└── utils/
    └── disk_manager.py          # Gestión espacio disco
```

## Comandos
```bash
# Re-scrape con extractor v2 (ALL partidos)
uv run python -c "from src.scraping.browser_scraper import main; import asyncio; asyncio.run(main(fase=1, workers=5))"

# Re-scrape distritos específicos
uv run python -c "from src.scraping.browser_scraper import main; import asyncio; asyncio.run(main(fase=1, workers=3, filtro_distritos=['MIRAFLORES','SAN JUAN DE MIRAFLORES']))"

# Descargar PDFs
uv run python -c "from src.scraping.browser_scraper import main; import asyncio; asyncio.run(main(fase=2, workers=3, filtro_distritos=['MIRAFLORES']))"

# Generar Excel avance
uv run python src/reporting/gen_reporte.py

# Tests
uv run pytest tests/ -v
```

## API ONPE — Endpoints
Base: `https://resultadoelectoral.onpe.gob.pe/presentacion-backend`
**Nota:** API solo funciona desde Playwright (CloudFront bloquea requests directos).

| Endpoint | Uso |
|----------|-----|
| `proceso/proceso-electoral-activo` | Proceso electoral activo (id=2) |
| `proceso/2/elecciones` | Elecciones dentro del proceso |
| `ubigeos/departamentos?idEleccion=10&idAmbitoGeografico=1` | Regiones |
| `ubigeos/distritos?...&idUbigeoProvincia=140100` | 43 distritos Lima |
| `actas?pagina={p}&tamanio=200&idAmbitoGeografico=1&idUbigeo={ubigeo}` | Actas paginadas |
| `actas/{id}` | Detalle completo: votos ALL partidos, archivos, metadata |
| `actas/file?id={archivoId}` | URL firmada S3 para PDF |

## Schema Forense (forensic.db)
- **actas**: ALL campos API + raw JSON + SHA-256 hash + trazabilidad
- **votos_por_mesa**: votos normalizados ALL partidos, fuente (api/pdf)
- **pdfs**: SHA-256 hash, estado disco, datos Gemini AI
- **snapshots**: tracking temporal (detectar cambios ONPE)
- **discrepancias**: API vs PDF cross-validation
- **cadena_custodia**: log completo de toda acción

## Convenciones
- RTK para TODOS los comandos bash
- Modo cavernicola: 0 relleno, max 5 palabras por punto
- Python: type hints, logging no print, < 400 líneas/archivo
- Immutable data patterns
- Rate limit: delays aleatorios 0.3-1.0s a ONPE
- SHA-256 de TODO PDF descargado
- Cadena de custodia en TODA operación
