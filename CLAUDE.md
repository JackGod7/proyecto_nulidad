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
- **actas**: ALL campos API + SHA-256 hash (raw JSON NO se guarda — ya normalizado)
- **votos_por_mesa**: votos normalizados ALL partidos, fuente (api/pdf)
- **pdfs**: SHA-256 hash, estado disco, datos Gemini AI
- **instalaciones**: hora apertura + metadata (sin gemini_raw, normalizado)
- **acta_estado_historial**: lineaTiempo normalizada (ex api_response_raw)
- **snapshots**: tracking temporal (detectar cambios ONPE)
- **discrepancias**: API vs PDF cross-validation
- **cadena_custodia**: log completo de toda acción

## Forensic v2 — Sync por distrito (NDJSON+gzip+manifest)

Cada distrito exporta a `sync/export/{SLUG}/`:
- `actas.ndjson.gz` · `votos.ndjson.gz` · `pdfs.ndjson.gz` · `instalaciones.ndjson.gz` · `acta_estado_historial.ndjson.gz`
- `manifest.json` → SHA-256 de cada archivo + rows + operador + fecha

Comandos:
```bash
uv run python -m src.sync.exporter_v2 "DISTRITO"     # exportar
uv run python -m src.sync.verifier                   # verificar todos
uv run python -m src.sync.verifier --changed         # solo modificados en git
```

Pre-commit hook (`scripts/install_hooks.sh`) bloquea commit si manifest no verifica.

## Branches por distrito (Forensic v2)

1 operador = 1 branch `distrito/<slug>` activa.
- `distrito/vmt`, `distrito/chorrillos`, `distrito/sjm`, etc.
- Cada commit va a la branch del distrito
- PR a `main` solo cuando distrito queda COMPLETO (gap=0)
- Branches viejas `maquina-1..6` → archivadas

## Convenciones
- RTK para TODOS los comandos bash
- Modo cavernicola: 0 relleno, max 5 palabras por punto
- Python: type hints, logging no print, < 400 líneas/archivo
- Immutable data patterns
- Rate limit: delays aleatorios 0.3-1.0s a ONPE
- SHA-256 de TODO PDF descargado
- Cadena de custodia en TODA operación

## QUICKSTART Máquina Nueva (3 pasos)

```bash
git clone https://github.com/JackGod7/proyecto_nulidad.git
cd proyecto_nulidad
bash scripts/setup_machine.sh
```

Luego editar dos archivos:
- `.env` → pegar `GEMINI_API_KEY`
- `machine_config.json` → ajustar `machine_id` y `distritos` según tabla abajo

Finalmente abrir Claude Code:
```bash
claude
```

Dentro de Claude, ejecutar **un solo comando**:
```
/iniciar
```

El hook `SessionStart` ya muestra el briefing automático al abrir. `/iniciar` lo re-ejecuta y guía el siguiente paso.

## Comandos Claude disponibles

| Comando | Qué hace |
|---------|----------|
| `/iniciar` | Muestra estado + distrito asignado + siguiente acción |
| `/trabajar [DISTRITO]` | Pipeline completo: scrape → pdfs → extraer |
| `/sync` | Exporta JSON locales y los commitea al repo |

## Sync entre máquinas (via repo git)

Cada máquina worker:
```
/sync     ← exporta + push a GitHub
```

Máquina principal:
```bash
git pull
uv run python src/sync/merger.py
```

Los JSON viajan por el repo (`sync/export/*.json`), no se pierden.

## LOTE 1 — Distritos Prioritarios (6 Lima Sur)
Distritos con apertura tardía confirmada 12-abr-2026. Evidencia forense más sólida.

| # | Distrito | Locales pendientes | Prioridad |
|---|----------|---------------------|-----------|
| 1 | SAN JUAN DE MIRAFLORES | 15 | CRITICA |
| 2 | LURIN | 12 | CRITICA |
| 3 | PACHACAMAC | 9 | ALTA |
| 4 | CIENEGUILLA | 7 | ALTA |
| 5 | PUCUSANA | 6 | ALTA |
| 6 | VILLA EL SALVADOR | 5 | ALTA |

### Distribución sugerida 3 máquinas (Lote 1)
| Máquina | Distritos | Carga estimada |
|---------|-----------|----------------|
| MAQUINA_PRINCIPAL | SAN JUAN DE MIRAFLORES, VILLA EL SALVADOR | Alta (2 grandes) |
| MAQUINA_2 | LURIN, PACHACAMAC | Media |
| MAQUINA_3 | CIENEGUILLA, PUCUSANA | Baja (permite apoyar otros) |

## LOTE 2 — Siguientes 6 (demoras sin cifras oficiales)
VILLA MARIA DEL TRIUNFO, MIRAFLORES, SAN ISIDRO, SANTIAGO DE SURCO, SAN BORJA, PUNTA HERMOSA.
Activar SOLO tras completar Lote 1.

---

## CCA — Claude Certified Architect (5 dominios)

### 1. Agentic Architecture
- **Topología**: Hub-and-Spoke. Claude principal orquesta sub-agents (scraper-worker, ocr-reader).
- **Fan-Out**: scraping paralelo por distrito (workers=3-5).
- **Pipeline**: scrape → pdfs → extract → sync (comando `/trabajar`).
- **Exit conditions**: max retries=3, backoff exponencial en 429/503.
- **Handoff**: cada sub-agent recibe task + distrito + constraints, devuelve JSON estructurado.

### 2. Claude Code Config
- **Hooks activos**:
  - `SessionStart` → `scripts/mission_briefing.py` (estado + distrito + siguiente acción)
  - `PreToolUse(Bash)` → `.claude/guardrail.py` (bloqueo destructivo)
- **Permissions**: `defaultMode: bypassPermissions` en `settings.local.json` (ejecución autónoma).
- **Branches por máquina**: `maquina-1..6`, cada una con su `machine_config.json`.
- **Slash commands**: `/iniciar`, `/trabajar`, `/sync` en `.claude/commands/`.
- **Sub-agents**: `.claude/agents/scraper-worker.md`, `ocr-reader.md`.

### 3. Prompt Engineering
- **CLAUDE.md hierarchy**: global → neuracode → proyecto_nulidad (más específico gana).
- **Modo cavernicola**: max 3-8 palabras/bullet, 0 relleno.
- **Structured output**: JSON para sync (`sync/export/*.json`), no prosa.
- **Few-shot**: extractor Gemini con ejemplos de actas reales.
- **Role + task + constraints**: patrón obligatorio en `/trabajar [DISTRITO]`.

### 4. MCP & Tools
- **MCP activos (global)**: engram (memoria persistente), drawio (diagramas).
- **MCP deshabilitados**: Vercel, Vercel Next Dev Tools (no aplica aquí).
- **Tools propias**:
  - `browser_scraper` (Playwright stealth → API ONPE)
  - `gemini_extractor` (PDF → JSON estructurado)
  - `custody` (cadena de custodia SQLite)
- **Principio**: una tool = una acción. Idempotente. Inputs validados server-side.

### 5. Context & Reliability
- **Prioridad contexto**: CLAUDE.md proyecto > global > rules > memoria > tool results.
- **Pollution prevention**: Grep con `head_limit`, Read con `offset+limit`, diffs acotados.
- **Retry strategy**:
  - Transient (network ONPE): 3x backoff exponencial
  - Parse error (Gemini): 1x con instrucciones de formato
  - Budget exceeded: reportar progreso + stop graceful
- **Memoria persistente**: `~/.claude/projects/.../memory/MEMORY.md` (índice + entries por dominio).
- **Observabilidad**: `cadena_custodia` SQLite + logs estructurados.
- **Guardrails**: `.claude/guardrail.py` bloquea rm destructivo, git --force, DROP SQL, fork bombs.
