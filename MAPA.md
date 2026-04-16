# MAPA del Proyecto — Auditoría Forense ONPE 2026

Auditoría archivo por archivo. Enfoque Musk: cada archivo tiene un **dueño** y una **razón de existir**. Si no aporta → se elimina.

## Leyenda
- **USADO** ← código vivo en producción
- **LEGACY** ← reemplazado, mantener temporalmente como referencia
- **BORRAR** ← basura, eliminar

---

## Raíz

| Archivo | Estado | Función |
|---------|--------|---------|
| `CLAUDE.md` | USADO | Instrucciones Claude + quickstart + asignación distritos |
| `MAPA.md` | USADO | Este archivo — auditoría de archivos |
| `EQUIPO.md` | USADO | Documentación equipo/roles |
| `pyproject.toml` | USADO | Deps Python (uv) |
| `uv.lock` | USADO | Lock deps |
| `.env` | LOCAL | API keys (gitignored) |
| `.env.example` | USADO | Template |
| `machine_config.json` | LOCAL | Config por máquina (gitignored) |
| `machine_config.example.json` | USADO | Template config worker |
| `.gitignore` | USADO | Ignora .env, data/*.db, machine_config.json |

## src/ — Código

### src/scraping/ — Captura ONPE
| Archivo | Estado | Función |
|---------|--------|---------|
| `browser_scraper.py` | **USADO** | **Scraper v2 principal** (Playwright stealth, 2 fases) |
| `api_client.py` | LEGACY | Cliente httpx async — reemplazado por browser (CloudFront bloqueaba) |
| `scraper.py` | LEGACY | Scraper v1 httpx — reemplazado por browser_scraper |

### src/extraction/ — Extracción datos
| Archivo | Estado | Función |
|---------|--------|---------|
| `extractor.py` | USADO | Parser JSON API ONPE → votos normalizados |
| `instalacion_extractor.py` | **USADO** | **Extractor AI principal** (OpenAI gpt-4o-mini) — horas instalación |
| `gemini_extractor.py` | LEGACY | Extractor Gemini (reemplazado por OpenAI por precisión) |
| `ocr_hora.py` | LEGACY | OCR Tesseract — reemplazado por AI |
| `test_calibrar.py` | USADO | Tests calibración Gemini vs GPT (útil para A/B) |

### src/db/ — Base de datos
| Archivo | Estado | Función |
|---------|--------|---------|
| `schema.py` | **USADO** | **Schema v2 forense** + migración v1→v2 |
| `progress_db.py` | LEGACY | CRUD sobre progress.db v1 |

### src/audit/ — Auditoría forense
| Archivo | Estado | Función |
|---------|--------|---------|
| `integrity.py` | USADO | SHA-256 hashing PDFs (cadena de custodia) |

### src/reporting/ — Reportes
| Archivo | Estado | Función |
|---------|--------|---------|
| `reporte_estadistico.py` | **USADO** | **Excel para estadístico** (Miraflores horas + participación) |
| `gen_reporte.py` | LEGACY | Genera XLSX desde progress.db v1 |
| `progress_cli.py` | LEGACY | CLI resumen de progress.db v1 |

### src/sync/ — Sincronización multi-máquina
| Archivo | Estado | Función |
|---------|--------|---------|
| `exporter.py` | **USADO** | Exporta distritos a JSON (sync/export/) |
| `merger.py` | **USADO** | Importa JSON de otras máquinas a forensic.db |

### src/utils/ y src/config.py
| Archivo | Estado | Función |
|---------|--------|---------|
| `config.py` | USADO | Constantes: URLs API, rutas, ubigeos Lima |
| `utils/` | VACÍO | Dir vacío para utilidades futuras |

## scripts/ — Scripts de operación

| Archivo | Estado | Función |
|---------|--------|---------|
| `mission_briefing.py` | **USADO** | Hook SessionStart — muestra estado al abrir Claude |
| `setup_machine.sh` | **USADO** | Setup one-shot: deps + dirs + playwright |

## .claude/ — Configuración Claude Code

| Archivo | Estado | Función |
|---------|--------|---------|
| `settings.json` | USADO | Hook SessionStart + permisos |
| `settings.local.json` | LOCAL | Permisos específicos máquina |
| `commands/iniciar.md` | USADO | `/iniciar` — briefing + plan |
| `commands/trabajar.md` | USADO | `/trabajar DISTRITO` — pipeline scrape+pdfs+extract |
| `commands/sync.md` | USADO | `/sync` — export+commit+push |
| `agents/ocr-reader.md` | USADO | Agente OCR local |
| `agents/scraper-worker.md` | USADO | Agente worker scraping |
| `rules/cavernicola.md` | USADO | Reglas modo cavernícola |
| `rules/scraper.md` | USADO | Reglas scraping ONPE |

## tests/

| Archivo | Estado | Función |
|---------|--------|---------|
| `test_extractor.py` | USADO | Tests extractor JSON API |

## data/ — Datos (gitignored casi todo)

### data/ (raíz)
| Archivo | Estado | Función |
|---------|--------|---------|
| `forensic.db` | **USADO** | DB SQLite v2 forense — 26,800+ actas (172 MB) |
| `forensic.db-shm` | USADO | SQLite WAL shared memory |
| `forensic.db-wal` | USADO | SQLite WAL log |
| `ANCÓN/` | USADO | PDFs distrito ANCÓN (~311 archivos, 140 MB) |
| `MIRAFLORES/` | USADO | PDFs MIRAFLORES (~1293, 648 MB) |
| `SAN JUAN DE MIRAFLORES/` | USADO | PDFs SJM (~2023, 920 MB) |

### data/exports/ — CSVs derivados
| Archivo | Estado | Función |
|---------|--------|---------|
| `actas_sin_pdf.csv` | USADO | 442 mesas pendientes/sin archivos |
| `instalaciones_miraflores.csv` | USADO | 418 horas extraídas (output antiguo OCR) |

### data/reports/ — Excel generados
| Archivo | Estado | Función |
|---------|--------|---------|
| `AVANCE_SCRAPING_20260415_1628.xlsx` | LEGACY | Snapshot avance (timestamp) |
| `AVANCE_SCRAPING_20260415_1644.xlsx` | LEGACY | Snapshot avance |
| `AVANCE_SCRAPING_20260415_1805.xlsx` | LEGACY | Snapshot avance |
| `MIRAFLORES_estadistico.xlsx` | USADO | Reporte entregable estadístico |

### data/logs/ — Logs
| Archivo | Estado | Función |
|---------|--------|---------|
| `instalaciones.log` | LEGACY | Log extracción (420 KB) |
| `instalaciones2.log` | LEGACY | Log extracción v2 (8 KB) |

### data/legacy/ — v1 obsoleto (no borrar todavía)
| Archivo | Estado | Función |
|---------|--------|---------|
| `progress.db` | LEGACY | DB v1 (33 MB) — ya migrado a forensic.db |
| `dataset_lima_provincia.csv` | LEGACY | CSV v1 (29 MB) — ya en forensic.db |

### data/_BORRAR_MANUAL/ ← ELIMINAR MANUALMENTE
Archivos de exploración/desarrollo sin valor forense.

**Ruta a borrar:** `C:\Users\jaaguilar\Documents\neuracode\proyecto_nulidad\data\_BORRAR_MANUAL\`

Contenido:
- `recon_01_actas.png` ... `recon_05_ancon.png` (5 screenshots reconnaissance ONPE)
- `preview_escrutinio.png`, `preview_instalacion.png`, `preview_sufragio.png` (3 previews desarrollo)

Borrar con:
```bash
rm -rf data/_BORRAR_MANUAL/
```

---

## Archivos LEGACY — Rutas para eliminación manual

Estos archivos **NO rompen nada si los borras**, pero los dejo para referencia histórica. Si confirmas que no los necesitas, ejecuta:

```bash
# src/scraping legacy (reemplazados por browser_scraper)
rm src/scraping/api_client.py
rm src/scraping/scraper.py

# src/extraction legacy (reemplazados por instalacion_extractor.py)
rm src/extraction/gemini_extractor.py
rm src/extraction/ocr_hora.py

# src/db legacy (v1 obsoleto)
rm src/db/progress_db.py

# src/reporting legacy (dependen de progress_db v1)
rm src/reporting/gen_reporte.py
rm src/reporting/progress_cli.py

# data legacy (migrado a forensic.db)
rm -rf data/legacy/

# data logs antiguos
rm -rf data/logs/

# reports viejos (solo dejar MIRAFLORES_estadistico)
rm data/reports/AVANCE_SCRAPING_*.xlsx
```

**Total liberable: ~65 MB en legacy + 8 MB src** (si borras todo).

---

## Flujo actual verificado

1. `bash scripts/setup_machine.sh` ✓
2. Editar `.env` + `machine_config.json` ✓
3. `claude` → SessionStart hook corre `mission_briefing.py` ✓
4. `/iniciar` → briefing manual ✓
5. `/trabajar DISTRITO` → pipeline completo
6. `/sync` → push a repo

**Datos actuales (máquina principal):**
- Scraping: 1870/2278 actas (82%) en SJM + VES
- PDFs: 663 SJM completos, 418 MIRAFLORES extraídos
- Gemini/GPT: 417 actas Miraflores con hora confirmada
