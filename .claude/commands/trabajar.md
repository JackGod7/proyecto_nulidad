---
description: Ejecuta pipeline completo del distrito actual (scrape → pdfs → extraer)
argument-hint: [distrito opcional, ej: LURIN]
---

Pipeline por distrito. Si se pasa distrito como argumento `$ARGUMENTS`, usar ese. Si no, leer de `machine_config.json` el primer distrito pendiente.

## Flujo (ejecutar en orden, confirmar con usuario entre fases)

### Fase 1 — Scraping actas API ONPE
```bash
uv run python -c "from src.scraping.browser_scraper import main; import asyncio; asyncio.run(main(fase=1, workers=5, filtro_distritos=['$DISTRITO']))"
```
Verificar en DB: `sqlite3 data/forensic.db "SELECT COUNT(*) FROM actas WHERE distrito='$DISTRITO';"`

### Fase 2 — Descarga PDFs instalación
```bash
uv run python -c "from src.scraping.browser_scraper import main; import asyncio; asyncio.run(main(fase=2, workers=3, filtro_distritos=['$DISTRITO']))"
```
Verificar: `ls data/$DISTRITO/ | wc -l`

### Fase 3 — Extracción Gemini (actas instalación)
```bash
uv run python src/extraction/instalacion_extractor.py --distrito "$DISTRITO"
```
Verificar: `sqlite3 data/forensic.db "SELECT COUNT(*) FROM instalaciones WHERE distrito='$DISTRITO' AND error IS NULL;"`

### Fase 4 — Análisis horas
Mostrar distribución de horas instalación del distrito:
```sql
SELECT CASE
  WHEN hora_instalacion_min < 450 THEN '< 07:30 (normal)'
  WHEN hora_instalacion_min < 480 THEN '07:30-07:59'
  WHEN hora_instalacion_min < 540 THEN '08:00-08:59'
  WHEN hora_instalacion_min < 600 THEN '09:00-09:59'
  WHEN hora_instalacion_min < 660 THEN '10:00-10:59'
  ELSE '>= 11:00 (critico)'
END as bloque, COUNT(*)
FROM instalaciones WHERE distrito='$DISTRITO' GROUP BY bloque;
```

### Fase 5 — Proponer sync
Al finalizar, sugerir `/sync` para exportar y subir al repo.

## Reglas
- Modo cavernicola: respuestas cortas
- Entre cada fase, confirmar con el usuario antes de seguir
- Si una fase falla, NO continuar, reportar error
