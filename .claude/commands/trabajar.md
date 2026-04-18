---
description: Pipeline completo por distrito — scrape → pdfs → extraer → export → verify
argument-hint: [slug distrito, ej: VMT o CHORRILLOS]
---

**Flujo por distrito** (forensic v2 — una branch, un distrito).

Si `$ARGUMENTS` está vacío, leer `machine_config.json` → primer distrito pendiente.

## Pre-check

```bash
git branch --show-current    # debe ser distrito/<slug>
```

Si no estás en la branch correcta:
```bash
git checkout distrito/<slug>
```

## Fase 1 — Scrape actas (API ONPE)
```bash
uv run python -c "from src.scraping.browser_scraper import main; import asyncio; asyncio.run(main(fase=1, workers=5, filtro_distritos=['$DISTRITO']))"
```

Verificar: `uv run python -m src.db.repair` → sección distrito.

## Fase 2 — Descarga PDFs instalación
```bash
uv run python -c "from src.scraping.browser_scraper import main; import asyncio; asyncio.run(main(fase=2, workers=3, filtro_distritos=['$DISTRITO']))"
```

## Fase 3 — Extracción OpenAI / Gemini (hora instalación)
```bash
uv run python -m src.extraction.instalacion_extractor "$DISTRITO"
```

## Fase 4 — Borrar PDFs procesados (liberar disco)
```bash
uv run python -c "
import sqlite3, os
from pathlib import Path
conn = sqlite3.connect('data/forensic.db')
rows = conn.execute('''
    SELECT p.archivo_id, p.distrito, p.nombre_destino
    FROM pdfs p JOIN instalaciones i ON i.archivo_id=p.archivo_id
    WHERE p.tipo=3 AND i.hora_instalacion_min IS NOT NULL
      AND p.distrito LIKE ?
''', (f'%$DISTRITO%',)).fetchall()
for r in rows:
    p = Path('data/distritos') / r[1].upper() / Path(r[2]).name
    if p.exists():
        p.unlink()
        conn.execute('UPDATE pdfs SET archivo_en_disco=0 WHERE archivo_id=?', (r[0],))
conn.commit()
print(f'Borrados {len(rows)} PDFs')
"
```

## Fase 5 — Export NDJSON+gzip+manifest
```bash
uv run python -m src.sync.exporter_v2 "$DISTRITO"
```

## Fase 6 — Verificar manifest (STRICT)
```bash
uv run python -m src.sync.verifier "$(python -c 'import unicodedata;s=unicodedata.normalize(\"NFKD\",\"$DISTRITO\");print(\"\".join(c for c in s if not unicodedata.combining(c)).upper().replace(\" \",\"_\"))')"
```

Si falla → NO commitear. Re-exportar.

## Fase 7 — Commit + push branch distrito
```bash
git add sync/export/ data/forensic.db  # DB local no se sube (gitignored)
git commit -m "feat(distrito): $DISTRITO scrape+extract+export"
git push origin distrito/<slug>
```

Pre-commit hook verifica manifest automáticamente. Si falla → commit rechazado.

## Fase 8 — PR al main (cuando gap=0)

Solo cuando todas las actas del distrito tienen `hora_instalacion_min IS NOT NULL`:
```bash
gh pr create --title "distrito: $DISTRITO COMPLETO" --body "Gap=0. Manifest hash en sync/export/<slug>/manifest.json"
```

## Reglas duras

1. **1 operador = 1 distrito activo.** Termina antes de pasar al siguiente.
2. **Pre-commit hook** bloquea si manifest no verifica.
3. **PDFs nunca al repo** (solo DB + NDJSON+gz).
4. **Respuestas cortas** (modo cavernicola).
5. **Si falla una fase** → NO continuar, reportar al operador.
