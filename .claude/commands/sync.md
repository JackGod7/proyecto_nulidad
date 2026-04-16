---
description: Exporta datos locales a JSON y los commitea al repo para sincronizar con otras máquinas
---

## Flujo

1. Ejecutar exporter:
```bash
uv run python src/sync/exporter.py
```
Esto genera `sync/export/<MACHINE_ID>_<DISTRITO>.json` por cada distrito configurado.

2. Mostrar archivos generados:
```bash
ls -la sync/export/
```

3. Commit y push (confirmar con usuario antes):
```bash
git add sync/export/*.json
git commit -m "sync: <MACHINE_ID> - datos distritos procesados"
git push origin main
```

4. Avisar al usuario que desde la máquina principal puede correr:
```bash
git pull && uv run python src/sync/merger.py
```

## Reglas
- Confirmar antes de git commit/push
- Si no hay archivos en sync/export → no hacer commit
- Un commit por sesión de sync (no uno por archivo)
