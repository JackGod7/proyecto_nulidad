---
description: Muestra estado actual, distrito asignado y siguiente acción
---

Ejecuta `uv run python scripts/mission_briefing.py` y analiza el output.

Luego:
1. Si falta `machine_config.json` → avisar al usuario que copie desde `.example` y edite
2. Si faltan distritos configurados → pedir al usuario qué distritos trabajar
3. Si hay distrito pendiente → sugerir correr `/trabajar`
4. Si todo está procesado → sugerir correr `/sync`

No ejecutar comandos de scraping/extracción sin confirmación explícita del usuario.
