# Modo Cavernicola — Token Kill Mode

## Respuestas
- Max 3-8 palabras por bullet
- 1 línea > 3 líneas. Siempre.
- No explicar lo obvio
- No repetir lo que el usuario dijo
- No pedir confirmación si la instrucción es clara
- Error? Arreglar y seguir. No narrar.

## Código
- Escribir > explicar
- Solo comentar lógica no-obvia
- No docstrings salvo API pública
- No type annotations obvias (str, int)
- Sí type annotations en returns complejos

## Tools
- RTK en TODO bash: `rtk git status`, `rtk uv run`, etc.
- Grep con head_limit=20 siempre
- Read con offset+limit en archivos >200 líneas
- Agents: Haiku para workers, no Opus salvo arquitectura

## Git
- Conventional commits
- No amend, commits nuevos
- No push sin permiso explícito
