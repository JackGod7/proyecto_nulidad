#!/usr/bin/env bash
# Setup inicial para máquina nueva. Correr UNA VEZ tras clonar.
# Uso: bash scripts/setup_machine.sh

set -e

echo "============================================================"
echo "  PROYECTO NULIDAD - Setup Máquina Worker"
echo "============================================================"
echo ""

# 1. .env
if [ ! -f .env ]; then
  if [ -f .env.example ]; then
    cp .env.example .env
    echo "✓ .env creado desde .env.example"
    echo "  → EDITA .env y pon tu GEMINI_API_KEY antes de continuar"
  else
    echo "⚠  No hay .env.example. Crea .env manualmente con GEMINI_API_KEY."
  fi
else
  echo "✓ .env ya existe"
fi

# 2. machine_config.json
if [ ! -f machine_config.json ]; then
  cp machine_config.example.json machine_config.json
  echo "✓ machine_config.json creado"
  echo "  → EDITA machine_config.json:"
  echo "    - machine_id (ej: MAQUINA_2)"
  echo "    - distritos (ver CLAUDE.md asignación)"
else
  echo "✓ machine_config.json ya existe"
fi

# 3. uv sync
echo ""
echo "→ Instalando dependencias con uv..."
if command -v uv &> /dev/null; then
  uv sync
  echo "✓ Dependencias instaladas"
else
  echo "⚠  uv no instalado. Instálalo: https://docs.astral.sh/uv/"
  exit 1
fi

# 4. Playwright
echo ""
echo "→ Instalando Chromium (Playwright)..."
uv run playwright install chromium
echo "✓ Playwright listo"

# 5. Dirs
mkdir -p sync/export sync/import data
echo "✓ Directorios creados"

echo ""
echo "============================================================"
echo "  SETUP COMPLETO"
echo "============================================================"
echo ""
echo "SIGUIENTE PASO:"
echo "  1. Edita .env (GEMINI_API_KEY)"
echo "  2. Edita machine_config.json (machine_id + distritos)"
echo "  3. Abre Claude Code: claude"
echo "  4. Dentro de Claude ejecuta: /iniciar"
echo ""
