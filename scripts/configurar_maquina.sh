#!/usr/bin/env bash
# Configura machine_config.json segun operador.
# Uso: bash scripts/configurar_maquina.sh <jack|lynn|jim|hector>

set -e

OPERADOR="${1:-}"

if [[ -z "$OPERADOR" ]]; then
  echo "Uso: bash scripts/configurar_maquina.sh <jack|lynn|jim|hector>"
  exit 1
fi

SRC="configs/${OPERADOR}.json"

if [[ ! -f "$SRC" ]]; then
  echo "ERROR: no existe $SRC"
  echo "Operadores validos: jack, lynn, jim, hector"
  exit 1
fi

cp "$SRC" machine_config.json

echo "[OK] machine_config.json <- $SRC"
echo ""
cat machine_config.json
echo ""
echo "Siguiente paso:"
echo "  git checkout distrito/<tu-distrito>"
echo "  claude   # luego /iniciar"
