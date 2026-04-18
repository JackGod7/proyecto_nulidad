#!/usr/bin/env bash
# Instala pre-commit hook que verifica manifest forense.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOOK_DIR="$ROOT/.git/hooks"
HOOK_FILE="$HOOK_DIR/pre-commit"

mkdir -p "$HOOK_DIR"

cat > "$HOOK_FILE" << 'EOF'
#!/usr/bin/env bash
# Pre-commit: verifica manifest.json de sync/export/ distritos modificados.
# STRICT: rechaza commit si cualquier hash no coincide.

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

# Solo correr si hay cambios en sync/export/
if ! git diff --cached --name-only | grep -qE '^sync/export/'; then
    exit 0
fi

echo "[pre-commit] Verificando manifest.json de distritos modificados..."

if ! uv run python -m src.sync.verifier --changed --quiet; then
    echo ""
    echo "ERROR: verificación manifest falló. Commit rechazado."
    echo "Si el manifest está desactualizado, re-exporta con:"
    echo "  uv run python -m src.sync.exporter_v2 \"DISTRITO\""
    exit 1
fi

echo "[pre-commit] OK"
exit 0
EOF

chmod +x "$HOOK_FILE"
echo "Pre-commit hook instalado en $HOOK_FILE"
