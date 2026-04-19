#!/usr/bin/env bash
# Propaga settings autonomos + guardrail a TODAS las branches de maquinas.
# Uso: bash .claude/propagar_autonomous.sh
set -euo pipefail

BRANCHES=(main maquina-1 maquina-2 maquina-3 maquina-4 maquina-5 maquina-6)

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

ORIG_BRANCH="$(git rev-parse --abbrev-ref HEAD)"

# Archivos fuente desde la branch actual
SETTINGS_JSON="$(cat .claude/settings.json)"
SETTINGS_LOCAL="$(cat .claude/settings.local.json)"
GUARDRAIL_PY="$(cat .claude/guardrail.py)"

for br in "${BRANCHES[@]}"; do
  echo "=== Branch: $br ==="
  git switch "$br"
  git pull origin "$br" --ff-only || true

  mkdir -p .claude
  printf '%s\n' "$SETTINGS_JSON" > .claude/settings.json
  printf '%s\n' "$SETTINGS_LOCAL" > .claude/settings.local.json
  printf '%s\n' "$GUARDRAIL_PY" > .claude/guardrail.py
  chmod +x .claude/guardrail.py

  git add .claude/settings.json .claude/settings.local.json .claude/guardrail.py
  if git diff --cached --quiet; then
    echo "$br: sin cambios"
  else
    git commit -m "chore($br): bypassPermissions + guardrail autonomo"
    git push origin "$br"
  fi
done

git switch "$ORIG_BRANCH"
echo "=== LISTO ==="
