#!/usr/bin/env bash
# Propaga CLAUDE.md (con seccion CCA) a todas las branches de maquinas.
set -euo pipefail

BRANCHES=(main maquina-1 maquina-2 maquina-3 maquina-4 maquina-5 maquina-6)

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

ORIG_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
CLAUDE_MD="$(cat CLAUDE.md)"

for br in "${BRANCHES[@]}"; do
  echo "=== Branch: $br ==="
  git switch "$br"
  git pull origin "$br" --ff-only || true

  printf '%s\n' "$CLAUDE_MD" > CLAUDE.md
  git add CLAUDE.md
  if git diff --cached --quiet; then
    echo "$br: sin cambios"
  else
    git commit -m "docs($br): CLAUDE.md con dominios CCA"
    git push origin "$br"
  fi
done

git switch "$ORIG_BRANCH"
echo "=== LISTO ==="
