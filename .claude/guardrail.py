#!/usr/bin/env python3
"""PreToolUse guardrail: bloquea operaciones destructivas en modo autonomo.

Exit 0 = permitir. Exit 2 = bloquear con mensaje en stderr.
"""
from __future__ import annotations

import json
import re
import sys

DANGEROUS_BASH_PATTERNS = [
    (r"\brm\s+(-[rRf]+\s+)*(/|~|\*|\.|C:|/c/)", "rm sobre raiz/home/wildcard bloqueado"),
    (r"\brm\s+(-[rRf]+\s+)*.*forensic\.db", "borrar forensic.db prohibido"),
    (r"\brm\s+(-[rRf]+\s+)*.*\.env\b", "borrar .env prohibido"),
    (r"\bgit\s+push\s+.*--force\b", "git push --force bloqueado"),
    (r"\bgit\s+push\s+.*-f\b", "git push -f bloqueado"),
    (r"\bgit\s+push\s+.*--no-verify\b", "git push --no-verify bloqueado"),
    (r"\bgit\s+reset\s+--hard\s+(origin|HEAD~|[0-9a-f]{7,})", "git reset --hard requiere confirmacion"),
    (r"\bgit\s+clean\s+-[a-z]*f", "git clean -f bloqueado"),
    (r"\bgit\s+branch\s+-D\b", "git branch -D bloqueado"),
    (r"\bgit\s+checkout\s+\.\s*$", "git checkout . (descarta cambios) bloqueado"),
    (r"\bdrop\s+(database|table|schema)\b", "SQL DROP bloqueado"),
    (r"\btruncate\s+table\b", "SQL TRUNCATE bloqueado"),
    (r":\(\)\s*\{.*:\|:&\s*\}\s*;:", "fork bomb bloqueado"),
    (r"\bchmod\s+(-R\s+)?777\b", "chmod 777 bloqueado"),
    (r"\bcurl\s+.*\|\s*(sudo\s+)?(bash|sh)\b", "curl | bash bloqueado"),
    (r"\bwget\s+.*\|\s*(sudo\s+)?(bash|sh)\b", "wget | bash bloqueado"),
    (r"\bdd\s+.*of=/dev/", "dd a /dev/ bloqueado"),
    (r">\s*/dev/sd[a-z]", "escritura a /dev/sd* bloqueada"),
    (r"\bmkfs\.", "mkfs bloqueado"),
    (r"\bformat\s+[A-Z]:", "format disco bloqueado"),
]

PROTECTED_WRITE_PATHS = [
    r"^\.env$",
    r"forensic\.db$",
    r"machine_config\.json$",
]


def check_bash(cmd: str) -> str | None:
    for pattern, msg in DANGEROUS_BASH_PATTERNS:
        if re.search(pattern, cmd, re.IGNORECASE):
            return msg
    return None


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0

    tool_name = payload.get("tool_name", "")
    tool_input = payload.get("tool_input", {}) or {}

    if tool_name == "Bash":
        cmd = tool_input.get("command", "")
        reason = check_bash(cmd)
        if reason:
            print(f"[GUARDRAIL] BLOQUEADO: {reason}\nComando: {cmd}", file=sys.stderr)
            return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
