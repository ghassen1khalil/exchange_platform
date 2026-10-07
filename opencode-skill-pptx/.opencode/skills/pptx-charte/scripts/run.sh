#!/usr/bin/env sh
# Lanceur macOS/Linux/Git-Bash du skill pptx-charte.  Usage : scripts/run.sh <commande> [options]
HERE="$(cd "$(dirname "$0")" && pwd)"
export PYTHONUTF8=1 PYTHONIOENCODING=utf-8 PYTHONDONTWRITEBYTECODE=1
for PY in "$HERE/../python/python.exe" python3 python py; do
  if command -v "$PY" >/dev/null 2>&1 || [ -x "$PY" ]; then
    if [ "$PY" = "py" ]; then exec py -3 "$HERE/pptx_tool.py" "$@"; fi
    "$PY" -c "import sys; assert sys.version_info >= (3, 8)" >/dev/null 2>&1 && exec "$PY" "$HERE/pptx_tool.py" "$@"
  fi
done
echo "[pptx-charte] Python 3.8+ introuvable." >&2
exit 9
