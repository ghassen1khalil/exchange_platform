#!/usr/bin/env sh
# Usage : make_wheelhouse.sh 3.12   (version de Python CIBLE Windows ; defaut 3.12)
V="${1:-3.12}"
OUT="$(cd "$(dirname "$0")" && pwd)/wheelhouse-$V"
python3 -m pip download -d "$OUT" --only-binary=:all: --platform win_amd64 --python-version "$V" --implementation cp \
  python-pptx lxml Pillow XlsxWriter typing_extensions && echo "Wheels pour Python $V dans : $OUT"
