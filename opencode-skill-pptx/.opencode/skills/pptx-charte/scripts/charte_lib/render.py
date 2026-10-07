# -*- coding: utf-8 -*-
"""Rendu d'apercu (PNG + planche contact) via PowerPoint (COM) ou LibreOffice, si disponibles."""
from __future__ import annotations

import glob
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from .common import ToolError

_PS_EXPORT = r"""
$ErrorActionPreference = 'Stop'
$ppt = New-Object -ComObject PowerPoint.Application
try {
  $pres = $ppt.Presentations.Open($env:PPTX_IN, -1, 0, 0)
  try { $pres.Export($env:PNG_OUT, 'PNG', 1280, 720) } finally { $pres.Close() }
} finally {
  # ne jamais fermer PowerPoint si l'utilisateur a d'autres presentations ouvertes
  if ($ppt.Presentations.Count -eq 0) { $ppt.Quit() }
}
"""

def _have_powerpoint():
    if os.name != "nt":
        return False
    try:
        r = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command",
                            "if (Test-Path 'Registry::HKEY_CLASSES_ROOT\\PowerPoint.Application') {'yes'}"],
                           capture_output=True, text=True, timeout=30)
        return "yes" in (r.stdout or "")
    except Exception:
        return False


def _find_soffice():
    for name in ("soffice", "soffice.exe", "libreoffice"):
        p = shutil.which(name)
        if p:
            return p
    cands = []
    for env in ("ProgramFiles", "ProgramFiles(x86)", "LOCALAPPDATA"):
        base = os.environ.get(env)
        if base:
            cands += [os.path.join(base, "LibreOffice", "program", "soffice.exe"),
                      os.path.join(base, "Programs", "LibreOffice", "program", "soffice.exe")]
    for c in cands:
        if os.path.isfile(c):
            return c
    return None


def available_engines():
    eng = []
    if _have_powerpoint():
        eng.append("powerpoint")
    if _find_soffice():
        eng.append("libreoffice")
    return eng


def _export_powerpoint(pptx, out_dir):
    env = dict(os.environ, PPTX_IN=str(Path(pptx).resolve()), PNG_OUT=str(out_dir.resolve()))
    r = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command", _PS_EXPORT],
                       capture_output=True, text=True, env=env, timeout=300)
    if r.returncode != 0:
        raise ToolError("Export PowerPoint impossible : %s" % (r.stderr or r.stdout).strip()[:400])
    files = sorted(glob.glob(str(out_dir / "Slide*.PNG")) + glob.glob(str(out_dir / "Slide*.png")),
                   key=lambda f: int("".join(ch for ch in Path(f).stem if ch.isdigit()) or 0))
    return files


def _export_libreoffice(pptx, out_dir, soffice):
    tmp = Path(tempfile.mkdtemp(prefix="pptx-charte-"))
    r = subprocess.run([soffice, "--headless", "--convert-to", "pdf", "--outdir", str(tmp), str(Path(pptx).resolve())],
                       capture_output=True, text=True, timeout=300)
    pdfs = list(tmp.glob("*.pdf"))
    if not pdfs:
        raise ToolError("Conversion LibreOffice echouee : %s" % (r.stderr or r.stdout).strip()[:300])
    pdf = pdfs[0]
    shutil.copy(str(pdf), str(out_dir / "apercu.pdf"))
    pdftoppm = shutil.which("pdftoppm")
    if pdftoppm:
        subprocess.run([pdftoppm, "-png", "-r", "80", str(pdf), str(out_dir / "slide")], check=False, timeout=300)
        return sorted(glob.glob(str(out_dir / "slide-*.png")))
    try:
        import fitz  # PyMuPDF, optionnel
        doc = fitz.open(str(pdf))
        files = []
        for i, page in enumerate(doc, 1):
            f = out_dir / ("slide-%02d.png" % i)
            page.get_pixmap(dpi=80).save(str(f))
            files.append(str(f))
        return files
    except Exception:
        return []


def contact_sheet(files, out_file, cols=3, thumb_w=640):
    from PIL import Image
    thumbs = []
    for f in files:
        with Image.open(f) as im:
            im = im.convert("RGB")
            ratio = thumb_w / float(im.width)
            thumbs.append(im.resize((thumb_w, int(im.height * ratio))))
    if not thumbs:
        return None
    th = max(t.height for t in thumbs)
    rows = (len(thumbs) + cols - 1) // cols
    pad = 12
    sheet = Image.new("RGB", (cols * (thumb_w + pad) + pad, rows * (th + pad) + pad), (225, 225, 225))
    for i, t in enumerate(thumbs):
        r, c = divmod(i, cols)
        sheet.paste(t, (pad + c * (thumb_w + pad), pad + r * (th + pad)))
    sheet.save(str(out_file))
    return str(out_file)


def render(pptx, out_dir=None, engine="auto"):
    pptx = Path(pptx)
    if not pptx.is_file():
        raise ToolError("Fichier introuvable : %s" % pptx)
    out = Path(out_dir) if out_dir else pptx.with_suffix("").parent / (pptx.stem + "-apercu")
    if out.exists():
        for f in out.glob("*"):
            if f.is_file():
                try:
                    f.unlink()
                except OSError:
                    pass
    out.mkdir(parents=True, exist_ok=True)
    engines = available_engines()
    chosen = engine if engine != "auto" else (engines[0] if engines else None)
    if chosen is None:
        raise ToolError("Aucun moteur de rendu (PowerPoint ou LibreOffice) detecte : apercu impossible. "
                        "S'appuyer sur 'validate' et demander a l'utilisateur d'ouvrir le fichier.")
    if chosen == "powerpoint":
        files = _export_powerpoint(pptx, out)
    elif chosen == "libreoffice":
        so = _find_soffice()
        if not so:
            raise ToolError("LibreOffice introuvable.")
        files = _export_libreoffice(pptx, out, so)
    else:
        raise ToolError("Moteur inconnu : %s" % chosen)
    sheet = contact_sheet(files, out / "planche-contact.png") if files else None
    return chosen, files, sheet, out
