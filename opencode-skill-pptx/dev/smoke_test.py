# -*- coding: utf-8 -*-
"""Test de bout en bout avec le template de DEMO (fictif) : python dev/smoke_test.py"""
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SKILL = ROOT / ".opencode" / "skills" / "pptx-charte"
TOOL = SKILL / "scripts" / "pptx_tool.py"


def run(*args, cwd):
    env = dict(os.environ, PYTHONUTF8="1", PYTHONDONTWRITEBYTECODE="1")
    r = subprocess.run([sys.executable, str(TOOL)] + list(args), cwd=str(cwd), env=env, capture_output=True, text=True)
    print("$ run " + " ".join(args))
    print((r.stdout + r.stderr).rstrip()[-1500:])
    return r.returncode


def main():
    work = Path(tempfile.mkdtemp(prefix="pptx-charte-smoke-"))
    demo = ROOT / "dev" / "demo-template.potx"
    if not demo.exists():
        subprocess.check_call([sys.executable, str(ROOT / "dev" / "make_demo_template.py"), str(ROOT / "dev")])
    out = work / "out.pptx"
    steps = [
        ("doctor", "--template", str(demo)),
        ("catalog", "--template", str(demo), "--output", str(work / "catalog.md")),
        ("build", str(SKILL / "examples" / "exemple-deck.json"), "--template", str(demo), "-o", str(out)),
        ("validate", str(out), "--template", str(demo)),
        ("outline", str(out)),
    ]
    for s in steps:
        if run(*s, cwd=work) != 0:
            print("ECHEC a l'etape :", s[0])
            return 1
    pdemo = ROOT / "dev" / "demo-pattern-template.pptx"
    if not pdemo.exists():
        subprocess.check_call([sys.executable, str(ROOT / "dev" / "make_demo_pattern_template.py"), str(ROOT / "dev")])
    pout = work / "out-pattern.pptx"
    pspec = work / "pattern.json"
    pspec.write_text('{"slides":[{"layout":"slide:1","title":"Titre","content":["Sous-titre"]},'
                     '{"layout":"slide:2","title":"Trois points","content":[{"text":"A\\nTexte A"},{"text":"B\\nTexte B"},{"text":"C\\nTexte C"}]}]}',
                     encoding="utf-8")
    for s in [("build", str(pspec), "--template", str(pdemo), "-o", str(pout)),
              ("validate", str(pout), "--template", str(pdemo))]:
        if run(*s, cwd=work) != 0:
            print("ECHEC (mode diapo modele) a l'etape :", s[0])
            return 1
    print("\nOK - fichiers dans", work)
    return 0


if __name__ == "__main__":
    sys.exit(main())
