# -*- coding: utf-8 -*-
"""Analyse du template -> catalogue Markdown (layouts, couleurs, polices, icones)."""
from __future__ import annotations

import datetime
import re
from pathlib import Path

from .common import (NSMAP, classify_layout, collect_layouts, default_table_style, layout_slots, load_config,
                     theme_info, to_cm)
from . import pattern as pat
from .icons import IconLibrary
from .textfit import effective_font_pt, slot_capacity

ROLE_LABEL = {
    "cover": "page de garde", "section": "separateur de section / chapitre", "agenda": "sommaire / agenda",
    "title_only": "titre seul (zone libre : graphique, schema, image...)", "content": "titre + 1 zone de contenu",
    "two_content": "titre + 2 zones (colonnes)", "three_content": "titre + 3 zones ou plus",
    "comparison": "comparaison", "picture": "titre + visuel", "blank": "vierge", "end": "fin / contact / remerciements",
    "other": "autre",
}

_SRGB = re.compile(r'srgbClr val="([0-9A-Fa-f]{6})"')


def inspect_template(prs, template_path, cfg=None):
    cfg = cfg or load_config()
    th = theme_info(prs)
    sw, sh = prs.slide_width, prs.slide_height
    layouts = []
    for lr in collect_layouts(prs):
        by_kind, content = layout_slots(lr.layout, sh)
        role = classify_layout(lr.layout, sh)
        slots = []
        for kind in ("title", "subtitle"):
            for s in by_kind.get(kind, []):
                slots.append(_slot_dict(kind, None, s, lr.layout, cfg))
        for i, s in enumerate(content, 1):
            slots.append(_slot_dict("content", i, s, lr.layout, cfg))
        for kind in ("footer", "slidenum", "date"):
            for s in by_kind.get(kind, []):
                slots.append(_slot_dict(kind, None, s, lr.layout, cfg))
        decor = []
        for shp in lr.layout.shapes:
            if not shp.is_placeholder:
                decor.append("%s \"%s\"" % (str(shp.shape_type).split(".")[-1].split(" ")[0], shp.name))
        layouts.append({"index": lr.index, "master": lr.master_index + 1, "name": lr.name, "role": role,
                        "slots": slots, "decor": decor})
    samples = []
    for n, slide in enumerate(prs.slides, 1):
        texts = []
        for shp in slide.shapes:
            if shp.has_text_frame and shp.text_frame.text.strip():
                texts.append(shp.text_frame.text.strip().replace("\n", " / ")[:80])
        samples.append({"n": n, "layout": slide.slide_layout.name, "texts": texts[:4]})
    patterns = []
    for n, slide in enumerate(prs.slides, 1):
        title, body = pat.analyze_slide(slide, sw, sh)
        slots = []
        if title is not None:
            slots.append({"label": "title", "name": title.name, "desc": pat.describe_slot(title),
                          "w": to_cm(title.width), "h": to_cm(title.height), "x": to_cm(title.left), "y": to_cm(title.top)})
        for i, sl in enumerate(body):
            slots.append({"label": "content[%d]" % i, "name": sl.name, "desc": pat.describe_slot(sl),
                          "w": to_cm(sl.width), "h": to_cm(sl.height), "x": to_cm(sl.left), "y": to_cm(sl.top)})
        patterns.append({"n": n, "layout": slide.slide_layout.name, "summary": pat.summarize(title, body),
                         "slots": slots, "sample": (samples[n - 1]["texts"][0] if samples[n - 1]["texts"] else "")})
    usable = [l for l in layouts if any(s["kind"] == "title" for s in l["slots"]) and any(s["kind"] == "content" for s in l["slots"])]
    lib = IconLibrary.build(prs, cfg)
    icons = [{"name": e.name, "w": to_cm(e.width), "h": to_cm(e.height), "source": e.source} for e in lib.listing()]
    # couleurs "en dur" dans master/layouts (aide a la maintenance)
    hard = {}
    for m in prs.slide_masters:
        for el in [m._element] + [l._element for l in m.slide_layouts]:
            from lxml import etree
            for h in _SRGB.findall(etree.tostring(el).decode("utf-8", "ignore")):
                hard[h.upper()] = hard.get(h.upper(), 0) + 1
    theme_hex = set(th["colors"].values())
    hard_extra = {k: v for k, v in hard.items() if k not in theme_hex}
    return {
        "file": Path(template_path).name,
        "generated": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "slide_w_cm": to_cm(sw), "slide_h_cm": to_cm(sh),
        "ratio": "16:9" if abs(sw / float(sh) - 16 / 9.0) < 0.02 else ("4:3" if abs(sw / float(sh) - 4 / 3.0) < 0.02 else "autre"),
        "theme": th, "layouts": layouts, "samples": samples, "patterns": patterns, "usable_layouts": len(usable), "icons": icons,
        "table_style": default_table_style(prs), "masters": len(prs.slide_masters),
        "hard_colors": hard_extra,
    }


def _slot_dict(kind, ordinal, s, layout, cfg):
    d = {
        "kind": kind, "ordinal": ordinal, "idx": s.idx, "type": s.type_name, "name": s.name,
        "x": to_cm(s.left), "y": to_cm(s.top), "w": to_cm(s.width), "h": to_cm(s.height), "prompt": s.prompt.split("\n")[0][:60],
    }
    if kind in ("title", "subtitle", "content") and s.width and s.height and s.type_name not in ("PICTURE", "TABLE", "CHART"):
        try:
            pt = effective_font_pt(s.ph, 0)
            lim = cfg["limits"]
            cpl, lines = slot_capacity(s.width, s.height, pt, lim["avg_char_width_em"], lim["line_height_em"])
            d["font_pt"] = pt
            d["cap"] = "~%d car./ligne, ~%d lignes" % (cpl, lines)
        except Exception:
            pass
    return d


def to_markdown(info):
    L = []
    th = info["theme"]
    L.append("# Catalogue du template : %s" % info["file"])
    L.append("")
    L.append("_Genere automatiquement le %s par `run catalog` - ne pas editer a la main._" % info["generated"])
    L.append("")
    L.append("- Format : **%s x %s cm** (%s)" % (info["slide_w_cm"], info["slide_h_cm"], info["ratio"]))
    L.append("- Polices du theme : titres **%s**, texte **%s** (heritees automatiquement : ne jamais les forcer)" % (
        th["major_font"], th["minor_font"]))
    L.append("- Jeu de couleurs : %s" % (th["color_scheme_name"] or "-"))
    L.append("- Nombre de layouts : **%d**  (masters : %d)" % (len(info["layouts"]), info["masters"]))
    L.append("- Style de tableau par defaut : `%s`" % info["table_style"])
    L.append("")
    L.append("## Diagnostic")
    L.append("")
    nl, npat = info["usable_layouts"], len([p for p in info["patterns"] if p["slots"]])
    L.append("- Layouts avec titre + zone(s) de contenu : **%d** -> mode `\"layout\": \"<nom>\"` %s" % (
        nl, "(exploitable)" if nl >= 3 else "(peu de layouts : preferer les diapos modeles)"))
    L.append("- Diapos d'exemple utilisables comme modeles : **%d** -> mode `\"layout\": \"slide:N\"` %s" % (
        npat, "(voir plus bas)" if npat else ""))
    lay_title = {l["name"] for l in info["layouts"] if any(x["kind"] == "title" for x in l["slots"])}
    on_struct = sum(1 for sm in info["samples"] if sm["layout"] in lay_title)
    if info["samples"] and on_struct < 0.5 * len(info["samples"]):
        L.append("- **Constat : la plupart des diapos d'exemple n'utilisent pas de layout a placeholders (%d/%d) : le design "
                 "est porte par les diapos -> utiliser en priorite `slide:N`.**" % (len(info["samples"]) - on_struct, len(info["samples"])))
    L.append("- Choix : la conformite maximale s'obtient en reprenant une **diapo modele** quand le design (cartes, pictos, "
             "colonnes decorees) vit dans les diapos ; un **layout** quand le template definit de vrais placeholders.")
    if info.get("preview"):
        L.append("- Apercu visuel des diapos du template : `%s` (planche) et images par diapo dans le meme dossier." % info["preview"])
    L.append("")
    L.append("## Palette (couleurs du theme)")
    L.append("")
    L.append("| Role | Hex | Nom utilisable dans la spec (extras.color) |")
    L.append("|---|---|---|")
    for k in ("dk1", "lt1", "dk2", "lt2", "accent1", "accent2", "accent3", "accent4", "accent5", "accent6", "hlink"):
        if k in th["colors"]:
            L.append("| %s | #%s | `%s` |" % (k, th["colors"][k], k))
    if info["hard_colors"]:
        L.append("")
        L.append("Couleurs en dur trouvees dans le master/layouts (hors theme) : %s" % ", ".join(
            "#%s" % h for h in sorted(info["hard_colors"])))
    L.append("")
    L.append("## Layouts disponibles")
    L.append("")
    L.append("Utiliser le **nom exact** dans `layout` (ou `@role`). `content[n]` = n-ieme bloc du champ `content` "
             "(ordre de lecture : haut->bas, gauche->droite).")
    L.append("")
    for lay in info["layouts"]:
        L.append("### %d. \"%s\"" % (lay["index"], lay["name"]))
        L.append("")
        L.append("- Usage probable : %s  (`@%s`)" % (ROLE_LABEL.get(lay["role"], lay["role"]), lay["role"]))
        for s in lay["slots"]:
            if s["kind"] == "content":
                label = "content[%d]" % (s["ordinal"] - 1)
            else:
                label = {"title": "title", "subtitle": "subtitle", "footer": "(pied de page)",
                         "slidenum": "(numero de diapo, auto)", "date": "(date)"}[s["kind"]]
            line = "- `%s` -> idx %s, %s, \"%s\" - %s x %s cm @ (%s, %s)" % (
                label, s["idx"], s["type"], s["name"], s["w"], s["h"], s["x"], s["y"])
            if "cap" in s:
                line += " - %s pt, %s" % (int(s["font_pt"]) if s["font_pt"] == int(s["font_pt"]) else s["font_pt"], s["cap"])
            if s["prompt"] and s["kind"] in ("title", "subtitle", "content"):
                line += " - invite : \"%s\"" % s["prompt"]
            L.append(line)
        if lay["decor"]:
            L.append("- Elements graphiques du layout (non modifiables) : %s" % ", ".join(lay["decor"][:6]))
        L.append("")
    pats = [p for p in info["patterns"] if p["slots"]]
    if pats:
        L.append("## Diapos modeles (a utiliser avec `\"layout\": \"slide:N\"`)")
        L.append("")
        L.append("La diapo est **clonee** (fond, formes, pictos, couleurs, polices) ; seules les zones listees sont remplacees, "
                 "en conservant la mise en forme du texte d'exemple. `title` = zone titre ; `content[i]` = i-eme bloc de `content`. "
                 "Un `text` de plusieurs lignes (`\\n`) reprend la mise en forme des paragraphes d'exemple dans l'ordre "
                 "(ex. intertitre gras puis texte). Les zones non remplies sont supprimees.")
        L.append("")
        for pt in pats:
            L.append("### slide:%d - %s" % (pt["n"], pt["summary"]))
            L.append("")
            L.append("- Layout PowerPoint sous-jacent : \"%s\" ; texte d'exemple du titre : \"%s\"" % (pt["layout"], (pt["sample"] or "-")[:60]))
            for sl in pt["slots"]:
                L.append("- `%s` -> \"%s\" - %s - %s x %s cm @ (%s, %s)" % (sl["label"], sl["name"], sl["desc"], sl["w"], sl["h"], sl["x"], sl["y"]))
            L.append("")
    L.append("## Icones disponibles")
    L.append("")
    if info["icons"]:
        L.append("Utilisation : `{\"icon\": \"nom\"}` dans un bloc `content`, ou dans `extras` avec x/y/size (cm).")
        L.append("")
        for ic in info["icons"]:
            L.append("- `%s` (%s x %s cm) - %s" % (ic["name"], ic["w"], ic["h"], ic["source"]))
    else:
        L.append("Aucune icone detectee. Ajoutez des PNG dans `assets/icons/` ou indiquez les diapos de la "
                 "bibliotheque d'icones dans `config/charte.json` (`icon_library_slides`).")
    L.append("")
    return "\n".join(L)
