# -*- coding: utf-8 -*-
"""Estimation de la taille de police effective et du risque de debordement.

Sans moteur de rendu on ne peut qu'estimer : l'objectif est d'attraper les gros
debordements (texte 2x trop long), pas de mesurer au pixel pres.
"""
from __future__ import annotations

import math

from .common import A_NS, NSMAP, EMU_PER_PT

_TITLE_TYPES = {"title", "ctrTitle", "vertTitle"}
DEFAULT_INSETS = {"lIns": 91440, "tIns": 45720, "rIns": 91440, "bIns": 45720}


def _ph_type_attr(el):
    ph = el.find(".//p:nvPr/p:ph", NSMAP)
    if ph is None:
        return None, None
    return ph.get("type") or "obj", ph.get("idx") or "0"


def _shape_el(shape):
    return shape._element


def _txstyle_kind(ph_type):
    if ph_type is None:
        return "otherStyle"
    if ph_type in _TITLE_TYPES:
        return "titleStyle"
    if ph_type in ("body", "obj", "subTitle", "tbl", "chart", "pic", "media", "clipArt", "dgm"):
        return "bodyStyle"
    return "otherStyle"


def _chain(shape):
    """[element de la forme, element placeholder du layout, element placeholder du master]."""
    chain = [_shape_el(shape)]
    ph_type, ph_idx = _ph_type_attr(chain[0])
    if ph_type is None:
        return chain, None, None
    part = shape.part
    layout = master = None
    try:
        slide = part.slide
        layout = slide.slide_layout
    except Exception:
        layout = getattr(part, "slide_layout", None)
    master = None
    if layout is not None:
        for ph in layout.placeholders:
            if str(ph.placeholder_format.idx) == str(ph_idx) and ph._element is not chain[0]:
                chain.append(ph._element)
                break
        try:
            master = layout.slide_master
        except Exception:
            master = None
    else:
        try:
            master = part.slide_master
        except Exception:
            master = None
    if master is not None:
        for ph in master.placeholders:
            t, _ = _ph_type_attr(ph._element)
            if t is None:
                continue
            same = (t == ph_type) or (t in ("body", "obj") and ph_type in ("body", "obj", "subTitle")) \
                or (t in _TITLE_TYPES and ph_type in _TITLE_TYPES)
            if same:
                chain.append(ph._element)
                break
    return chain, ph_type, master


def _lvl_size(el, level):
    xp = ".//a:lstStyle/a:lvl%dpPr/a:defRPr" % (level + 1)
    for d in el.iterfind(xp, NSMAP):
        sz = d.get("sz")
        if sz:
            return int(sz) / 100.0
    return None


def effective_font_pt(shape, level=0):
    chain, ph_type, master = _chain(shape)
    for el in chain:
        v = _lvl_size(el, level)
        if v:
            return v
    if master is not None:
        kind = _txstyle_kind(ph_type)
        tx = master._element.find(".//p:txStyles/p:%s" % kind, NSMAP)
        if tx is not None:
            d = tx.find("a:lvl%dpPr/a:defRPr" % (level + 1), NSMAP)
            if d is not None and d.get("sz"):
                return int(d.get("sz")) / 100.0
        return {"titleStyle": 32.0, "bodyStyle": 18.0, "otherStyle": 18.0}[kind]
    return 18.0


def _autofit(shape):
    """'shrink' | 'grow' | None"""
    chain, _, _ = _chain(shape)
    for el in chain:
        bp = el.find(".//a:bodyPr", NSMAP)
        if bp is None:
            continue
        if bp.find("a:normAutofit", NSMAP) is not None:
            n = bp.find("a:normAutofit", NSMAP)
            return "shrink", float(n.get("fontScale", "100000")) / 100000.0
        if bp.find("a:spAutoFit", NSMAP) is not None:
            return "grow", 1.0
        if bp.find("a:noAutofit", NSMAP) is not None:
            return None, 1.0
    return None, 1.0


def _insets(shape):
    chain, _, _ = _chain(shape)
    vals = dict(DEFAULT_INSETS)
    for el in reversed(chain):
        bp = el.find(".//a:bodyPr", NSMAP)
        if bp is None:
            continue
        for k in vals:
            if bp.get(k) is not None:
                vals[k] = int(bp.get(k))
    return vals


def paragraph_info(shape):
    """[(texte, taille_pt, niveau, a_puce)] pour une forme avec texte."""
    out = []
    if not shape.has_text_frame:
        return out
    ph_type = _ph_type_attr(_shape_el(shape))[0]
    is_body = ph_type in ("body", "obj", "subTitle") or (ph_type is None and False)
    for p in shape.text_frame.paragraphs:
        text = "".join(r.text for r in p.runs) if p.runs else p.text
        lvl = p.level or 0
        size = None
        for r in p.runs:
            if r.font.size:
                size = max(size or 0, r.font.size.pt)
        if size is None:
            size = effective_font_pt(shape, lvl)
        pPr = p._p.find("a:pPr", NSMAP)
        no_bullet = pPr is not None and pPr.find("a:buNone", NSMAP) is not None
        out.append((text, size, lvl, bool(is_body and not no_bullet and ph_type in ("body", "obj"))))
    return out


def estimate_fill_ratio(shape, avg_char_em=0.52, line_em=1.2):
    """Rapport hauteur de texte estimee / hauteur disponible (1.0 = plein)."""
    mode, scale = _autofit(shape)
    if mode == "grow":
        return 0.0
    w = shape.width or 0
    h = shape.height or 0
    if not w or not h:
        return 0.0
    ins = _insets(shape)
    width_pt = (w - ins["lIns"] - ins["rIns"]) / EMU_PER_PT
    height_pt = (h - ins["tIns"] - ins["bIns"]) / EMU_PER_PT
    if width_pt <= 0 or height_pt <= 0:
        return 0.0
    total = 0.0
    first = True
    for text, size, lvl, bullet in paragraph_info(shape):
        if mode == "shrink" and scale < 1.0:
            size = size * scale
        indent = lvl * 27.0 + (18.0 if bullet else 0.0)
        usable = max(width_pt - indent, 20.0)
        cpl = max(int(usable / (size * avg_char_em)), 1)
        lines = 0
        for seg in (text.split("\v") if text else [""]):
            lines += max(1, int(math.ceil(len(seg) / float(cpl))))
        total += lines * size * line_em
        if not first:
            total += size * 0.2
        first = False
    return total / height_pt


def slot_capacity(width_emu, height_emu, size_pt, avg_char_em=0.52, line_em=1.2, insets=None):
    ins = insets or DEFAULT_INSETS
    width_pt = (width_emu - ins["lIns"] - ins["rIns"]) / EMU_PER_PT
    height_pt = (height_emu - ins["tIns"] - ins["bIns"]) / EMU_PER_PT
    cpl = max(int(width_pt / (size_pt * avg_char_em)), 1)
    lines = max(int(height_pt / (size_pt * line_em)), 1)
    return cpl, lines
