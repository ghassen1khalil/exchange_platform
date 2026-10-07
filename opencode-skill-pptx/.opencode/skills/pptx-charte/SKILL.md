---
name: pptx-charte
description: Crée, complète ou met en forme des présentations PowerPoint (.pptx) conformes à la charte graphique de l'organisation, à partir du template officiel (layouts, couleurs, polices, icônes). À utiliser dès qu'on demande de créer, générer, ajouter ou restructurer des diapositives, un deck, des slides ou une présentation PowerPoint (create slides, pptx, deck, brand template).
compatibility: opencode
metadata:
  langue: fr
  plateforme: windows-sans-admin
---

# Skill pptx-charte — présentations conformes à la charte

Ce skill pilote la création de diapositives **uniquement à partir du template PowerPoint officiel**.
Le template porte la charte (couleurs, polices, logo, icônes, layouts) : tu ne l'imites jamais, tu l'utilises.

## Règles non négociables

1. **Toujours passer par le script** (`build`). Ne génère jamais un .pptx avec ton propre code python-pptx / pptxgenjs / PowerShell, et ne repars jamais d'un fichier vierge.
2. **Mises en page du catalogue uniquement** (`assets/template-catalog.md`) : soit un **layout** (`"layout": "Nom"`), soit une **diapo modèle** du template (`"layout": "slide:N"`, la diapo d'exemple est clonée puis son texte remplacé). Choisis selon le contenu ; n'invente pas de mise en page.
3. **Aucune couleur, police ou taille « en dur »**. Couleurs : seulement les noms du thème (`accent1`…). Polices : héritées du thème. Pas de dégradé, d'ombre, de WordArt.
4. **Un message par diapo**, titre = phrase qui énonce ce qu'il faut retenir. Respecte `references/charte-rules.md`.
5. **Jamais de chiffres, citations ou sources inventés.** Une donnée manquante s'écrit `[À compléter : …]` et se signale à l'utilisateur.
6. **Confidentialité** : n'envoie aucun contenu vers un service externe, ne télécharge pas d'images. Visuels = fichiers fournis par l'utilisateur ou icônes du template.
7. Les fichiers de l'utilisateur ne sont jamais écrasés : tout est écrit dans `output/` (dossier du projet courant). Les itérations régénèrent le même fichier de sortie ; l'outil refuse d'écrire sur le fichier `--base`.

## Lancer les commandes

Le dossier de ce skill est indiqué par OpenCode (« Base directory for this skill »). Dans la suite, `<SKILL>` = ce dossier. Chemin sans espace de préférence ; **entre guillemets** sinon.

| Shell | Syntaxe |
|---|---|
| cmd / Git Bash | `"<SKILL>\scripts\run.cmd" <commande> …` |
| PowerShell | `& "<SKILL>\scripts\run.cmd" <commande> …` |
| macOS / Linux | `sh "<SKILL>/scripts/run.sh" <commande> …` |

Si une syntaxe est rejetée (erreur de parsing), essaie la suivante. Dans la suite : `run` = cette commande complète. Utilise des `/` dans les chemins des fichiers JSON.

Commandes : `doctor` · `setup` · `catalog` · `icons` · `build` · `validate` · `render` · `outline`.

## Workflow à suivre

### 0. Vérifier l'environnement (première utilisation de la session)
`run doctor`
- « Dépendances MANQUANTES » → `run setup` (installation en espace utilisateur, **sans droits admin** ; en cas d'échec réseau lis `references/windows-sans-admin.md`).
- « Python introuvable » → explique à l'utilisateur les options de `references/windows-sans-admin.md` (winget `--scope user`, Python embarqué, Centre logiciel).
- « Template ABSENT » → demande à l'utilisateur de déposer `template.potx` dans `<SKILL>/assets/`, puis `run catalog`.
- Si `assets/template-catalog.md` n'existe pas : `run catalog --render` (le `--render` ajoute un aperçu image des diapos du template si PowerPoint ou LibreOffice est disponible).

### 1. Lire le catalogue
Lis `assets/template-catalog.md` : section **Diagnostic** (quel mode privilégier), **layouts** (zones `title` / `subtitle` / `content[n]`, capacité en caractères), **diapos modèles** `slide:N` (zones remplaçables, longueur de texte attendue), palette, icônes. Si `assets/preview/planche-contact.png` existe, **regarde-la** pour voir le design réel des diapos du template. Lis ensuite `references/charte-rules.md`. `run icons` liste les icônes.

**Quel mode ?** Diapo modèle (`slide:N`) quand le design vit dans les diapos (colonnes décorées, pictos, cartes, fonds particuliers) : c'est le plus fidèle. Layout quand le template définit de vrais placeholders (titre, texte, image). Dans les deux cas, le choix se fait diapo par diapo et on peut les mélanger.

### 2. Cadrer le besoin
Il te faut : **objectif et public**, **durée ou nombre de diapos**, **langue**, **sources/données** (fichiers, texte collé), **éléments imposés** (plan, chiffres, visuels). Pose **en une seule fois** les questions manquantes (outil `question` si disponible, sinon dans le chat) ; si l'utilisateur dit d'avancer, prends des hypothèses raisonnables et liste-les.
Pour un document source (Word, notes, mail) : lis-le et synthétise ; ne recopie pas des paragraphes entiers.

### 3. Proposer le plan (storyline) AVANT de générer
Présente un tableau court : n° · titre-message · layout choisi · contenu prévu (puces / tableau / graphique / icône).
Attends la validation, sauf demande explicite d'enchaîner. Règles de plan : 1 page de garde (`@cover`), un fil logique, séparateurs de section si > 8 diapos, une conclusion/prochaines étapes.

### 4. Écrire la spec JSON
Fichier `output/<nom>.spec.json` (UTF-8). Référence complète : `references/spec-format.md`. Exemple : `examples/exemple-deck.json`.

```json
{
  "output": "output/mon-deck.pptx",
  "metadata": { "title": "Titre du document", "author": "Équipe X" },
  "slides": [
    { "layout": "@cover", "title": "Titre", "subtitle": "Sous-titre – date" },
    { "layout": "@content", "title": "Message clé de la diapo",
      "content": [ { "bullets": ["**Point 1** : détail", ["sous-point"], "Point 2"] } ],
      "notes": "Ce que dit l'orateur." },
    { "layout": "@two_content", "title": "Chiffres clés",
      "content": [ { "bullets": ["…"] },
                   { "table": { "header": ["Année","CA (M€)"], "rows": [["2025","12,4"]] } } ] },
    { "layout": "@title_only", "title": "Évolution du CA",
      "content": [ { "chart": { "type": "column", "categories": ["T1","T2"],
                               "series": [{ "name": "CA", "values": [10, 12] }] },
                     "alt": "CA par trimestre" } ] }
  ]
}
```

Aide-mémoire :
- `layout` : nom exact du catalogue, rôle `@cover @section @content @two_content @three_content @title_only @picture @end`, ou **`slide:N`** (diapo modèle N du catalogue).
- Avec `slide:N` : `title` + `content` (les zones sont numérotées `content[i]` dans le catalogue ; `{"text": "Intertitre\nTexte"}` reprend la mise en forme paragraphe par paragraphe de l'exemple) ; zones non remplies = supprimées ; pas de `subtitle`.
- `title`, `subtitle` ; `content` = liste de blocs remplissant les zones `content[0]`, `content[1]`… dans l'ordre du catalogue ; `placeholders` = remplissage explicite par `idx` ou nom.
- Blocs : `{"text"}` · `{"bullets"}` (`**gras**` en ligne, sous-liste = niveau suivant) · `{"image","alt"}` · `{"icon":"nom"}` · `{"table"}` · `{"chart"}`.
- Layout sans zone de contenu (ex. « Titre seul ») : les blocs image / icône / table / chart occupent automatiquement la zone sous le titre.
- `extras` (placement libre en cm : icône, image, texte) : **dernier recours**, jamais pour contourner un layout inadapté.
- `notes` : toujours renseigner les notes orateur pour une présentation orale.
- Respecte la capacité indiquée au catalogue (caractères/ligne × lignes). Trop de texte → scinder la diapo, pas réduire la police.

### 5. Générer
`run build output/<nom>.spec.json`
- Les erreurs sont listées **toutes ensemble** (layout inconnu, zone absente, icône inconnue…) et **rien n'est écrit** : corrige la spec et relance.
- Les avertissements (débordement probable, zone vide supprimée, puces nombreuses) se corrigent aussi.
- Ajouter des diapos à un deck existant : `run build spec.json --base mon-deck.pptx -o output/mon-deck-v2.pptx` (les diapos existantes sont conservées telles quelles).

### 6. Contrôler
`run validate output/<nom>.pptx` — doit afficher **0 erreur**. Traite aussi les avertissements (texte trop dense, couleur/police en dur, image sans texte alternatif, débordement).
Puis, si un moteur de rendu existe (`doctor` : PowerPoint ou LibreOffice) : `run render output/<nom>.pptx` et **regarde** `planche-contact.png` (puis les `slide-N.png` suspects) : texte coupé, chevauchements, alignements, lisibilité. Corrige la spec et régénère. Sans moteur de rendu, dis-le et demande à l'utilisateur d'ouvrir le fichier.

### 7. Livrer
Réponds court : chemin du .pptx, nombre de diapos, plan en une ligne par diapo, **hypothèses prises**, **éléments à compléter** (`[À compléter]`), points d'attention (données non vérifiées). Propose une seule suite utile (ex. version courte, ajout d'annexes).

## Si l'utilisateur fournit un .pptx existant
- **Ajouter des diapos** → `--base`.
- **Voir son plan** → `run outline fichier.pptx`.
- **Le remettre à la charte** : ne modifie pas le fichier ; lis son contenu (`outline`), reconstruis une spec avec les layouts du template et génère un nouveau fichier.

## Erreurs fréquentes
| Symptôme | Action |
|---|---|
| `PermissionError` / « ouvert dans PowerPoint » | Fermer le fichier ou changer le nom de sortie (`-v2`). |
| `layout … absent du template` | Utiliser un nom du catalogue ; la suggestion « Vouliez-vous dire » aide. Diapo modèle : `slide:N`. |
| `N bloc(s) content mais le layout n'offre que M zone(s)` | Choisir un layout avec assez de zones ou réduire les blocs. |
| `image introuvable` | Chemin relatif à la spec ; `/` dans les chemins ; pas d'accents exotiques. |
| JSON invalide | Pas de virgule finale, guillemets `"` droits, `\\` ou `/` dans les chemins. |
| Code de sortie 3 | Dépendances manquantes → `run setup`. |
| Code de sortie 9 | Python introuvable → `references/windows-sans-admin.md`. |

## Fichiers du skill
- `assets/template.potx` — template officiel (généré/mis à jour par la personne qui maintient la charte)
- `assets/template-catalog.md` — catalogue généré par `run catalog` (layouts, palette, icônes)
- `assets/icons/` — icônes PNG complémentaires
- `config/charte.json` — réglages (alias de layouts, limites, pied de page, palette tolérée…)
- `references/spec-format.md`, `references/charte-rules.md`, `references/windows-sans-admin.md`
- `examples/exemple-deck.json`
