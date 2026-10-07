# Format de la spec JSON (référence)

Fichier UTF-8 (BOM toléré). Les clés commençant par `_` sont ignorées (commentaires). Toute autre clé inconnue est une **erreur** (pour détecter les fautes de frappe).
Les chemins de fichiers sont relatifs au dossier de la spec (puis au dossier courant) ; utiliser `/` plutôt que `\`.

## Niveau racine

| Clé | Type | Rôle |
|---|---|---|
| `slides` | liste (obligatoire) | les diapositives, dans l'ordre |
| `output` | texte | chemin du .pptx (défaut : `output/<nom de la spec>.pptx`) ; `-o` en ligne de commande l'emporte |
| `metadata` | objet | `title`, `author`, `subject`, `keywords`, `comments` (propriétés du document) |
| `footer` | objet | `text` (pied de page), `slide_number` (bool), `date` (bool) — valeurs par défaut dans `config/charte.json` |
| `language` | texte | langue de relecture (défaut `fr-FR`) |

## Une diapositive

| Clé | Rôle |
|---|---|
| `layout` | **obligatoire**. Nom exact du catalogue (insensible à la casse/accents), `#12` (numéro du catalogue) ou rôle `@cover` `@section` `@agenda` `@content` `@two_content` `@three_content` `@comparison` `@picture` `@title_only` `@blank` `@end` (alias réglables dans `config/charte.json`) |
| `title` | texte du titre (`\n` = saut de ligne) |
| `subtitle` | sous-titre (seulement si le layout a une zone sous-titre) |
| `content` | liste de blocs, remplissant dans l'ordre les zones `content[0]`, `content[1]`… du catalogue |
| `placeholders` | `{ "<idx ou nom>": bloc }` : remplissage explicite d'une zone (retirée de la séquence `content`) |
| `notes` | notes de l'orateur |
| `footer` | `false` = pas de pied/numéro sur cette diapo ; texte = pied spécifique |
| `hidden` | `true` = diapo masquée |
| `extras` | éléments à position libre (cm) — dernier recours |

Une zone du layout non remplie est **supprimée** (avertissement). Un titre absent supprime la zone titre (déconseillé : navigation et accessibilité).

## Mode « diapo modèle » : `"layout": "slide:N"`

Quand le design du template est porté par des **diapos d'exemple** (et non par de vrais layouts), la diapo N est **clonée** — fond, formes, pictos, couleurs, polices — puis seules ses zones « à trous » sont remplacées, avec la mise en forme du texte d'exemple (police, taille, couleur, gras, puces).

- Zones : `title`, puis `content[0]`, `content[1]`… numérotées dans le catalogue (ordre de lecture). `placeholders` accepte aussi le **nom de la forme** (`{"Colonne 2": "…"}`).
- Texte : `{"text": "Intertitre\nTexte"}` : la 1re ligne reprend la mise en forme du 1er paragraphe d'exemple, la 2e celle du 2e, etc. `{"bullets": […]}` : mise en forme par niveau de puce.
- Respecter la longueur d'exemple (« ~N car. attendus » au catalogue ; au-delà de ~1,3×, risque de débordement).
- `image` remplace l'image d'exemple (recadrée pour remplir, `fit: "contain"` pour ne pas rogner) ; `table` / `chart` reconstruisent le tableau / graphique à la même place ; `icon` remplace une image.
- Une zone non remplie est supprimée (son décor — numéro, picto — reste : choisir un modèle avec le bon nombre de zones).
- Pas de `subtitle` (les sous-titres sont des zones `content[i]`). Le pied de page / numéro viennent de la diapo modèle.

## Blocs de contenu

Un bloc peut s'écrire en raccourci : une chaîne = `{"text": …}`, une liste = `{"bullets": …}`.
Un bloc contient **exactement un** type parmi :

### `text`
```json
{ "text": "Premier paragraphe\nDeuxième paragraphe", "bold": false, "no_bullet": true }
```
`no_bullet: true` supprime la puce héritée du template (utile pour une phrase d'intro dans une zone à puces).

### `bullets`
```json
{ "bullets": [
    "Niveau 1 avec **gras**",
    ["Niveau 2", "Niveau 2 bis", ["Niveau 3"]],
    { "text": "Item objet", "bold": true, "children": ["enfant"] }
] }
```
Une sous-liste (`[...]`) = niveau suivant. Max conseillé : 7 puces de niveau 1, 2 niveaux.

### `image`
```json
{ "image": "images/photo.png", "alt": "Description pour l'accessibilité", "fit": "contain" }
```
Zone de type image du layout : l'image est recadrée pour remplir (masque du template conservé). Autre zone : insérée sans déformation, centrée. `fit: "contain"` force ce second comportement. **`alt` obligatoire en pratique.**
Formats : PNG, JPG, GIF, BMP, TIFF (pas de SVG).

### `icon`
```json
{ "icon": "icone-cloud", "size": 3 }
```
Icône du template ou de `assets/icons/` (voir `run icons`), centrée dans la zone ; `size` en cm (défaut : remplit la zone).

### `table`
```json
{ "table": {
    "header": ["Trimestre", "Budget (k€)", "Réalisé (k€)"],
    "rows": [["T1", "120", "118"], ["T2", "150", "141"]],
    "col_widths": [2, 3, 3],
    "align": ["left", "right", "right"],
    "font_pt": 14,
    "alt": "Budget et réalisé par trimestre"
} }
```
Style = style de tableau du template (couleurs du thème). Colonnes numériques alignées à droite automatiquement. Max conseillé : 6 colonnes × 8 lignes ; au-delà, scinder ou passer en annexe.

### `chart`
```json
{ "chart": {
    "type": "column",
    "categories": ["T1", "T2", "T3"],
    "series": [ { "name": "Budget", "values": [120, 150, 130] },
                { "name": "Réalisé", "values": [118, 141, 128] } ],
    "title": "Budget vs réalisé (k€)",
    "legend": true, "data_labels": true, "number_format": "0"
  },
  "alt": "Budget et réalisé par trimestre" }
```
`type` : `column`, `column_stacked`, `column_percent`, `bar`, `bar_stacked`, `line`, `pie`, `doughnut`, `area`, `area_stacked`, `scatter` (séries `{"name", "points": [[x,y],…]}`).
Les couleurs suivent automatiquement les accents du thème. Pas de couleur personnalisée. 1 message par graphique ; ≤ 5 séries ; camembert ≤ 6 parts.

## `extras` (position libre, en cm depuis le coin haut-gauche)

```json
{ "icon":  "icone-lock", "x": 2, "y": 8, "size": 2.5 }
{ "image": "images/logo-client.png", "x": 25, "y": 1, "w": 6, "h": 3, "alt": "Logo client" }
{ "text":  "Légende", "x": 5, "y": 9, "w": 12, "h": 1.5, "size_pt": 18, "bold": true, "color": "accent1", "align": "left" }
```
`color` : uniquement un nom de couleur du thème (`dk1 lt1 dk2 lt2 accent1…accent6`). Rester dans les marges du layout ; `validate` signale chevauchements et dépassements.

## Zone libre (layouts sans zone de contenu)
Pour un layout sans `content[n]` (ex. « Titre seul »), les blocs `image`, `icon`, `table`, `chart` de `content` sont placés automatiquement sous le titre, jusqu'au pied de page ; plusieurs blocs = colonnes de même largeur. Le texte seul n'y est pas autorisé : utiliser un layout avec zone de texte.

## Ajouter à une présentation existante
`run build spec.json --base existant.pptx -o output/existant-v2.pptx` : les diapos de `existant.pptx` sont conservées intactes, celles de la spec sont ajoutées à la fin avec les layouts du template officiel. La sortie doit être un autre fichier que `--base`.

## Codes de sortie
`0` ok · `1` erreur de spec / de contrôle · `3` dépendances manquantes (`run setup`) · `9` Python introuvable.
