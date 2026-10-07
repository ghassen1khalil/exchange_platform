# Skill OpenCode « pptx-charte »

Permet à OpenCode de **créer des présentations PowerPoint conformes à la charte**, en partant exclusivement du template officiel de l'organisation (layouts, couleurs, polices, logo, icônes).

**Principe** : l'IA ne dessine rien. Elle lit un *catalogue* généré à partir du template, rédige une *spec JSON* (« diapo 3 = layout X, titre …, tableau … »), et un script déterministe construit le .pptx en remplissant les zones du template. Deux façons de réutiliser le design, mélangeables diapo par diapo : un **layout** du masque (`"layout": "Titre et contenu"`) ou une **diapo modèle** du template (`"layout": "slide:5"` : la diapo d'exemple est clonée, son lorem ipsum remplacé en gardant polices, couleurs, puces, pictos). Un second script contrôle ensuite la conformité. Ainsi la charte est respectée par construction, pas « au mieux ».

Conçu pour **Windows 11 sans droits administrateur** : aucun installeur, pas d'ExecutionPolicy à changer, dépendances installées dans le profil utilisateur (ou hors-ligne).

## Contenu

```
.opencode/skills/pptx-charte/
  SKILL.md                    instructions lues par OpenCode (workflow, règles)
  config/charte.json          réglages : alias de layouts, pied de page, limites, palette tolérée
  assets/
    template.pptx / .potx     ← À DÉPOSER : le template officiel
    template-catalog.md       ← généré par « run catalog »
    icons/                    icônes PNG complémentaires (optionnel)
  references/                 format de la spec, règles éditoriales, dépannage Windows
  examples/exemple-deck.json  exemple de spec
  scripts/
    run.cmd / run.sh          lanceurs (trouvent Python tout seuls)
    pptx_tool.py              doctor | setup | catalog | icons | build | validate | render | outline
    charte_lib/               moteur (python-pptx)
dev/                          template de démo fictif, test de bout en bout, kit hors-ligne
opencode.json.example         permissions suggérées
```

## 1. Mise en service (personne qui maintient la charte) — une fois

1. **Copier** `.opencode/skills/pptx-charte/` dans le dépôt/projet partagé (ou voir §2 pour une installation globale).
2. **Déposer le template** : `assets/template.pptx` (ou `.potx`) — un simple .pptx de diapos d'exemple en lorem ipsum convient.
   - Pas de macros. Si le design est dans les **diapos d'exemple**, elles deviennent des modèles `slide:N` ; s'il y a de vrais **layouts** à placeholders, ils sont utilisables directement. Le catalogue indique ce qui est exploitable.
   - Prévoir une **diapo « Icônes »** (titre ou texte contenant « icône »/« icon ») regroupant les icônes, chacune nommée (nom de forme, texte alternatif ou légende). Sinon : `icon_library_slides` dans `config/charte.json`, ou PNG dans `assets/icons/`.
3. **Générer le catalogue** : `scripts\run.cmd catalog --render` → relire `assets/template-catalog.md` (section *Diagnostic*, puis layouts et diapos modèles) et regarder `assets/preview/planche-contact.png` (aperçu des diapos, si PowerPoint ou LibreOffice est disponible). Chaque layout est-il reconnu avec le bon rôle (`@cover`, `@section`, `@content`…) ? Chaque diapo modèle a-t-elle les bonnes zones ?
   - Rôle faux ou ambigu → `layout_aliases` dans `config/charte.json` (ex. `{"cover": "Page de garde", "section": "Intercalaire"}`).
   - Layouts à bannir (anciens, tests) → `forbidden_layouts` ; ou `allowed_layouts` pour une liste blanche.
4. **Régler** `config/charte.json` : langue, texte de pied de page / classification (`footer.text`), `min_font_pt`, limites de densité, couleurs tolérées hors thème (`extra_palette`).
5. **Adapter** `references/charte-rules.md` (règles rédactionnelles de l'organisation).
6. **Tester** : `scripts\run.cmd build examples\exemple-deck.json` (les noms de layouts/icônes de l'exemple sont ceux de la démo : remplacer par ceux du catalogue), puis `validate`, puis ouvrir le .pptx dans PowerPoint. **Contrôler visuellement** chaque layout au moins une fois avec du vrai contenu (surtout les débordements de texte).
7. **Versionner** template + catalogue + config ensemble.

Essai rapide sans le vrai template : copier `dev/demo-template.potx` (design dans des layouts) ou `dev/demo-pattern-template.pptx` (design dans des diapos, comme un .pptx de lorem ipsum) en `assets/template.*` (fictifs — à retirer ensuite), ou lancer `python dev/smoke_test.py`.

### Premier passage sur le poste client (template non partageable)
1. Installer le skill, déposer `assets/template.pptx`, lancer `run doctor`, `run setup` si besoin.
2. `run catalog --render`.
3. Récupérer **`assets/template-catalog.md`** (texte seul, ne contient que des noms de formes, tailles et le lorem ipsum du template ; à relire avant envoi) et, si possible, `assets/preview/planche-contact.png` : cela suffit pour affiner les alias, les règles et les tests sans le fichier lui-même.

## 2. Installation sur le poste d'un utilisateur (Windows 11, sans admin)

**Au choix** (OpenCode cherche aussi dans `.claude/skills` et `.agents/skills`) :

| Portée | Emplacement |
|---|---|
| Projet (recommandé : partagé par git) | `<projet>\.opencode\skills\pptx-charte\` |
| Global (tous les projets de l'utilisateur) | `%USERPROFILE%\.config\opencode\skills\pptx-charte\` |

Puis, dans un terminal, **une seule fois** :

```
<dossier-du-skill>\scripts\run.cmd doctor
<dossier-du-skill>\scripts\run.cmd setup      (si « Dépendances MANQUANTES »)
```

- `doctor` vérifie Python, les bibliothèques, le template, l'écriture dans `output\`, et les moteurs d'aperçu.
- `setup` installe `python-pptx`, `lxml`, `Pillow` dans `%LOCALAPPDATA%\pptx-charte\` — **sans droit admin**. Pas d'Internet ? Mode hors-ligne avec `--wheelhouse` (kit : `dev\make_wheelhouse.cmd 3.12`). Pas de Python ? Voir `references/windows-sans-admin.md` (winget `--scope user`, Python embarqué, Centre logiciel).
- Optionnel : copier `opencode.json.example` vers `opencode.json` du projet pour autoriser le skill et le lanceur sans confirmation.

Python 3.9+ recommandé (code compatible 3.8 ; testé avec 3.13). `pip` choisit automatiquement les versions de bibliothèques compatibles avec votre Python.

## 3. Utilisation

Dans OpenCode, par exemple :

> Crée une présentation de 8 diapos pour le comité de pilotage sur le bilan du projet Phoenix, à partir de `notes.md`.
> Ajoute 3 diapos sur les risques à `output/deck.pptx`.
> Mets cette présentation à la charte : `ancienne-presentation.pptx`.

Déroulé attendu : l'assistant charge le skill → lit le catalogue → pose ses questions → **propose un plan à valider** → génère → `validate` (0 erreur) → aperçu si possible → livre le fichier dans `output\` avec ses hypothèses et les éléments « [À compléter] ».

## 4. Ce qui est contrôlé automatiquement (`validate`)

Layout et thème = ceux du template · aucun texte résiduel (« Cliquez pour… », lorem) · couleurs/polices « en dur » · taille mini · débordement probable · densité (puces, mots) · éléments hors cadre ou qui se chevauchent · images sans texte alternatif ou déformées · titre manquant/dupliqué.

## 5. Limites connues (à connaître avant déploiement)

- **Le débordement de texte est une estimation** (pas de moteur de mise en page sans PowerPoint/LibreOffice). Les cas manifestes sont détectés ; relire l'aperçu ou le fichier ouvert dans PowerPoint pour les cas limites.
- `run render` sur Windows utilise PowerPoint via COM (ou LibreOffice). **Ce chemin et `run.cmd` ont été écrits pour Windows mais testés ici sous Linux uniquement** : faire un premier essai sur un poste réel (`doctor`, `build`, `render`) avant déploiement large. L'ensemble du moteur (catalogue, build, validate, rendu LibreOffice) a été testé de bout en bout avec un template de démonstration.
- Icônes du template : images et groupes de formes sont copiés tels quels (pas de recoloration). Pas de SVG dans `assets/icons/` (PNG).
- **Mode diapo modèle** : les éléments décoratifs liés à une zone non remplie (numéro, picto) restent — choisir le modèle qui a le bon nombre de zones ; tableaux et graphiques sont reconstruits à la même place (style de tableau repris de l'exemple) ; transitions et animations des diapos modèles ne sont pas reprises ; les formes groupées sont conservées mais leur texte interne n'est pas remplaçable.
- Pas de création de nouveaux layouts, d'animations ou de transitions, de SmartArt/diagrammes éditables ; schémas = images/icônes/`extras` ou graphiques/tableaux natifs.
- Remise à la charte d'un .pptx existant = **reconstruction** à partir de son contenu (pas de modification en place).
- Les notes de l'orateur utilisent le masque de notes par défaut de python-pptx (pas celui du template).

## 6. Maintenance

| Besoin | Action |
|---|---|
| Nouvelle version du template | remplacer `assets/template.potx` → `run catalog` → relire le catalogue → `validate` sur un deck d'essai |
| Ajouter des icônes | dans la diapo « Icônes » du template, ou PNG dans `assets/icons/` → `run catalog` |
| Un layout est mal reconnu | `layout_aliases` dans `config/charte.json` |
| Texte systématiquement trop long | ajuster `limits` ou `references/charte-rules.md` |
| Diagnostic chez un utilisateur | `run doctor` (copier la sortie) |
