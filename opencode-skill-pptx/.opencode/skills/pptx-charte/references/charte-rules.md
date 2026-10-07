# Règles éditoriales de la charte

> **À adapter par l'équipe qui maintient la charte.** Les règles graphiques (couleurs, polices, positions, logo) sont portées par le template et ne se discutent pas ici ; ce fichier couvre le **fond** : comment écrire et structurer. Les valeurs par défaut ci-dessous sont des bonnes pratiques génériques. Les seuils chiffrés (puces, mots, taille minimale) sont contrôlés par `run validate` et réglables dans `config/charte.json` (`limits`, `min_font_pt`).

## Structure
- Une page de garde (`@cover`), puis un fil logique : contexte → constat → options/analyse → recommandation → prochaines étapes.
- Séparateur de section (`@section`) dès que la présentation dépasse 8 diapos.
- Ordre de grandeur : 1 diapo ≈ 1 à 2 minutes à l'oral. Annexes à la fin, après la conclusion.
- Terminer par « Prochaines étapes / Décisions attendues » plutôt que par « Merci » seul.

## Titres
- Phrase-message (verbe + résultat), pas un libellé : « Les coûts baissent de 12 % en 2026 » plutôt que « Coûts ».
- ≤ 90 caractères, idéalement sur une ligne ou deux. Pas de point final.
- Titres uniques (pas deux diapos identiques).

## Corps de texte
- ≤ 7 puces de niveau 1 par zone, ≤ 2 niveaux ; ≤ 110 mots par diapo.
- Puces = fragments de phrase (≈ 12 mots max), parallèles dans leur construction, sans point final.
- Mettre en **gras** 1 à 3 mots-clés par puce au plus. Jamais de MAJUSCULES pour insister, pas de soulignement.
- Pas de paragraphe copié-collé ; l'argumentaire détaillé va dans les notes de l'orateur.
- Pas de taille de police réduite pour « faire rentrer » : scinder la diapo.

## Chiffres, tableaux, graphiques
- Un message par graphique ; le titre de la diapo énonce la conclusion du graphique.
- Toujours une unité (k€, %, jours) et une période ; source en bas de diapo (zone de texte du layout ou `extras`) : « Source : … ».
- Graphique : ≤ 5 séries, camembert ≤ 6 parts, ordre logique (temps, ou valeur décroissante).
- Tableau : ≤ 6 colonnes × 8 lignes, chiffres alignés à droite, mêmes décimales par colonne.
- Format français : virgule décimale, espace pour les milliers (`12 400`), `%` collé au nombre, dates `07/10/2026` ou `7 octobre 2026`.
- Ne jamais inventer une donnée : `[À compléter : chiffre T3]` et le signaler.

## Visuels et icônes
- Icônes : uniquement celles du template / de `assets/icons/` ; une icône = un sens, pas de décoration gratuite.
- Images : fournies par l'utilisateur, avec texte alternatif (`alt`) décrivant l'information portée.
- Pas de capture d'écran illisible : recadrer, ≥ 12 pt apparent.

## Accessibilité
- Chaque diapo a un titre. Chaque image/graphique/tableau a un texte alternatif.
- Contraste et couleurs : celles du thème uniquement (déjà conçues pour cela). Ne pas coder une information par la seule couleur.

## Langue et ton
- Langue de la présentation = celle du demandeur (par défaut français), vouvoiement/impersonnel, ton factuel.
- Sigles définis à la première occurrence. Pas d'anglicismes inutiles, sauf termes métier établis.
- Casse « phrase » (majuscule initiale uniquement) pour titres et puces.

## Confidentialité
- Reprendre la mention de classification de l'organisation si elle existe (régler `footer.text` dans `config/charte.json`).
- Ne pas inclure de données personnelles ou confidentielles qui ne sont pas nécessaires au message.
