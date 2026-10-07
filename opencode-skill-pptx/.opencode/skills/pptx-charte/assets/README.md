# assets/

Déposer ici le **template officiel** de l'organisation, nommé :

    template.potx        (ou template.pptx)

Puis générer le catalogue lu par l'assistant :

    scripts\run.cmd catalog --render

Ce qui est produit : `template-catalog.md` (diagnostic, layouts, **diapos modèles**, capacités de texte, palette, polices, icônes) et, avec `--render` (PowerPoint ou LibreOffice requis), `preview/` : images des diapos du template, que l'assistant peut regarder.
**Régénérer le catalogue à chaque changement de template** et le versionner avec lui.

Notes :
- `.potx` et `.pptx` sont acceptés ; pas de macros (`.potm`/`.pptm`).
- Le template peut être un simple **.pptx** contenant des diapos d'exemple (lorem ipsum) : elles servent de **modèles** (`slide:N`) et sont retirées du fichier généré.
- Pour tester sans le vrai template : copier `dev/demo-template.potx` (fictif) sous le nom `template.potx`. **Ne pas le laisser en production.**
