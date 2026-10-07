# Windows 11 sans droits administrateur — dépannage

Le skill n'écrit **que** dans : le dossier du projet (`output/`), `%LOCALAPPDATA%\pptx-charte\` (bibliothèques Python), et lit le dossier du skill. Rien dans `C:\Program Files`, aucune clé de registre, aucun service.

## 1. Trouver / installer Python (3.8 ou plus)

Vérifier : `py -3 --version` ou `python --version` (⚠ si Windows ouvre le Microsoft Store, c'est le faux « alias » : Python n'est pas installé).

Options, de la plus simple à la plus contraignante (aucune ne demande l'élévation) :

1. **Déjà présent** : Centre logiciel / Portail d'entreprise → chercher « Python ».
2. **winget en espace utilisateur** : `winget install -e --id Python.Python.3.12 --scope user`
3. **Microsoft Store** (si autorisé) : « Python 3.12 ».
4. **python.org** : installeur 64 bits → décocher « Install for all users » (installation dans `%LOCALAPPDATA%\Programs\Python`).
5. **Python embarqué (zip, aucun installeur)** : télécharger « Windows embeddable package (64-bit) » sur python.org, décompresser dans `%LOCALAPPDATA%\pptx-charte\python\` (ou dans `<SKILL>\python\`). `run.cmd` le détecte automatiquement. Ce Python n'a pas pip : utiliser l'installation hors-ligne (§2, option B).
6. Sinon : demander à l'IT d'installer Python ou d'autoriser l'exécution depuis `%LOCALAPPDATA%` (AppLocker/SRP).

## 2. Installer les dépendances (`run setup`)

Installées dans `%LOCALAPPDATA%\pptx-charte\libs-pyXY\` (pas de droits admin, pas d'environnement virtuel).

**A. Avec accès à PyPI** : `run setup`. Variantes d'entreprise :
- Proxy : `run setup --proxy http://proxy.exemple:8080` (ou variable `HTTPS_PROXY`).
- Miroir interne (Artifactory/Nexus) : `run setup --index-url https://miroir.exemple/api/pypi/pypi/simple`.
- Certificat d'inspection TLS : `pip config set global.cert C:\chemin\ca-entreprise.pem`, ou `run setup --trusted-host pypi.org --trusted-host files.pythonhosted.org`.

**B. Hors-ligne (PyPI bloqué, ou Python embarqué sans pip)** : sur un poste ayant Internet, préparer un dossier de wheels **pour la version de Python cible** (ex. 3.12) :

```
py -3.12 -m pip download -d wheelhouse --only-binary=:all: --platform win_amd64 --python-version 3.12 --implementation cp python-pptx lxml Pillow XlsxWriter typing_extensions
```
(≈ 12 Mo ; ou utiliser `dev/make_wheelhouse.cmd 3.12`). Copier `wheelhouse\` sur le poste cible puis : `run setup --wheelhouse C:\chemin\wheelhouse`. Sans pip, les .whl sont simplement extraits.

La version de Python **doit correspondre** au dossier de wheels (`cp312` = Python 3.12). `run doctor` affiche la version et le dossier utilisés.

## 3. Lancer les scripts

- Toujours via `scripts\run.cmd` : un `.cmd` n'est **pas** soumis à l'ExecutionPolicy PowerShell (pas besoin de `Set-ExecutionPolicy`).
- Chemins avec espaces : entourer de guillemets ; sous PowerShell, préfixer par `& `.
- Si le chemin du skill est long ou contient des caractères spéciaux, copier le skill dans un chemin court (`C:\Users\<moi>\skills\pptx-charte`).
- `run doctor` doit se terminer par « Dossier output : … (écriture OK) ».

## 4. Aperçu des diapos (`run render`)

- **PowerPoint installé** : export natif via COM (PowerShell en `-Command`, pas de script .ps1). Ferme proprement PowerPoint seulement s'il n'avait aucun autre document ouvert.
- **LibreOffice** (même portable) dans le PATH ou dans `%LOCALAPPDATA%\Programs\LibreOffice` : conversion PDF → PNG si `pdftoppm` ou PyMuPDF existent ; sinon seul `apercu.pdf` est produit.
- Aucun moteur (ou PowerShell en « Constrained Language Mode ») : `render` est indisponible → s'appuyer sur `run validate` et demander à l'utilisateur d'ouvrir le .pptx.

## 5. Problèmes fréquents

| Symptôme | Cause / solution |
|---|---|
| `'run.cmd' n'est pas reconnu` | Chemin mal quoté ; sous PowerShell utiliser `& "…\run.cmd"`. |
| `Python was not found; run without arguments to install from the Microsoft Store` | Faux alias Store : installer Python (§1) ou désactiver l'alias dans Paramètres > Applications > Paramètres avancés > Alias d'exécution d'application. |
| `pip` : `SSLError` / `CERTIFICATE_VERIFY_FAILED` | Inspection TLS d'entreprise : certificat de l'entreprise (§2A) ou mode hors-ligne (§2B). |
| `pip` : `Could not find a version that satisfies…` | Mauvaise version de Python vs wheels, ou index sans binaires → §2B avec la bonne version. |
| `ImportError: DLL load failed` (lxml/Pillow) | Wheels d'une autre version de Python que celle utilisée ; supprimer `%LOCALAPPDATA%\pptx-charte\libs-*` puis `run setup`. |
| `PermissionError` à l'écriture du .pptx | Fichier ouvert dans PowerPoint, ou dossier synchronisé/verrouillé (OneDrive) : fermer, renommer, ou écrire sous un dossier local. |
| Caractères accentués illisibles dans la console | Les scripts forcent l'UTF-8 ; si besoin `chcp 65001`. Les fichiers JSON doivent être en UTF-8. |
| Antivirus / AppLocker bloque `python.exe` | Demander l'autorisation à l'IT (liste blanche du chemin) ; pas de contournement. |
