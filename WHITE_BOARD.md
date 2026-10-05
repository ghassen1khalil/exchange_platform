### MAIL :
Bonjour,

Dans le cadre d'un workflow n8n que nous développons (traitement vidéo via un nœud Code Python), nous avons besoin d'un composant d'infrastructure supplémentaire : un Task Runner Python externe, avec ffmpeg installé, connecté à votre instance n8n actuelle.

Contexte technique

L'image n8n déployée (docker.n8n.io/n8nio/n8n:2.27.5) est une image hardened qui n'embarque ni Python ni gestionnaire de paquets. Pour exécuter du code Python dans un nœud Code (et lui donner accès à ffmpeg), n8n nécessite un second conteneur dédié — le Task Runner en mode externe — qui se connecte à l'instance n8n principale via son "task broker" (port 5679).

Ce qu'il faut déployer

1. Un nouveau conteneur, construit à partir de l'image officielle n8nio/runners:2.27.5 (⚠️ la version doit correspondre exactement à celle de n8n), avec ffmpeg installé par-dessus.
2. Deux variables d'environnement à ajouter sur le conteneur n8n existant (nécessite un redémarrage du conteneur) :
   - N8N_RUNNERS_MODE=external
   - N8N_RUNNERS_BROKER_LISTEN_ADDRESS=0.0.0.0
   - N8N_RUNNERS_AUTH_TOKEN=<secret partagé à générer>
   - N8N_NATIVE_PYTHON_RUNNER=true
3. Connectivité réseau : le nouveau conteneur runner doit pouvoir joindre le conteneur n8n sur le port 5679 (task broker). Si les deux sont sur le même hôte Docker, un réseau Docker commun (bridge) suffit ; sinon merci de nous indiquer votre topologie réseau pour adapter.
4. Variables d'environnement sur le conteneur runner :
   - N8N_RUNNERS_TASK_BROKER_URI=http://<host-n8n>:5679
   - N8N_RUNNERS_AUTH_TOKEN=<même secret qu'à l'étape 2>
5. Un fichier de configuration (n8n-task-runners.json) à monter sur le conteneur runner, qui définit la liste blanche des modules Python autorisés dans le sandbox (subprocess, base64 uniquement — rien de plus large n'est nécessaire).

Fichiers fournis en pièce jointe

- Dockerfile (image runner + ffmpeg)
- docker-compose.yml (référence complète, les deux services)
- n8n-task-runners.json (config du sandbox)

Questions pour vous

- Confirmez-vous que l'instance n8n tourne bien en version 2.27.5 ? (L'image runner doit être strictement alignée.)
- Quel est le mode de déploiement actuel (Docker standalone, Docker Compose, Kubernetes) ? Les fichiers fournis sont en Docker Compose, à adapter si besoin.
- Un redémarrage du conteneur n8n (ajout des variables d'environnement) est-il possible à planifier, ou faut-il passer par un process de changement particulier ?

Restons disponibles pour un point technique si besoin.

Cordialement,
Ghassen
---
