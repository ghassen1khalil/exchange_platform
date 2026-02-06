
1 - Upload de fichier vers CMX (FSADA) : 
java '-Dspring.datasource.url=jdbc:sqlite:C:\Chemin\DB\Sqlite.db' -Dspring.profiles.active=fsada -jar 'C:\Chemin\CMX\CLI.jar' --fsada

2 - L'Extractor :

java 'C:\Chemin\CMX\EXTRACTOR.jar' -resourcesPath='C:\Chemin\Repertoire\RESSOURCES' -task=NOM_TACHE

- CsvFilesExtractor
- DeleteDocumentV3
- DocumentExistenceChecker
- CoreDataDump


java -jar cmx-extractor.jar -resources=resources -task=CoreDataDump


Hello messieurs,

Comme convenu, je reviens vers vous avec les éléments demandés lors de notre dernière rencontre :

- Pour le Swagger, pas d'URL publique aujourd'hui mais je vous joins le fichier `openapi.yaml` qui représente la dernière version de l'API CMX Core.
- La documentation technique de CMX est décrite dans le document "Service Definition" que je vous joints en PDF ici.
- Les 2 outils sous forme de CLI sont également joints : cmx-extractor.jar et cmx-content.transfer.jar

# CMX Tools – Modes opératoires

Ce document décrit les principaux modes opératoires permettant :
- le transfert de fichiers depuis un file-system vers CMX,
- ainsi que les opérations de vérification, d’inventaire, de suppression et d’extraction des documents stockés dans CMX.

---

## 1. Transfert de fichiers depuis un file-system vers CMX

Le transfert de fichiers depuis un file-system vers CMX s’effectue à l’aide de l’outil :

- **cmx-content-transfer-cli.jar**

Documentation associée :  
https://confluence.axa.com/confluence/spaces/BB3A/pages/127751013/Content+Transfer+CLI

---

## 2. Opérations sur les fichiers dans CMX

L’outil **cmx-extractor.jar** permet d’exécuter différentes opérations sur les documents stockés dans CMX.

---

### 2.1 Vérification de l’existence de documents  
**Tâche : DocumentExistenceChecker**

#### Utilisation
```bash
java 'C:\Chemin\CMX\EXTRACTOR.jar' -resourcesPath='C:\Chemin\Repertoire\RESSOURCES' -task=DocumentExistenceChecker
```

#### Paramétrage (documentExistenceChecker.json)
```json
{
  "inputFile": "liste_des_ids_de_fichiers_à_vérifier.csv",
  "outputDir": "outputs",
  "nbThreads": 4,
  "searchPageSize": 100
}
```

---

### 2.2 Inventaire d’un DocStore  
**Tâche : CsvFilesExtractor**

#### Utilisation
```bash
java 'C:\Chemin\CMX\EXTRACTOR.jar' -resourcesPath='C:\Chemin\Repertoire\RESSOURCES' -task=CsvFilesExtractor
```

#### Paramétrage (referential.json)
```json
{
  "filesToCreate": [
    {
      "fileName": "PV_RECETTE_GIE_AGPC8.csv",
      "columns": [
        "_internalId",
        "_externalId",
        "_creationDate",
        "_name",
        "oldFilePath",
        "projectId",
        "dataPrivacy",
        "dataCategory",
        "_sensitivity",
        "_maxRetentionDate"
      ],
      "searchCriteria": {
        "$and": [
          {
            "applicationSource": {
              "$eq": "agpc8"
            }
          }
        ]
      },
      "fileColumns": [
        "id",
        "fileName",
        "size"
      ]
    }
  ]
}
```

---

### 2.3 Suppression de documents CMX  
**Tâche : DeleteDocumentV3**

#### Utilisation
```bash
java 'C:\Chemin\CMX\EXTRACTOR.jar' -resourcesPath='C:\Chemin\Repertoire\RESSOURCES' -task=DeleteDocumentV3
```

#### Paramétrage (deleteDocument.json)
```json
{
  "erase": false,
  "dryRun": false,
  "searchCriteria": {
    "$and": [
      {
        "applicationSource": {
          "$eq": "prnasclio"
        }
      },
      {
        "_maxRetentionDate": {
          "$eq": "1945-01-01T00:00:01+01:00"
        }
      }
    ]
  }
}
```

---

### 2.4 Téléchargement du contenu d’un DocStore  
**Tâche : CoreDataDump**

#### Utilisation
```bash
java 'C:\Chemin\CMX\EXTRACTOR.jar' -resourcesPath='C:\Chemin\Repertoire\RESSOURCES' -task=CoreDataDump
```

#### Paramétrage (coreDataDump.json)
```json
{
  "extractionCriteria": {
    "$and": [
      {
        "applicationSource": {
          "$eq": "prnasclio"
        }
      }
    ]
  }
}
```

---

## 3. Configuration cmx-extractor.properties

```properties
cmx.maam.url=URL de OneLogin par environnement
cmx.maam.user=Client ID
cmx.maam.password=Client Secret

cmx.core.url=URL de CMX Core
cmx.core.storeid=ID du CMX DocStore
cmx.core.nbThreads=Nombre de threads
cmx.core.max-retry=Nombre de retry

cmx.core.profile=Profil CMX
```
