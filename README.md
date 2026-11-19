# Flight Delays — Data Lakehouse Hadoop, Spark et Delta Lake

Projet de cours consacré aux retards des vols américains de 2015, basé sur le dépôt pédagogique [aptitek/hadoop-kaggle](https://github.com/aptitek/hadoop-kaggle).

**Problématique : quels aéroports, compagnies et créneaux concentrent les retards ?**

- **Données :** 1 943 809 vols de janvier, avril, juillet et novembre 2015, 14 compagnies et 322 aéroports.
- **Jour 1 terminé :** ingestion HDFS, 12 blocs pour le CSV des vols, trois tables Delta Bronze et crash-test d'un DataNode vérifié.
- **Jour 2 terminé :** diagnostic qualité, nettoyage, jointures broadcast, table Silver, journal Delta, Schema Enforcement, `MERGE` et Time Travel validés.
- **Jour 3 terminé :** asymétrie des retards (médiane, IQR, P90), quatre tables Gold (Pareto des aéroports, tendance quotidienne avec `LAG`, compagnies par mois avec `ROW_NUMBER`, profil horaire) et compaction `OPTIMIZE`.
- **Jour 4, partie tests et segmentation terminée :** trois tests statistiques (dont un Spearman qui croise le référentiel des aéroports et les vols), bootstrap des jours et trois profils K-Means.
- **Jour 4 terminé :** dashboard Streamlit sur les cinq tables Gold (carte, compagnies, évolution, profils d'aéroports ; filtres mois, compagnie et aéroport), vérifié sur http://localhost:8501.
- [Présentation et progression du projet](PROJET-VOLS.md)
- [Démarrage sous Windows](DEMARRAGE-WINDOWS.md)
- [Notebook Jour 1 exécuté](notebooks/J1_HDFS_Ingestion_Bronze.ipynb)
- [Rapport Jour 1 — réponses aux exercices](Jour%201.md)
- [Notebook Jour 2 exécuté](notebooks/J2_Spark_Delta_Silver.ipynb)
- [Rapport Jour 2 — Silver et transactions Delta](Jour%202.md)
- [Notebook Jour 3 exécuté](notebooks/J3_Analytics_SQL_Gold.ipynb)
- [Rapport Jour 3 — statistiques et couche Gold](Jour%203.md)
- [Rapport Jour 4 — tests, segmentation et dashboard](Jour%204.md)
- [Revue globale et vérifications](REVUE-GLOBALE.md)
- [Cahier des charges du dashboard](DASHBOARD-A-FAIRE.md) et [application](app/dashboard.py)

Les fichiers CSV restent hors Git. Après démarrage de Docker avec `docker compose up -d --build`, les données peuvent être régénérées avec :

```bash
docker exec spark-jupyter python /workspace/scripts/prepare_flights.py
```

Source : [2015 Flight Delays and Cancellations](https://www.kaggle.com/datasets/usdot/flight-delays), version 1. Le périmètre de quatre mois ne représente pas toute l'année ; les limites sont documentées dans le projet.

---

## Documentation de la stack pédagogique

# TP Big Data Lakehouse — HDFS (3 DataNodes) + PySpark + Delta Lake + Streamlit

Environnement pédagogique complet compatible nativement **`linux/amd64` (PC x86_64 / WSL2)** et **`linux/arm64` (Apple Silicon M1–M4 / Linux ARM64)** sur **Ubuntu 26.04 LTS**, reposant sur un **cluster HDFS 3.5 réel distribué (1 NameNode + 3 DataNodes)** avec blocs réduits à **16 Mo** et détection de panne abaissée à **~50 secondes**, couplé à une image mutualisée légère (`ubuntu:26.04` + **Python 3.15** via `uv` + OpenJDK 21 + JARs Delta pré-embarqués) pour **PySpark 4.2**, **Delta Lake 4.4**, **JupyterLab 4.6** et **Streamlit**.

Chaque étudiant / groupe **choisit librement sa thématique métier et combine un dataset multi-tables ou plusieurs datasets publics complémentaires sur Kaggle** (croisés par jointure distribuée) pour construire de bout en bout une architecture **Data Lakehouse Médaillon (Bronze → Silver → Gold → Dashboard)**.

```mermaid
flowchart LR
    K1["Dataset Kaggle #1 (Faits)<br/>data_raw/source_1/"] --> B["Bronze (Multi-tables)<br/>/lakehouse/bronze"]
    K2["Dataset Kaggle #2 (Enrichissement)<br/>data_raw/source_2/"] --> B
    B -->|"Nettoyage & Jointure Spark"| S["Silver (ACID Delta)<br/>/lakehouse/silver"]
    S --> G["Gold (KPI & Segments)<br/>/lakehouse/gold"]
    G --> ST["Dashboard Streamlit<br/>(:8501)"]

    subgraph HDFS["Cluster HDFS (blocs 16 Mo, réplication = 2)"]
      NN["NameNode (:9870 / :9000)"]
      DN1["DataNode 1"]
      DN2["DataNode 2"]
      DN3["DataNode 3"]
    end

    B -.-> HDFS
    S -.-> HDFS
    G -.-> HDFS
```

---

## Critères de choix de vos Datasets Kaggle (Mono ou Multi-Sources avec Jointure)

Pour tirer pleinement parti de l'infrastructure distribuée, des jointures Spark (Jour 2) et des exercices statistiques (Jours 3 & 4), choisissez sur [Kaggle Datasets](https://www.kaggle.com/datasets) **soit un dataset relationnel multi-tables, soit 2 (ou plusieurs) datasets complémentaires** respectant ces critères :

1. **Volumétrie cumulée : ~50 Mo à 500 Mo** (avec au moins un fichier dépassant **20 à 32 Mo** afin d'observer physiquement le découpage multi-blocs de **16 Mo** sur HDFS au Jour 1).
2. **Axe de jointure clair entre tables / datasets** :
   - **Jointure temporelle** (`date`, `jour + heure`, `yyyy-MM`) — *ex. courses de mobilité / vols aériens / consommation électrique × relevés météo horaires ou calendrier des jours fériés*.
   - **Jointure géographique** (`code pays ISO`, `ville / État`, `code postal`, `aéroport IATA`) — *ex. commandes e-commerce / immobilier / santé × indicateurs socio-démographiques ou référentiel géographique*.
   - **Jointure par identifiant ou nomenclature métier** (`product_id`, `category`, `station_id`, `carrier_code`).
3. **Richesse des attributs** :
   - Au moins **une colonne temporelle** (dates / timestamps d'événements) pour les analyses de séries et de croissance (`LAG`).
   - Des **variables quantitatives continues** (montants, durées, distances, consommations, retards, températures...) permettant d'étudier l'asymétrie (médiane vs moyenne, IQR) et de tester statistiquement l'effet croisé entre vos datasets au Jour 4.
   - Des **dimensions catégorielles ou géographiques** permettant de pratiquer le *Broadcast Hash Join* (et/ou *SortMergeJoin*) sans explosion N-à-M.

---

## Arborescence du dépôt

```text
.
├── .devcontainer/
│   └── devcontainer.json       # Connexion VS Code directe au conteneur spark-jupyter
├── .vscode/
│   ├── extensions.json         # Extensions recommandées (Python, Jupyter, Ruff, Docker, Task)
│   ├── launch.json             # Débogage PySpark & prévisualisation Streamlit (:8502)
│   ├── settings.json           # Kernel Python/Jupyter pré-configuré, Ruff & propreté éditeur
│   └── tasks.json              # Actions VS Code en 1 clic (Démarrer, Télécharger Kaggle, Crash-test...)
├── docker-compose.yml          # Cluster HDFS (1 NN + 3 DN) + JupyterLab + Streamlit
├── Taskfile.yml                # Orchestration go-task (démarrage, crash-test, tests J1..J4)
├── mise.toml                   # Gestionnaire d'outils (go-task, uv, python 3.15)
├── pyproject.toml              # Dépendances Python compatibles uv (Python >= 3.15)
├── config/
│   ├── core-site.xml           # Point d'entrée RPC hdfs://namenode:9000
│   ├── hdfs-site.xml           # Blocs 16 Mo, réplication=2, détection panne ~50s
│   └── spark-defaults.conf     # Pré-configuration automatique HDFS + Delta Lake
├── docker/
│   ├── hadoop/
│   │   └── Dockerfile          # Image HDFS 3.5.0 Ubuntu 26.04 native multi-arch (amd64 & arm64)
│   └── workspace/
│       ├── Dockerfile          # Image Ubuntu 26.04 multi-arch (Python 3.15 + uv + OpenJDK 21 + Delta 4.4)
│       └── requirements.txt    # Dépendances Python 3.15 pour le workspace
├── data_raw/                   # Dépôt local de vos fichiers Kaggle avant ingestion HDFS
├── notebooks/                  # Notebooks TP pas-à-pas agnostiques (Jours 1 à 4)
│   ├── J1_HDFS_Ingestion_Bronze.ipynb
│   ├── J2_Spark_Delta_Silver.ipynb
│   ├── J3_Analytics_SQL_Gold.ipynb
│   └── J4_Stats_Segmentation_Dashboard.ipynb
└── app/
    └── dashboard.py            # Squelette Streamlit connecté à /lakehouse/gold/
```

---

## Démarrage rapide

### Avec `mise` et `go-task` (recommandé)

```bash
# 1. Installer automatiquement task (go-task) et uv si absents
mise trust && mise install

# 2. Construire l'image et démarrer tout le cluster (attend la disponibilité des 3 DataNodes)
task up

# 3. Télécharger un ou plusieurs datasets Kaggle dans data_raw/<nom-dataset>/
task data:download DATASET="auteur1/dataset-1,auteur2/dataset-2"
```

### Sans `task` (commandes Docker natives)

```bash
docker compose up -d --build
```

---

## Interfaces exposées

| Service | URL | Détails |
|---|---|---|
| **HDFS NameNode UI** | http://localhost:9870 | Onglets *Datanodes* & *Utilities > Browse the file system* |
| **JupyterLab** | http://localhost:8888 | Mot de passe / Token : `bigdata` |
| **Spark Web UI** | http://localhost:4040 | DAG, Stages & Shuffle *(actif dès qu'une `SparkSession` tourne)* |
| **Dashboard Streamlit** | http://localhost:8501 | Lecture des tables Delta de `/lakehouse/gold/` avec cache de 10 min |

---

## Travailler dans VS Code (Kernel Jupyter & Conteneur pré-configurés)

### Option A — Mode Dev Container intégré *(recommandé : auto-complétion PySpark, Ruff, Git & Kernel en 1 clic)*
1. Ouvrez ce dossier dans VS Code et installez l'extension recommandée **Dev Containers** (`ms-vscode-remote.remote-containers`).
2. Cliquez sur **Reopen in Container** (ou `Ctrl+Shift+P` → `Dev Containers: Reopen in Container`).
3. VS Code vous connecte directement dans `spark-jupyter` (`/workspace`) avec le kernel **`Python 3.15 (PySpark 4.2 + Delta Lake 4.4)`**, Pylance, Ruff, `git`, `jq` et `pytest` prêts à l'emploi.

### Option B — Mode Léger depuis l'hôte *(idéal pour les PC portables à 8 Go de RAM)*
1. Lancez `task up`.
2. Ouvrez un notebook dans `notebooks/`, cliquez sur **Select Kernel** → **Existing Jupyter Server...** → `http://localhost:8888/?token=bigdata`.

---

## Programme des Notebooks (`notebooks/`)

1. **`J1_HDFS_Ingestion_Bronze.ipynb` — Cadrage du sujet, HDFS & Tolérance aux pannes**
   - Sélection et téléchargement de votre dataset Kaggle via `kagglehub`.
   - Téléversement sur HDFS, inspection du découpage en **blocs de 16 Mo** et de la **Data Locality** (`1 bloc = 1 partition Spark`).
   - **Crash-test en direct** : arrêt de `datanode2`, détection en ~50s, auto-réplication sur le 3e DataNode et lecture sans interruption.
   - Ingestion vers la couche **Bronze** (`/lakehouse/bronze/`) au format Delta Lake avec métadonnées de traçabilité.
2. **`J2_Spark_Delta_Silver.ipynb` — Nettoyage, Jointures distribuées & Transactions ACID**
   - Profilage qualité, typage fort, dédoublonnage et création d'indicateurs métier propres à votre sujet.
   - Optimisation des jointures (**Broadcast Hash Join** vs SortMergeJoin) et prévention des produits cartésiens N-M.
   - Garanties ACID Delta Lake sur `/lakehouse/silver/` : `_delta_log`, **Schema Enforcement**, **Upsert (`MERGE INTO`)** et **Time Travel (`VERSION AS OF`)**.
3. **`J3_Analytics_SQL_Gold.ipynb` — Spark SQL avancé, Rigueur statistique & Couche Gold**
   - Diagnostic d'asymétrie (`SKEWNESS`) : justification statistique du couple **Médiane (`PERCENTILE_APPROX`) + IQR** face à la moyenne.
   - Fonctions de fenêtrage SQL (`DENSE_RANK`, courbe cumulée de **Pareto**, `ROW_NUMBER`, évolution temporelle `LAG`).
   - Alimentation de vos tables **Gold** (`/lakehouse/gold/`) et compaction `OPTIMIZE`.
4. **`J4_Stats_Segmentation_Dashboard.ipynb` — Tests statistiques, Segmentation & Dashboard**
   - Tests d'hypothèses non-paramétriques (`scipy.stats` : **Mann-Whitney U**, **Kruskal-Wallis**, **Spearman**).
   - Segmentation / typologie non supervisée (**K-Means** avec `log1p` + `StandardScaler`) enregistrée en couche Gold.
   - Restitution décisionnelle interactive et documentée dans `app/dashboard.py` sur `http://localhost:8501`.

---

## Commandes `task` principales (`Taskfile.yml`)

```bash
task up                                  # Démarrer le cluster et attendre les 3 DataNodes + Jupyter + Streamlit
task status                              # État des conteneurs et rapport HDFS (dfsadmin -report)
task data:download DATASET="a/ds1,b/ds2" # Télécharger un ou plusieurs datasets dans data_raw/<nom>/
task hdfs:ls                             # Lister l'occupation disque des couches Bronze / Silver / Gold
task hdfs:fsck                           # Inspecter les blocs de 16 Mo et leurs DataNodes hôtes
task hdfs:crash-dn2                      # [TP Jour 1] Couper datanode2 pour observer l'auto-réparation en ~50s
task hdfs:restore-dn2                    # [TP Jour 1] Relancer datanode2
task test:docker                         # Vérifier la santé de HDFS, JupyterLab et Streamlit
task test:notebooks                      # Exécuter et valider vos 4 notebooks (J1 -> J4)
task down                                # Arrêter les conteneurs (conserve les volumes HDFS)
task clean                               # Arrêter et purger les volumes HDFS
```


## Vérification du projet sur cette machine

Les notebooks J1 à J4 ont été exécutés dans l'ordre. Pour contrôler les résultats sans réécrire les données :

```bash
docker exec spark-jupyter python /workspace/scripts/validate_project.py
```

Le rapport est écrit dans `reports/global_validation.json`. Après un clone, les volumes HDFS ne sont pas téléchargés depuis Git : préparer les données puis exécuter J1, J2, J3 et J4 dans cet ordre. Le dashboard (http://localhost:8501) lit ensuite les tables Gold ; il est contrôlé à part (voir la section « Dashboard Streamlit » de [Jour 4](Jour%204.md)).
