# Rapport — Jour 1 : HDFS, ingestion et couche Bronze

**Projet : analyse des retards des vols américains de 2015**  
**Binôme : TheoHugo et aminetaleb18**  
**Date de réalisation : 16 novembre 2026**

Ce rapport présente les réponses du Jour 1 et les résultats de l'exécution du [notebook](notebooks/J1_HDFS_Ingestion_Bronze.ipynb). Les effectifs proviennent des fichiers préparés et des tables Delta relues ; les résultats de panne proviennent du crash-test enregistré.

## 1. Cadrage du sujet et choix des données

### Quelle est notre problématique métier ?

**Quels aéroports, compagnies et créneaux concentrent les retards ?**

Nous souhaitons distinguer les aéroports qui concentrent beaucoup de minutes de retard, notamment en raison de leur trafic, de ceux qui présentent un taux de retard élevé. Nous étudierons ensuite les différences entre compagnies, horaires et périodes. Les résultats décriront des associations ; ils ne suffiront pas à démontrer qu'un aéroport ou une compagnie cause les retards.

### Quel dataset avons-nous retenu ?

Nous utilisons [2015 Flight Delays and Cancellations](https://www.kaggle.com/datasets/usdot/flight-delays), identifiant Kaggle `usdot/flight-delays`, version 1. Les données proviennent du Bureau of Transportation Statistics américain et comportent trois tables joignables.

| Fichier | Contenu | Lignes de données retenues | Taille locale |
|---|---|---:|---:|
| `flights.csv` | Vols, horaires, retards et annulations | 1 943 809 | 199 285 860 octets |
| `airlines.csv` | Référentiel des compagnies | 14 | 359 octets |
| `airports.csv` | Référentiel des aéroports et coordonnées | 322 | 23 867 octets |

Le volume total est de **199 310 086 octets**, environ **190,08 Mio**. Le fichier principal dépasse 32 Mio : il permet donc d'observer plusieurs blocs HDFS de 16 Mio. Dans ce rapport, **1 Mio = 1 048 576 octets**.

### Quel périmètre avons-nous sélectionné ?

Nous conservons tous les vols de quatre mois, sans filtrer selon leur retard.

| Mois de 2015 | Nombre de vols |
|---|---:|
| Janvier | 469 968 |
| Avril | 485 151 |
| Juillet | 520 718 |
| Novembre | 467 972 |
| **Total** | **1 943 809** |

Ce choix couvre un mois de chaque saison météorologique tout en respectant la volumétrie du TP. Il ne constitue pas un échantillon aléatoire représentatif de toute l'année. Novembre inclut Thanksgiving, ce qui devra être pris en compte dans les interprétations. Octobre n'a pas été retenu, car les codes des aéroports y sont numériques dans la source. Les performances observées en 2015 ne décrivent pas les performances actuelles.

Le script [`scripts/prepare_flights.py`](scripts/prepare_flights.py) télécharge la version 1, sélectionne ces mois et conserve les deux référentiels. Le [manifeste](reports/data_manifest.json) enregistre les tailles, les effectifs par mois et les empreintes SHA-256 permettant de vérifier les fichiers.

### Quelles variables et clés de jointure seront utilisées ?

| Usage | Variables principales |
|---|---|
| Temps | `YEAR`, `MONTH`, `DAY`, `DAY_OF_WEEK`, `SCHEDULED_DEPARTURE` |
| Mesures quantitatives | `DEPARTURE_DELAY`, `ARRIVAL_DELAY`, `DISTANCE`, `ELAPSED_TIME` |
| Catégories | `AIRLINE`, `ORIGIN_AIRPORT`, `DESTINATION_AIRPORT` |
| Annulations | `CANCELLED`, `CANCELLATION_REASON` |

Les jointures prévues pour le Jour 2 sont :

- `flights.airline = airlines.iata_code` pour obtenir le nom de la compagnie ;
- `flights.origin_airport = airports.iata_code` pour enrichir l'aéroport de départ ;
- `flights.destination_airport = airports.iata_code` avec un second alias du référentiel pour la destination.

Nous vérifierons l'unicité des clés des dimensions et les codes non appariés avant de valider ces jointures. La granularité doit rester **une ligne par vol**.

Les retards sont exprimés en minutes et la distance en miles. Un retard négatif correspond à une avance : ce n'est pas automatiquement une erreur. Les horaires HHMM sont locaux ; leur soustraction entre deux aéroports de fuseaux différents ne donne pas directement une durée de vol.

## 2. Exercice 2.1 — Initialiser Spark et contrôler HDFS

La session a été créée avec le nom `J1_HDFS_Ingestion_Bronze`. Pour limiter la consommation locale, nous avons utilisé deux threads Spark et huit partitions de shuffle.

```python
spark = (
    SparkSession.builder
    .appName("J1_HDFS_Ingestion_Bronze")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

default_fs = spark.conf.get("spark.hadoop.fs.defaultFS")
block_size = int(spark.conf.get("spark.hadoop.dfs.blocksize"))
replication = int(spark.conf.get("spark.hadoop.dfs.replication"))
```

| Paramètre vérifié | Valeur obtenue | Signification |
|---|---|---|
| Version Spark | `4.2.0` | Moteur utilisé pour l'exécution |
| Système de fichiers par défaut | `hdfs://namenode:9000` | Les chemins HDFS sont résolus par le NameNode |
| Taille des blocs | `16777216` octets | 16 Mio par bloc au maximum |
| Réplication | `2` | Deux copies de chaque bloc sur des DataNodes distincts |

Le cluster comporte **un NameNode et trois DataNodes** sous Hadoop 3.5.0. Le NameNode gère les métadonnées et les emplacements des blocs ; les DataNodes stockent les blocs. Spark effectue les calculs et lit les données depuis HDFS.

Dans cette installation pédagogique, les DataNodes sont des conteneurs sur la même machine et Spark fonctionne en mode local. La distribution HDFS est réelle entre conteneurs, mais ce montage ne simule pas trois machines physiques indépendantes ni un cluster Spark multi-exécuteurs.

## 3. Copie des fichiers et questions sur les blocs HDFS

Les trois fichiers ont été copiés vers :

```text
/lakehouse/bronze/raw/flight-delays/flights.csv
/lakehouse/bronze/raw/flight-delays/airlines.csv
/lakehouse/bronze/raw/flight-delays/airports.csv
```

La copie utilise l'API Hadoop `FileSystem` depuis Spark. L'inspection utilise `getFileBlockLocations` pour afficher les hôtes de chaque bloc.

### Question 1 — Le nombre de blocs correspond-il à la taille divisée par 16 Mio ?

Pour le fichier principal :

```text
Nombre de blocs = plafond(199 285 860 / 16 777 216) = 12
199 285 860 = 11 × 16 777 216 + 14 736 484
```

Nous avons effectivement observé **12 blocs** : onze blocs de 16 Mio et un dernier bloc de **14 736 484 octets**, soit environ **14,05 Mio**. Les deux petits référentiels occupent chacun un bloc. Les trois fichiers bruts totalisent donc **14 blocs logiques** ; la réplication ne double pas leur nombre logique, mais leur nombre de copies physiques.

Avant la panne, le bloc 0 du fichier des vols était présent sur `datanode2` et `datanode3`, et le bloc 1 sur `datanode2` et `datanode1`. Les sorties du notebook conservent le placement observé ; celui-ci peut changer après une panne et une réparation.

### Question 2 — Un fichier de 3 Mio réserve-t-il réellement 16 Mio sur les DataNodes ?

**Non.** La taille de bloc est une taille maximale, pas une réservation complète pour chaque fichier. Un fichier de 3 Mio tient dans un bloc partiellement rempli. Avec une réplication de 2, ses données représentent environ **6 Mio**, auxquels s'ajoutent les sommes de contrôle et les métadonnées.

Les petits fichiers restent toutefois coûteux en métadonnées et en opérations d'accès. Leur faible taille ne les rend pas gratuits pour le NameNode.

### Question 3 — Pourquoi une réplication de 2 sur trois DataNodes ?

Les deux copies permettent de continuer à lire les blocs lorsqu'un DataNode tombe en panne. Le troisième DataNode fournit une destination disponible pour reconstruire la copie manquante. Après réparation avec deux nœuds survivants, chaque bloc peut à nouveau disposer de deux copies.

Le compromis est un stockage de données d'environ deux fois le volume logique, hors surcoûts. Cette configuration protège contre la panne simple testée ; elle ne garantit pas la survie à toutes les pannes simultanées. Comme les conteneurs partagent une machine, une panne de l'hôte reste un point commun de défaillance.

## 4. Exercice 4.1 — Crash-test, continuité de lecture et partitions

### Protocole réalisé

Le script [`scripts/crash_test_flights.py`](scripts/crash_test_flights.py) a effectué les opérations suivantes :

1. Vérifier que trois DataNodes sont actifs et que des blocs existent.
2. Arrêter `datanode2` avec `docker stop datanode2`.
3. Interroger les métriques JMX jusqu'à ce que le NameNode déclare le nœud mort.
4. Relire le CSV des vols avec Spark pendant la panne et vérifier son effectif.
5. Attendre la fin de la sous-réplication et lancer `hdfs fsck` sur les fichiers bruts.
6. Redémarrer `datanode2` et vérifier le retour à trois nœuds actifs.

Le redémarrage est placé dans un bloc `finally` pour être tenté même si une vérification échoue. Le test a débuté le **16 novembre 2026 à 14 h 19 min 06 s, heure de Paris**.

### Résultats observés

| État du cluster | DataNodes actifs | DataNodes morts | Blocs sous-répliqués | Blocs manquants |
|---|---:|---:|---:|---:|
| Avant la panne | 3 | 0 | 0 | 0 |
| À la détection de la panne | 2 | 1 | 4 | 0 |
| Après réparation, nœud 2 toujours arrêté | 2 | 1 | 0 | 0 |
| Après redémarrage du nœud 2 | 3 | 0 | 0 | 0 |

Ces compteurs JMX portent sur le cluster, qui contenait **24 blocs logiques** au moment du test, incluant les tables Bronze et leurs fichiers Delta. Le contrôle `fsck` ciblant uniquement les trois CSV bruts a validé **14 blocs**, une réplication moyenne de **2,0**, aucun bloc corrompu et l'état **HEALTHY**.

La détection a été observée après **60,9 secondes**, contre environ 50 secondes annoncées dans la configuration pédagogique. Le temps observé comprend l'arrêt Docker et l'attente par interrogations espacées ; ce n'est pas une mesure isolée du seul délai interne de heartbeat. La valeur théorique n'est donc pas un délai exact garanti.

Pendant que `datanode2` était arrêté, Spark a relu **1 943 809 lignes**, soit la totalité de l'extrait. La lecture a donc continué grâce aux copies disponibles sur les survivants. Le retour à zéro bloc sous-répliqué, avant de redémarrer le nœud 2, montre aussi que la seconde copie a été reconstruite. Le [rapport du crash-test](reports/crash_test_flights.json) contient les observations et la sortie complète de `fsck`.

### Quel est le lien entre blocs HDFS et partitions Spark ?

```python
df_check = spark.read.option("header", True).csv(largest_hdfs_path)
df_check.count()                    # 1 943 809
df_check.rdd.getNumPartitions()     # 2
```

Nous observons **12 blocs HDFS mais seulement 2 partitions Spark**. Il n'y a donc pas d'équivalence générale « un bloc HDFS = une partition Spark ».

Les blocs organisent le stockage et la réplication. Les partitions organisent les tâches de calcul et de lecture. Ici, `spark.sql.files.maxPartitionBytes` vaut **134 217 728 octets**, soit **128 Mio**. Spark peut planifier des portions de lecture plus grandes qu'un bloc HDFS pour ce CSV non compressé et découpable. Les paramètres de lecture, le coût d'ouverture des fichiers et le parallélisme interviennent également.

La *data locality* désigne l'intérêt de calculer au plus près des données afin de limiter les transferts réseau. Dans notre mode Spark local, la lecture traverse le réseau Docker vers les DataNodes ; nous ne démontrons pas ici le placement de tâches Spark sur plusieurs machines hébergeant leurs blocs.

## 5. Exercice 5.1 — Construction de la couche Bronze Delta Lake

### Quels traitements avons-nous appliqués ?

Nous avons lu les CSV avec leur en-tête, normalisé les noms de colonnes en minuscules avec des underscores, conservé les valeurs métier en chaînes et ajouté trois colonnes de traçabilité :

| Colonne | Origine | Utilité |
|---|---|---|
| `_ingested_at` | `F.current_timestamp()` | Date et heure de l'ingestion |
| `_source_dataset` | `F.lit("usdot/flight-delays")` | Dataset d'origine |
| `_source_file` | `F.col("_metadata.file_path")` | Chemin du fichier lu |

Le nettoyage métier, le typage des dates et nombres, les jointures et les colonnes calculées sont prévus au Jour 2. Les CSV bruts restent également disponibles dans HDFS.

### Comment les tables ont-elles été écrites ?

Pour chaque source, après ajout des métadonnées :

```python
(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(bronze_delta_path)
)
```

Le mode `overwrite` permet de rejouer cette préparation pédagogique en remplaçant le contenu de la table cible. Il ne s'agit pas encore d'un pipeline incrémental de production.

Delta Lake associe des fichiers de données Parquet, organisés par colonnes, à un journal de transactions `_delta_log`. Le format colonnaire est adapté aux lectures analytiques ciblées ; le journal permet la gestion transactionnelle et les versions qui seront étudiées au Jour 2. La réplication HDFS protège les blocs, tandis que Delta gère la cohérence des tables : ces deux mécanismes ont des rôles différents.

### Quels résultats avons-nous obtenus ?

| Table Delta | Chemin HDFS | Lignes après relecture |
|---|---|---:|
| Vols | `/lakehouse/bronze/flights` | 1 943 809 |
| Compagnies | `/lakehouse/bronze/airlines` | 14 |
| Aéroports | `/lakehouse/bronze/airports` | 322 |

Les trois tables ont été relues avec `spark.read.format("delta")`. Les effectifs correspondent aux données préparées. Les contrôles n'ont trouvé aucune valeur nulle dans `_source_file` ou `_ingested_at`. L'auto-contrôle final a détecté un dossier `_delta_log` dans chacune des trois tables. Les effectifs sont enregistrés dans [`reports/bronze_counts.json`](reports/bronze_counts.json).

## 6. Bilan du Jour 1 et suite

Le notebook a été exécuté intégralement sans erreur. Nous disposons d'un extrait reproductible, des trois fichiers bruts dans HDFS et de trois tables Delta Bronze traçables. Le test a vérifié la continuité de lecture pendant une panne de DataNode, la reconstruction des copies et le rétablissement du cluster.

Au Jour 2, nous typerons les colonnes, mesurerons les valeurs manquantes et doublons, enrichirons les vols avec les référentiels en broadcast et construirons Silver. Les annulations ne seront pas transformées en retards nuls. Les taux de retard devront préciser leur dénominateur, et chaque comparaison devra afficher ses effectifs.

### Pièces de vérification

- [Notebook exécuté du Jour 1](notebooks/J1_HDFS_Ingestion_Bronze.ipynb)
- [Manifeste des données et empreintes SHA-256](reports/data_manifest.json)
- [Effectifs Bronze](reports/bronze_counts.json)
- [Rapport du crash-test et sortie fsck](reports/crash_test_flights.json)
- [Script reproductible de préparation](scripts/prepare_flights.py)
- [Script d'orchestration du crash-test](scripts/crash_test_flights.py)
