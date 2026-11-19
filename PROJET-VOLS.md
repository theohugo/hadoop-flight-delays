# Retards des vols américains — fil rouge Lakehouse

**Question : quels aéroports, compagnies et créneaux concentrent les retards ?**

## Données

Source : [2015 Flight Delays and Cancellations](https://www.kaggle.com/datasets/usdot/flight-delays), `usdot/flight-delays`, version 1.

L'extrait comprend **1 943 809 vols** de janvier, avril, juillet et novembre 2015, ainsi que **14 compagnies** et **322 aéroports**. Le CSV des vols occupe 199 285 860 octets (environ 190 Mio), soit **12 blocs HDFS de 16 Mio** avant réplication. Les quatre mois sont conservés intégralement, sans filtrage sur le retard. Empreintes SHA-256 et effectifs par mois : `reports/data_manifest.json`.

Les résultats concernent ces mois historiques : ils ne décrivent ni les performances actuelles ni une année entière. Novembre inclut Thanksgiving. Une association statistique ne suffit pas à attribuer une responsabilité causale à un aéroport ou une compagnie.

## Progression

- **Jour 1 terminé et exécuté sans erreur :** cadrage, contrôle des données, copie HDFS, inspection des blocs, lecture Spark et tables Delta Bronze (`flights`, `airlines`, `airports`). Les résultats d'exécution sont dans le notebook.
- **Résilience vérifiée :** lecture des 1 943 809 vols pendant la panne, aucun bloc manquant après réparation, puis retour à trois DataNodes. `scripts/crash_test_flights.py` enregistre la panne, la lecture sur deux nœuds, la réparation et le redémarrage dans `reports/crash_test_flights.json`.
- **Jour 2 terminé :** diagnostic qualité, typage, trois jointures broadcast contrôlées et table Silver de 1 943 809 vols. Journal `_delta_log`, refus d'un schéma incompatible, `MERGE` (2 mises à jour, 1 insertion), Time Travel puis `RESTORE` de l'état validé. Voir [le rapport](Jour%202.md) et `reports/j2_second_half.json`.
- **Jour 3 terminé :** retards très asymétriques (skewness 7,4 ; moyenne 9 min, médiane −2 min), d'où médiane, IQR, P90 et taux ≥ 15 min. Quatre tables Gold : `airport_delay_pareto`, `daily_delay_trend`, `airline_monthly_performance`, `hourly_delay_profile`. Silver compactée par `OPTIMIZE` (8 fichiers → 1). Voir [le rapport](Jour%203.md) et `reports/j3_gold.json`.
- **Jour 4, partie tests et segmentation terminée :** Mann–Whitney, Kruskal–Wallis, correction de Holm, contrôle par bootstrap des jours, K-Means (3 profils sur 304 aéroports ; 18 en données limitées). Table Gold `airport_segments` : 322 aéroports. Voir [Jour 4](Jour%204.md).
- **À faire par Amine :** dashboard Streamlit et vérification de son interface. Voir [la fiche dédiée](DASHBOARD-A-FAIRE.md).
- **Revue globale du 19 novembre :** les quatre notebooks ont été rejoués sans erreur ; les totaux Silver/Gold et la segmentation sont contrôlés. Voir [Revue globale](REVUE-GLOBALE.md).

## Reprendre le travail

Dans PowerShell, depuis ce dossier :

```powershell
.\cours.ps1 up
.\cours.ps1 test:docker
```

Ouvrir [JupyterLab](http://localhost:8888/lab?token=bigdata), puis `notebooks/J1_HDFS_Ingestion_Bronze.ipynb`.

Pour régénérer l'extrait (téléchargement initial nécessaire) :

```powershell
docker exec spark-jupyter python /workspace/scripts/prepare_flights.py
```

Pour rejouer le notebook puis le crash-test :

```powershell
docker exec spark-jupyter jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=600 /workspace/notebooks/J1_HDFS_Ingestion_Bronze.ipynb
python scripts/crash_test_flights.py
```

## Démarche retenue

**Silver :** dates et types numériques, contrôle des clés et doublons, jointures gauches avec les deux référentiels en broadcast. Ajouter saison, créneau prévu, indicateur de retard ≥ 15 minutes et minutes positives de retard. Ne pas supprimer les avances (retards négatifs), ni assimiler une valeur manquante à zéro.

**Gold :** classement/Pareto des minutes de retard par aéroport, performance des compagnies avec effectifs et P90, évolution par date et créneau. Comparer volume total et taux : un gros aéroport peut concentrer les retards sans avoir le pire taux.

**Statistiques :** comparer les distributions matin/soir avec Mann–Whitney et les quatre mois avec Kruskal–Wallis. Décrire aussi les écarts observés et les limites (groupes de composition différente, dépendances entre vols), au-delà des seules p-values.

**Segmentation :** K-Means sur les aéroports (volume, médiane/P90 du retard, taux d'annulation). Appliquer `log1p` uniquement aux mesures non négatives adaptées, puis standardiser et interpréter les profils.

**Dashboard :** carte des aéroports, classement des compagnies, évolution et répartition des retards, filtres mois/compagnie/aéroport. Lecture des seules tables Gold et explication des indicateurs.

## Mise à jour du cours

Base pédagogique intégrée : `6f39640` (branche `main` du professeur). Stack du professeur : Hadoop 3.5, PySpark 4.2, Delta Lake 4.4 et Python 3.15. Les contrôles de disponibilité du Taskfile restent adaptés à Windows ; `task` est fixé à 3.54.0 et Java 25 reste disponible sur l'hôte. Une sauvegarde Git stash des adaptations précédentes est conservée.
