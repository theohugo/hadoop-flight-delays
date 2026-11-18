# Rapport — Jour 2 : Spark, jointures et couche Silver Delta Lake

**Projet : analyse des retards des vols américains de 2015**  
**Binôme : TheoHugo et aminetaleb18**  
**Dates de réalisation : 17 et 18 novembre 2026**

Ce rapport présente les résultats du [notebook du Jour 2](notebooks/J2_Spark_Delta_Silver.ipynb), exécuté intégralement sans erreur.

## 1. Exercices 1.1 et 2.1 — Qualité, nettoyage, jointures et table Silver

- Bronze et Silver : **1 943 809 vols**. Aucun doublon strict métier, aucune exclusion selon les règles de validité appliquées, aucune erreur de conversion sur les colonnes numériques contrôlées.
- Clés de jointure renseignées ; référentiels uniques : **14 compagnies**, **322 aéroports**.
- Trois jointures gauches en **BroadcastHashJoin**, avec **100 % de rapprochement** pour chacune ; effectif strictement conservé. Un taux de rapprochement de 100 % ne garantit pas que tous les attributs enrichis sont renseignés.
- **24 756 retards de départ** et **30 794 retards d'arrivée** sont manquants dans Bronze. Ils restent manquants après typage : on ne les assimile pas à une arrivée à l'heure. Les causes de retard et d'annulation sont des champs conditionnels, à interpréter selon le statut du vol.
- Les coordonnées sont absentes pour **3 aéroports** du référentiel ; filtrer les coordonnées manquantes pour une carte et signaler cette couverture.
- **1 104 197 retards au départ négatifs** sont conservés : ils représentent des avances. Aucun vol annulé n'a d'indicateur `departure_delay_15` renseigné.
- **57 colonnes Silver**, dont date, saison, créneau du départ prévu, indicateur de retard de 15 minutes, minutes positives de retard et indicateur de trajet entre États (`interstate_route`). Ce dernier combine la table de faits et les deux référentiels d'aéroports.
- Évaluation paresseuse : **0 job** lors de la construction du DataFrame de démonstration, **6 jobs** observés après l'action. `select`/`filter` sont narrow ; le `groupBy` introduit un échange de données dans le plan.

Les règles de filtrage contrôlent date, numéro de vol, indicateurs 0/1, distance positive, codes et horaire prévu HHMM. Elles ne prouvent pas l'absence de toute anomalie métier. Les horaires locaux sont conservés sans calculer de durée entre fuseaux. La saison n'est représentée que par un mois de l'extrait : pas de conclusion annuelle.

Preuves : [`reports/j2_first_half.json`](reports/j2_first_half.json) et [`reports/j2_join_plan.txt`](reports/j2_join_plan.txt).

## 2. Exercice 3.1 — Journal `_delta_log` et Schema Enforcement

### Que contient le journal de transactions ?

Chaque transaction validée sur `/lakehouse/silver/flights` produit un fichier JSON numéroté dans `_delta_log`. Le premier, `00000000000000000000.json`, contient **11 actions** :

| Action | Nombre | Contenu |
|---|---:|---|
| `commitInfo` | 1 | Opération `WRITE`, mode `Overwrite`, 1 943 809 lignes écrites |
| `protocol` | 1 | Versions minimales de lecteur (1) et d'écrivain (2) |
| `metaData` | 1 | Identifiant de table et schéma de **57 colonnes** |
| `add` | 8 | Un fichier Parquet ajouté par action |

Une version de la table correspond à l'ensemble des fichiers ajoutés et non retirés par les commits successifs. C'est ce mécanisme qui permet l'atomicité et le voyage dans le temps.

### Delta refuse-t-il un schéma incompatible ?

**Oui.** L'ajout en mode `append` d'un DataFrame aux colonnes `fake_id` et `intruder_col` a été rejeté :

```text
AnalysisException: [DELTA_METADATA_MISMATCH] A metadata mismatch was detected when writing to the Delta table.
```

Après ce refus, la version de la table et son effectif (1 943 809 lignes) sont inchangés : la transaction échoue entièrement, sans écriture partielle. Un fichier Parquet seul n'offre pas cette protection. Une évolution de schéma reste possible, mais seulement de façon explicite avec l'option `mergeSchema`.

## 3. Exercice 3.2 — `MERGE`, historique et Time Travel

### Quel lot avons-nous appliqué ?

Nous avons simulé un lot correctif de la source : deux vols retardés de 10 et 11 minutes reçoivent 5 minutes supplémentaires, et un vol fictif est ajouté. Le rapprochement utilise une clé métier : date, compagnie, numéro de vol, aéroport d'origine et départ prévu. L'opérateur `<=>` compare aussi correctement les valeurs nulles. Avant l'opération, nous avons vérifié que chaque correction visait exactement un vol.

```python
(silver_delta.alias('t').merge(source.alias('s'), match_cond)
 .whenMatchedUpdate(set={c: f's.{c}' for c in updated_cols})
 .whenNotMatchedInsertAll()
 .execute())
```

| Vol | Retard avant | Retard après | Indicateur ≥ 15 min |
|---|---:|---:|---|
| AA 65, JFK, 01/01/2015 | 10 | 15 | 0 → 1 |
| AA 96, DFW, 01/01/2015 | 11 | 16 | 0 → 1 |
| AA 99999 (fictif) | — | −5 | inséré, 0 |

L'historique Delta indique **2 lignes mises à jour et 1 insérée**. Les colonnes dérivées (`departure_delay_15`, `positive_departure_delay`) sont recalculées dans le lot, sinon elles deviendraient incohérentes avec le retard corrigé. Les lignes modifiées portent la source `demo_merge_j2`, ce qui préserve le lignage.

Delta fonctionne en *copy-on-write* : les fichiers Parquet contenant les vols visés sont réécrits (485 613 lignes recopiées), puis les anciens fichiers sont retirés du journal. Ils ne sont pas modifiés sur place et restent donc disponibles pour les versions antérieures.

### Que montre le Time Travel ?

| Version lue | Lignes |
|---|---:|
| Version 0 (création) | 1 943 809 |
| Avant le `MERGE` | 1 943 809 |
| Après le `MERGE` | 1 943 810 |

Les valeurs avant/après du tableau précédent ont été lues sur la même table avec `.option("versionAsOf", n)`. Une analyse peut ainsi être reproduite sur l'état exact des données utilisé à une date donnée.

### Pourquoi restaurer la table ensuite ?

Le lot était une démonstration : le vol fictif et les retards modifiés fausseraient les analyses Gold. Nous avons exécuté `RESTORE TABLE ... TO VERSION AS OF` vers la version précédant le `MERGE`. La table compte de nouveau **1 943 809 lignes** et **aucune ligne `demo_merge_j2`**.

L'historique conserve les opérations `WRITE`, `MERGE` puis `RESTORE` : la correction et son annulation restent auditables. Les versions 1 et 2 de l'historique proviennent d'un essai préalable identique ; la réécriture de la table Silver par la section 2 a créé la version 3.

Preuve : [`reports/j2_second_half.json`](reports/j2_second_half.json).

## 4. Bilan du Jour 2 et suite

La table Silver `/lakehouse/silver/flights` est typée, enrichie par trois jointures broadcast contrôlées et conserve une ligne par vol. Delta Lake apporte les garanties que HDFS seul ne fournit pas : transactions atomiques, contrôle du schéma, mises à jour ciblées et versions consultables. La réplication HDFS protège les blocs ; Delta protège la cohérence de la table.

Au Jour 3, nous mesurerons l'asymétrie des retards pour justifier la médiane et l'IQR, puis construirons les tables Gold : Pareto des minutes de retard par aéroport, évolution temporelle avec `LAG` et classement par groupe avec `ROW_NUMBER`.

### Rejouer le Jour 2

```bash
docker exec -w /workspace/notebooks spark-jupyter jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=900 J2_Spark_Delta_Silver.ipynb
```

Pour rejouer seulement les exercices 1.1 et 2.1 : `docker exec spark-jupyter python /workspace/scripts/run_j2_first_half.py`. L'interface Spark UI n'est disponible que pendant une session Spark active.
