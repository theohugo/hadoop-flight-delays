# Revue globale — 19 novembre 2026

## Périmètre et résultat

Revue des notebooks, scripts, rapports et données HDFS du projet. Les Jours 1, 2 et 3 ont été rejoués dans l'ordre ; le Jour 4 a été exécuté pour les tests statistiques et la segmentation uniquement. Le dashboard, réalisé ensuite par Amine, est contrôlé dans la dernière section.

**Résultat : les 24 cellules de code implémentées sont exécutées sans erreur** (J1 : 8 ; J2 : 6 ; J3 : 6 ; J4 : 4). Le script de contrôles croisés termine avec `success: true` dans [le rapport de validation](reports/global_validation.json).

## Corrections apportées

| Constat | Correction | Vérification |
|---|---|---|
| Le Pareto classait en A les seuls aéroports restant sous 80 %, tout en annonçant que ce groupe atteignait 80 %. | Inclure l'aéroport qui franchit le seuil ; même convention à 95 %. | **45 aéroports, 80,28 % des minutes**. Sans le dernier aéroport, le cumul est inférieur à 80 %. Rapports et interprétations mis à jour. |
| `delay_rate_change_pts` contenait une proportion, malgré son nom en points. | Multiplier la différence des taux par 100. La variation relative des minutes est également stockée en pourcentage. | Contrôle de l'égalité entre les valeurs enregistrées et `100 × (taux courant − taux précédent)`. |
| Une assertion entre `MERGE` et `RESTORE` pouvait laisser les lignes de démonstration dans Silver. | Placer la restauration dans un bloc `finally`. | Réexécution du cas normal : 2 mises à jour, 1 insertion, restauration à **1 943 809 vols**, aucune ligne `demo_merge_j2`. Le chemin d'échec est protégé dans le code ; aucune panne artificielle de restauration n'a été injectée. |
| Une explication assimilait chaque fichier à au moins une tâche Spark. | Préciser que Spark planifie des partitions de lecture et peut regrouper de petits fichiers. | Cohérence avec le Jour 1 : 12 blocs CSV, 2 partitions de lecture. |
| Le test matin/soir était présenté comme un test du mécanisme d'accumulation des retards. | Distinguer comparaison des distributions et explication causale. | Hypothèses, tailles d'effet et limites explicites dans le Jour 4. |
| Certains numéros de versions, tailles de fichiers et textes de progression dépendaient d'une ancienne exécution. | Actualiser les rapports et expliquer les valeurs qui dépendent de l'historique local. | Rapports JSON issus de l'exécution du 19 novembre, README et présentation mis à jour. |

## Contrôles sur les données

Les quatre tables Gold d'analyse réconcilient exactement les mêmes totaux avec Silver :

| Indicateur | Total |
|---|---:|
| Vols | 1 943 809 |
| Annulations | 25 907 |
| Vols non annulés au retard de départ renseigné | 1 917 902 |
| Vols retardés d'au moins 15 minutes parmi ces observations | 349 940 |

Les taux sont vérifiés à partir des numérateurs et dénominateurs enregistrés. Les valeurs manquantes ne sont pas assimilées à zéro. Les retards négatifs sont conservés. Les clés des dimensions restent uniques et les trois jointures ne multiplient pas les vols.

| Table Gold | Lignes | Contrôle |
|---|---:|---|
| `airport_delay_pareto` | 322 | Totaux, seuil 80 %, taux |
| `daily_delay_trend` | 122 | Totaux, quatre débuts de mois sans LAG, unités |
| `airline_monthly_performance` | 54 | Totaux et taux |
| `hourly_delay_profile` | 96 | Totaux et taux |
| `airport_segments` | 322 | Aéroports uniques, segments renseignés, tous les vols représentés |

Le fichier Parquet actif de Silver mesure **75 604 074 octets** et son bloc HDFS est configuré à **134 217 728 octets** ; il tient dans un bloc. Cela confirme que le découpage CSV de 16 Mio ne doit pas être extrapolé à l'écriture Parquet. Les anciens fichiers Delta restent disponibles pour le Time Travel ; aucun `VACUUM` n'a été exécuté.

## Vérifications du Jour 4

- Seulement 80 000 observations utiles au maximum passent vers Pandas pour les tests ; l'agrégation de segmentation est faite dans Spark avant de rapatrier les aéroports.
- Deux tests avec hypothèses écrites, correction de Holm et tailles d'effet. Un troisième (Spearman, latitude × taux de retard) a été ajouté ensuite pour croiser les deux sources ; Holm porte alors sur trois tests. Le sous-flux numérique de la p-value de Mann–Whitney est signalé.
- Vérification complémentaire par rééchantillonnage de 122 jours. La dépendance entre jours et la composition des groupes restent des limites.
- Quatre variables de segmentation ; `log1p` seulement sur des caractéristiques non négatives, puis standardisation.
- Comparaison de K=2 à K=6. K=3 retenu, silhouette 0,298 : séparation modérée et faible écart avec K=5, donc prudence sur l'interprétation des groupes.
- 304 aéroports entraînés ; 18 conservés avec la mention « Données limitées ». Le seuil de 100 vols observés et les paramètres sont documentés.

## Vérifications techniques

- Syntaxe Python des cellules contrôlée avant exécution ; exécution effective des quatre notebooks.
- `ruff check scripts` : aucune erreur après correction de l'ordre des imports du nouveau validateur.
- `git diff --check` : aucune erreur d'espacement.
- Contrôles HDFS/Silver/Gold via `scripts/validate_project.py` : succès.
- Le crash-test de DataNode du Jour 1 conserve sa preuve du 16 novembre ; il n'a pas été répété lors de cette revue. La relecture et l'ingestion du Jour 1 ont été réexécutées.

## Complément — dashboard (Amine)

Après un `git pull`, J2, J3 puis J4 ont été réexécutés sur le cluster local pour disposer des tables Gold corrigées et de `airport_segments`, puis `scripts/validate_project.py` a de nouveau réussi. `app/dashboard.py` respecte la [fiche](DASHBOARD-A-FAIRE.md) : lecture des seules tables Gold, taux réagrégés par sommes de numérateurs et dénominateurs, médianes et P90 affichés à leur granularité d'origine, portée de chaque filtre indiquée, mois séparés sur les courbes, ECP/PBG/UST signalés hors carte, requête SQL et justification sous chaque visualisation. Contrôles : `AppTest` de Streamlit sur huit combinaisons de filtres (dont sélection vide et aéroport sans coordonnées) sans exception, puis vérification visuelle sur http://localhost:8501. Détails dans la section « Dashboard Streamlit » de [Jour 4](Jour%204.md).

Un test de Spearman a ensuite été ajouté au notebook J4, qui compte désormais **5 cellules de code** (25 au total pour J1 à J4). La correction de Holm porte sur les trois tests. `scripts/validate_project.py` confirme l'exécution sans erreur des quatre notebooks. Le rapport `reports/global_validation.json` reste celui de la revue du 19 novembre au matin : il donne 4 cellules pour J4 et un périmètre sans dashboard.

Limites scientifiques conservées : quatre mois historiques non représentatifs d'une année entière, dépendance entre vols, différences de composition entre groupes, absence de démonstration causale et clusters descriptifs. Ces limites sont expliquées ; elles ne sont pas masquées par les petites p-values ou un taux de jointure de 100 %.
