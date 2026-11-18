# Rapport — Jour 3 : Spark SQL, statistiques robustes et couche Gold

**Projet : analyse des retards des vols américains de 2015**  
**Binôme : TheoHugo et aminetaleb18**  
**Date de réalisation : 18 novembre 2026**

Ce rapport présente les résultats du [notebook du Jour 3](notebooks/J3_Analytics_SQL_Gold.ipynb), exécuté intégralement sans erreur. Toutes les analyses partent de la table Silver `/lakehouse/silver/flights` (1 943 809 vols), exposée en vue SQL `v_silver`. Seules les 18 colonnes utiles sont projetées et mises en cache, ce qui évite de relire les 57 colonnes sur HDFS à chaque requête.

## 1. Dénominateurs utilisés

Un taux n'a de sens qu'avec son dénominateur explicite :

| Indicateur | Numérateur | Dénominateur |
|---|---|---|
| Taux d'annulation | Vols annulés | Tous les vols programmés |
| Taux de retard ≥ 15 min | Vols partis avec au moins 15 min de retard | Vols non annulés dont le retard est renseigné |
| Médiane, IQR, P90 du retard | — | Vols non annulés dont le retard est renseigné |
| Total des minutes de retard | Somme des retards positifs (les avances comptent 0) | — |

Les vols annulés ne sont jamais comptés comme « à l'heure ». Les taux sont calculés avec `TRY_DIVIDE`, qui renvoie une valeur nulle au lieu d'une erreur si un groupe n'a aucun vol observé ; aucun groupe n'est dans ce cas.

Contrôle de cohérence : pour chacune des quatre tables Gold, la somme des vols (1 943 809), des annulations (25 907), des vols observés (1 917 902) et des vols retardés d'au moins 15 min (349 940) est identique à celle de Silver.

## 2. Exercice 1.1 — Asymétrie : moyenne ou médiane ?

```sql
SELECT COUNT(*), AVG(minutes), STDDEV(minutes), SKEWNESS(minutes),
       PERCENTILE_APPROX(minutes, 0.25), PERCENTILE_APPROX(minutes, 0.50), ...
FROM observations GROUP BY measure
```

| Mesure | Vols | Moyenne | Écart-type | Skewness | P25 | Médiane | P75 | P90 | P95 | Max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Retard au départ | 1 917 902 | 8,98 | 35,84 | **7,36** | −5 | **−2** | 7 | 34 | 65 | 1 988 |
| Retard à l'arrivée | 1 913 015 | 4,18 | 38,17 | **6,25** | −13 | **−5** | 8 | 34 | 65 | 1 971 |

Valeurs en minutes.

### Que révèlent ces résultats ?

Au départ, la moyenne (8,98 min) dépasse la médiane (−2 min) d'environ **11 minutes**. Plus de la moitié des vols partent en avance (57,6 %), mais une longue traîne de retards importants tire la moyenne vers le haut : le P95 atteint 65 min et le maximum dépasse 33 heures. Le coefficient d'asymétrie de 7,36 confirme cette forte asymétrie à droite. L'écart-type, lui aussi dominé par la traîne, ne décrit pas bien la dispersion des vols ordinaires.

### Quels indicateurs avons-nous retenus ?

- **Médiane et IQR** (P75 − P25 = 12 min au départ) pour le retard typique et sa dispersion, peu sensibles aux valeurs extrêmes.
- **P90** pour décrire la queue de distribution, c'est-à-dire les retards qui pèsent réellement sur les voyageurs.
- **Taux de retard ≥ 15 min** (18,25 % des vols observés), seuil de ponctualité du ministère américain des transports.
- **Total des minutes de retard** pour l'analyse de concentration (Pareto).

La médiane est proche de zéro dans presque tous les groupes et discrimine donc peu. Les comparaisons entre groupes s'appuient surtout sur le taux de retard et le P90, toujours accompagnés des effectifs. La moyenne est conservée à titre indicatif.

## 3. Exercice 2.1 — Table Gold #1 : Pareto des aéroports

Table **`/lakehouse/gold/airport_delay_pareto`** : 322 lignes, une par aéroport d'origine.

```sql
DENSE_RANK() OVER (ORDER BY total_delay_minutes DESC) AS rank_total_minutes,
SUM(total_delay_minutes) OVER (ORDER BY total_delay_minutes DESC, airport
                               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
  / SUM(total_delay_minutes) OVER () AS cumulative_share_minutes
```

Le second critère de tri (`airport`) rend le cumul déterministe en cas d'égalité. La table contient aussi les effectifs, les taux, la médiane, l'IQR, le P90, une classe de Pareto (A : 80 % des minutes ; B : jusqu'à 95 % ; C : le reste) et les coordonnées pour la carte du dashboard. Trois aéroports n'ont pas de coordonnées dans le référentiel : ECP (1 496 vols), PBG (106) et UST (60). Ils restent dans les classements, mais ne pourront pas figurer sur la carte.

| Rang | Aéroport | Vols | Minutes de retard | Taux ≥ 15 min | Part cumulée |
|---:|---|---:|---:|---:|---:|
| 1 | ORD — Chicago O'Hare | 103 590 | 1 637 645 | 23,7 % | 7,3 % |
| 2 | ATL — Atlanta | 125 524 | 1 227 733 | 16,8 % | 12,7 % |
| 3 | DFW — Dallas-Fort Worth | 87 449 | 1 174 415 | 22,1 % | 17,9 % |
| 4 | DEN — Denver | 70 792 | 1 019 818 | 24,0 % | 22,4 % |
| 5 | LAX — Los Angeles | 70 578 | 870 282 | 20,5 % | 26,3 % |

### Les retards sont-ils concentrés ?

**44 aéroports sur 322 (13,7 %)** concentrent 80 % des minutes de retard ; les 10 premiers en cumulent 39,9 %. Mais ces 44 aéroports assurent aussi **77,8 % des vols**. La concentration des minutes reflète donc d'abord celle du trafic : il ne s'agit pas d'une poignée d'aéroports anormalement mauvais.

### Volume ou taux : les classements concordent-ils ?

Non. Pour le vérifier, la table contient un second rang, par taux de retard, limité aux aéroports d'au moins 5 000 vols afin d'écarter les petits effectifs :

| Rang par taux | Aéroport | Vols | Taux ≥ 15 min | Rang en minutes |
|---:|---|---:|---:|---:|
| 1 | MDW — Chicago Midway | 29 467 | 24,3 % | 16 |
| 2 | DEN — Denver | 70 792 | 24,0 % | 4 |
| 3 | ORD — Chicago O'Hare | 103 590 | 23,7 % | 1 |
| 4 | BWI — Baltimore | 31 437 | 23,6 % | 14 |
| 5 | DAL — Dallas Love Field | 21 611 | 23,2 % | 26 |

**Atlanta** est 2e en minutes cumulées, mais son taux (16,8 %) est inférieur au taux global : son poids vient de son trafic. **Chicago Midway** a le plus fort taux, mais n'est que 16e en minutes. **Chicago O'Hare** et **Denver** cumulent volume et taux élevé : ce sont les aéroports où une amélioration de la ponctualité aurait le plus d'effet sur le total.

## 4. Exercice 3.1 — Table Gold #2 : dynamique temporelle avec `LAG`

Table **`/lakehouse/gold/daily_delay_trend`** : 122 lignes, une par jour.

```sql
LAG(delay_rate_15) OVER w                       AS prev_day_delay_rate,
delay_rate_15 - LAG(delay_rate_15) OVER w       AS delay_rate_change_pts,
AVG(delay_rate_15) OVER (PARTITION BY month ORDER BY flight_date
                         ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS delay_rate_7d_avg
...
WINDOW w AS (PARTITION BY month ORDER BY flight_date)
```

L'extrait n'est pas continu : sans `PARTITION BY month`, le `LAG` comparerait le 31 janvier au 1er avril. Le premier jour de chaque mois n'a donc pas de valeur précédente, ce que vérifie une assertion. L'évolution d'un taux est exprimée en points, et celle des minutes en pourcentage.

| Mois | Vols | Taux ≥ 15 min | Taux d'annulation | Médiane | P90 |
|---|---:|---:|---:|---:|---:|
| Janvier | 469 968 | 19,8 % | **2,5 %** | −2 | 37 |
| Avril | 485 151 | 16,6 % | 0,9 % | −2 | 29 |
| Juillet | 520 718 | **21,3 %** | 0,9 % | −1 | 42 |
| Novembre | 467 972 | 15,0 % | 1,0 % | −2 | 26 |

Juillet a le plus fort taux de retard et le P90 le plus élevé ; janvier se distingue par ses annulations. Les journées les plus difficiles sont **du 3 au 6 janvier** (de 34,7 % à 46,7 % de vols retardés ; médiane de +12 min le 4 janvier). La plus forte hausse d'un jour sur l'autre a lieu le **18 juillet** : +14,8 points, de 19,1 % à 33,9 %. Les données agrégées ne permettent pas d'en attribuer la cause : les colonnes de causes de retard de Silver pourraient être exploitées pour aller plus loin.

Un seul mois représente chaque saison : ces écarts restent descriptifs. Leur significativité sera testée au Jour 4 avec Kruskal-Wallis.

## 5. Exercice 4.1 — Table Gold #3 et compaction `OPTIMIZE`

### Quelle compagnie est la moins ponctuelle chaque mois ?

Table **`/lakehouse/gold/airline_monthly_performance`** : 54 lignes, compagnie × mois. On attendrait 56 lignes (14 × 4), mais US Airways (US) n'a aucun vol en juillet ni en novembre dans la source : après sa fusion avec American Airlines, ses vols n'y apparaissent plus sous son propre code.

```sql
ROW_NUMBER() OVER (PARTITION BY month ORDER BY delay_rate_15 DESC, airline) AS rank_in_month
```

| Mois | Compagnie (rang 1) | Vols | Taux ≥ 15 min |
|---|---|---:|---:|
| Janvier | MQ — American Eagle | 29 900 | 29,1 % |
| Avril | F9 — Frontier | 7 148 | 22,6 % |
| Juillet | UA — United | 46 478 | 29,7 % |
| Novembre | F9 — Frontier | 7 763 | 21,3 % |

La compagnie en tête change selon le mois et les effectifs varient fortement. American Eagle cumule aussi 7,7 % d'annulations en janvier. Ces classements ne suffisent pas à désigner une compagnie structurellement moins ponctuelle.

### Les créneaux horaires comptent-ils ?

Table complémentaire **`/lakehouse/gold/hourly_delay_profile`** : 96 lignes (4 mois × 24 heures de départ prévu), qui répond à l'axe « créneaux » de la problématique.

| Créneau prévu | Vols | Taux ≥ 15 min | Médiane | P90 |
|---|---:|---:|---:|---:|
| Nuit (0 h – 5 h 59) | 47 965 | 7,2 % | −3 | 8 |
| Matin (6 h – 11 h 59) | 754 527 | 11,5 % | −3 | 18 |
| Après-midi (12 h – 17 h 59) | 714 789 | 21,2 % | −1 | 39 |
| Soir (18 h – 23 h 59) | 426 528 | **26,4 %** | 0 | **51** |

Le taux de retard et le P90 augmentent nettement au fil de la journée. Ce gradient est compatible avec une accumulation des retards (un avion en retard retarde ses rotations suivantes), hypothèse que nous testerons au Jour 4 avec Mann-Whitney (matin contre soir). Les créneaux ne regroupent pas les mêmes aéroports ni les mêmes compagnies : la comparaison reste une association.

### Pourquoi compacter avec `OPTIMIZE` ?

`OPTIMIZE` a réécrit les **8 fichiers** actifs de la table Silver en **1 seul fichier** (75 604 073 octets), dans une transaction Delta. L'effectif est resté de 1 943 809 lignes.

Les micro-fichiers pénalisent HDFS parce que le NameNode garde en mémoire un objet pour chaque fichier et chaque bloc (de l'ordre de 150 octets chacun) : c'est leur nombre qui le charge, pas leur volume. Chaque ouverture de fichier coûte en outre un appel au NameNode et une connexion à un DataNode, et Spark planifie au moins une tâche par fichier.

Constat sur notre cluster : `hdfs fsck` montre que le fichier compacté (72 Mio) n'occupe qu'**un seul bloc HDFS**, comme les autres fichiers Parquet de Silver, malgré la taille de bloc de 16 Mio configurée. L'écrivain Parquet crée en effet ses fichiers avec une taille de bloc au moins égale à celle d'un *row group* (128 Mio par défaut), pour qu'un row group ne soit pas coupé entre deux blocs. Le découpage en blocs de 16 Mio observé au Jour 1 concerne les CSV bruts.

Les anciens fichiers restent sur HDFS pour le Time Travel tant qu'un `VACUUM` ne les supprime pas. Nous ne l'exécutons pas, afin de conserver l'historique démontré au Jour 2. Les tables Gold sont petites et écrites avec `coalesce(1)` : un fichier chacune.

## 6. Bilan du Jour 3 et suite

| Table Gold | Lignes | Usage dans le dashboard |
|---|---:|---|
| `airport_delay_pareto` | 322 | Carte, Pareto, classement volume / taux |
| `daily_delay_trend` | 122 | Courbe quotidienne et moyenne glissante |
| `airline_monthly_performance` | 54 | Classement des compagnies par mois |
| `hourly_delay_profile` | 96 | Profil horaire des retards |

Ces tables stockent les effectifs (vols, vols observés, vols retardés) à côté des taux : le dashboard pourra ainsi réagréger correctement plusieurs mois sans moyenner des taux. Les médianes et P90, eux, ne s'additionnent pas et restent valables à la granularité de chaque table.

Au Jour 4, nous testerons la différence matin / soir (Mann-Whitney) et entre les quatre mois (Kruskal-Wallis), segmenterons les aéroports par K-Means, puis construirons le dashboard Streamlit à partir de ces seules tables Gold.

### Pièces de vérification

- [Notebook exécuté du Jour 3](notebooks/J3_Analytics_SQL_Gold.ipynb)
- [Résultats chiffrés](reports/j3_gold.json)

### Rejouer le Jour 3

```bash
docker exec -w /workspace/notebooks spark-jupyter jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=900 J3_Analytics_SQL_Gold.ipynb
```

Le notebook a été rejoué une seconde fois pour vérification : toutes les sorties sont identiques, sauf `OPTIMIZE`, qui n'a plus rien à compacter (1 fichier → 1). Une réexécution réécrit donc `reports/j3_gold.json` avec ce constat ; les valeurs de référence (8 → 1) sont celles de la première exécution, conservées dans le notebook.
