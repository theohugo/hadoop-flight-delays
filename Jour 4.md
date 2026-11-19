# Rapport — Jour 4 (tests statistiques, segmentation et dashboard)

**Date : 19 novembre 2026.** Tests et segmentation : TheoHugo ; dashboard Streamlit : Amine. Le notebook a été exécuté sans erreur et le dashboard a été vérifié sur `http://localhost:8501` (section « Dashboard Streamlit »).

## Méthode des tests

H0 du test de Mann–Whitney : mêmes distributions du retard entre matin (06–11 h) et soir (18–23 h). H1 bilatérale : distributions différentes. H0 de Kruskal–Wallis : distributions de rangs identiques entre les quatre mois ; une interprétation en termes de médianes exige des formes comparables.

Un tri par hash fixe d'une clé de vol fournit un sous-ensemble reproductible de 80 000 observations maximum, sans choisir selon le retard. Les vols annulés et les retards manquants sont exclus des tests. Les quatre colonnes utiles sont sélectionnées dans Spark avant le transfert au driver. Les retards négatifs sont conservés. Les mêmes vols échantillonnés servent aux deux analyses ; les effectifs mensuels sont enregistrés dans le JSON.

Mann–Whitney utilise l'approximation asymptotique avec correction des ex æquo. Nous appliquons Holm aux deux p-values pour un seuil familial de 5 %. Les vols ne sont pas indépendants au sens strict (dates, aéroports, retards partagés) : les p-values restent nominales sous cette hypothèse, et non une certitude sur une population annuelle. Le bootstrap des jours est une vérification complémentaire avec une unité plus agrégée, pas une suppression de tous les biais.

## Résultats — tests et segmentation

- Échantillon déterministe : **80 000 vols** parmi **1 917 902** vols non annulés dont le retard est renseigné.
- Mann–Whitney : matin **30 945**, soir **17 399**. Médianes échantillonnées **−3 min** et **0 min**, delta de Cliff soir/matin **0,221**. H0 rejetée nominalement après Holm ; la p-value calculée est zéro par sous-flux numérique, ce qui ne signifie pas une probabilité exactement nulle. Le test compare les distributions, pas seulement leurs médianes.
- Kruskal–Wallis : **H = 421,77**, **p corrigée = 4,25 × 10⁻⁹¹**, **epsilon² = 0,0052**. L'effet mensuel de rang est faible malgré la très petite p-value. Le test global ne désigne pas les paires de mois différentes.
- Sensibilité par jours : écart moyen des moyennes journalières soir − matin **8,66 min**, intervalle bootstrap 95 % **[7,44 ; 9,91]**, sur **122 jours**. Chaque jour a ici le même poids ; ce n'est pas l'écart de moyennes pondéré par le nombre de vols. L'indépendance entre jours reste une hypothèse.
- K-Means : **K = 3**, silhouette **0,298**, **304 aéroports entraînés**, **18 hors entraînement** faute de 100 vols observés. Les **322 aéroports** et **1 943 809 vols** restent représentés dans Gold.

La séparation des groupes est modérée. K=3 n'est que légèrement devant K=5 : les clusters sont une aide descriptive, pas une preuve de trois catégories naturelles. Les mots « élevé » et « fréquent » sont relatifs à la médiane des aéroports éligibles, et non à une norme de ponctualité. Les échantillons de vols partagent des dates et des perturbations ; les résultats ne prouvent pas que l'heure ou le mois cause le retard.

Le dashboard Streamlit qui restitue ces résultats est décrit à la section « Dashboard Streamlit ».

## Segmentation : méthode et profils

L'agrégation PySpark calcule quatre caractéristiques par aéroport d'origine : volume de vols, taux de retard ≥ 15 minutes, minutes positives de retard par vol observé (les avances contribuent zéro) et taux d'annulation. Le dénominateur des deux premières mesures de retard exclut les annulations et les retards manquants ; le taux d'annulation porte sur tous les vols.

Nous appliquons `log1p` au volume et aux minutes positives moyennes, puis `StandardScaler` sur les quatre variables. Aucun logarithme n'est appliqué directement aux retards signés. Les coordonnées ne servent pas à l'entraînement et leurs valeurs manquantes n'excluent donc pas un aéroport.

Les aéroports d'au moins 100 vols observés sont entraînés. Ce seuil pragmatique limite les taux fondés sur de très petits effectifs ; il n'a pas été optimisé statistiquement. Les autres sont conservés avec `cluster_id = -1` et une mention explicite. Comparaison de K=2 à K=6, 30 initialisations par K et graine 42. Nous retenons la meilleure silhouette parmi les partitions dont chaque cluster comporte au moins dix aéroports.

| K | Silhouette | Plus petit groupe |
|---:|---:|---:|
| 2 | 0,273 | 108 |
| 3 | 0,298 | 74 |
| 4 | 0,294 | 43 |
| 5 | 0,295 | 29 |
| 6 | 0,260 | 18 |

Les valeurs ci-dessous sont les **médianes entre aéroports** du groupe, pas des indicateurs pondérés par le trafic de tous les vols.

| Profil | Aéroports | Vols médians | Taux de retard médian | Taux d'annulation médian |
|---|---:|---:|---:|---:|
| Profil 1 — trafic modeste, retards fréquents | 80 | 348,5 | 15,27 % | 3,58 % |
| Profil 2 — trafic modeste, retards moins fréquents | 74 | 622,5 | 10,47 % | 1,01 % |
| Profil 3 — trafic élevé, retards fréquents | 150 | 3 368 | 16,01 % | 1,33 % |

La table `/lakehouse/gold/airport_segments` comporte une ligne par aéroport, le profil attribué, les indicateurs et les coordonnées. Les effectifs de vols sont conservés pour permettre au dashboard de distinguer volume et taux. Des assertions vérifient l'unicité des 322 aéroports, les 1 943 809 vols et l'absence de segment nul.

## Dashboard Streamlit

Application : [`app/dashboard.py`](app/dashboard.py), servie sur `http://localhost:8501` par le conteneur `streamlit`.

### Architecture

Le dashboard lit **uniquement les cinq tables Delta de `/lakehouse/gold/`** : `airport_delay_pareto`, `daily_delay_trend`, `airline_monthly_performance`, `hourly_delay_profile` et `airport_segments` (322 lignes au plus chacune). Ni Silver ni les CSV ne sont chargés : les agrégations lourdes restent dans les notebooks J3 et J4, l'application ne fait que filtrer et réagréger des compteurs. Les tables sont mises en cache 10 minutes (`st.cache_data`). Si une table manque, l'application l'indique et rappelle d'exécuter J3 puis J4.

La session Spark de l'application tourne en `local[2]`, avec une seule partition pour la reconstruction de l'état Delta et les shuffles : l'état Delta était sinon reconstruit sur 50 partitions pour quelques centaines de lignes. Le premier affichage prend environ une minute (démarrage de la JVM et chargement de Delta), puis chaque table se lit en 2 à 3 secondes et le cache évite de la relire.

### Filtres et indicateurs clés

Trois filtres sur une ligne au-dessus des onglets ; leur portée est indiquée dans l'interface :

| Filtre | Agit sur | N'agit pas sur |
|---|---|---|
| Mois (sélection multiple) | indicateurs clés, compagnies, évolution quotidienne, profil horaire | carte, Pareto, segments (quatre mois regroupés) |
| Compagnies | indicateurs clés, mise en évidence dans le classement, détail mensuel | autres vues : la dimension compagnie n'existe que dans `airline_monthly_performance` |
| Aéroport | mise en évidence sur la carte, le nuage des segments et le Pareto ; fiche de l'aéroport | tables mensuelles, sans dimension aéroport |

Quatre indicateurs clés : vols, taux de retard ≥ 15 min, taux d'annulation et vols observés. Quand plusieurs mois ou compagnies sont combinés, les taux sont **recalculés par somme des numérateurs / somme des dénominateurs**, jamais par moyenne de taux. Médianes, IQR et P90 ne sont affichés qu'à leur granularité d'origine. Sans filtre, on retrouve les totaux de Silver : 1 943 809 vols, 1 917 902 observés, 18,2 % de retards et 1,3 % d'annulations ; juillet seul donne 21,3 %, comme au Jour 3.

### Visualisations

Chaque visualisation est suivie d'un `st.expander("Requête SQL & justification statistique")` qui reprend la requête Spark SQL du notebook ayant produit la table et justifie l'indicateur.

1. **Carte des aéroports** (`airport_delay_pareto` + `airport_segments`) : un point par aéroport, de taille proportionnelle au nombre de vols. La couleur montre soit le profil K-Means, soit le taux de retard en dégradé d'une seule teinte. ECP, PBG et UST, sans coordonnées, sont listés sous la carte. La sélection d'un aéroport l'entoure et affiche sa fiche : vols, taux, médiane/P90, annulations, rang en minutes cumulées, classe Pareto et profil. Exemple : MDW, 29 467 vols, 24,3 %, 16e en minutes, classe A.
2. **Classement des compagnies** (`airline_monthly_performance`) : barres horizontales triées par taux pondéré, ligne de référence de l'ensemble et effectifs au survol. Un tableau détaille chaque compagnie par mois, avec rang, médiane, IQR et P90.
3. **Évolution des retards** (`daily_delay_trend`, `hourly_delay_profile`) : un panneau par mois, car l'extrait est discontinu, avec le taux du jour et la moyenne glissante sur 7 jours. Le survol donne la variation en points par rapport à la veille. Le profil horaire trace une courbe par mois ; les heures de moins de 30 vols observés sont masquées. Une légende rappelle les taux par créneau (matin 11,5 %, soir 26,4 %) et le test de Mann–Whitney.
4. **Profils d'aéroports** (`airport_segments`, `airport_delay_pareto`) : nuage volume (échelle log) × taux coloré par segment, courbe de Pareto (classe A : 45 aéroports, 80,3 % des minutes) et tableau des médianes par segment, identiques au tableau des profils ci-dessus.

Un dernier onglet, « Méthode et limites », rappelle les dénominateurs, la portée des filtres et les limites de l'étude.

### Choix de présentation

- Les couleurs servent à identifier : le mois ou le segment garde la même couleur quel que soit le filtre. Le gris est réservé à « Données limitées », qui n'est pas un cluster entraîné. Une seule teinte, du clair au foncé, sert pour le taux de retard.
- Les nombres sont affichés au format français. Une sélection vide (aucun mois) affiche un avertissement au lieu de graphiques vides.

### Vérification

- **Test automatique** avec `streamlit.testing.v1.AppTest`, sur huit scénarios : affichage par défaut, juillet seul, aucun mois, compagnie UA, aéroports ATL et ECP (sans coordonnées), carte colorée par taux, avril + novembre avec MDW. Aucun scénario ne lève d'exception, et les indicateurs clés correspondent aux totaux Gold et Silver.
- **Contrôle visuel** sur `http://localhost:8501` dans Chrome : les quatre onglets, la fiche MDW et les requêtes SQL dépliées. Les défauts observés ont été corrigés : carte trop petite, zoom de la carte au défilement de la page, légendes qui chevauchaient les titres, nombres au format anglais.
- Avant ces tests, J2, J3 puis J4 ont été réexécutés sur ce poste pour disposer des tables Gold corrigées lors de la revue globale, et `scripts/validate_project.py` termine avec `success: true`.

### Limites

Les vues décrivent quatre mois de 2015 et montrent des associations, pas des causes. Les segments sont des profils relatifs à cet extrait. Le filtre compagnie ne peut pas s'appliquer à la carte ni aux courbes, car ces tables Gold ne conservent pas cette dimension. Il faudrait une table Gold supplémentaire (compagnie × aéroport ou compagnie × jour) pour l'étendre.

## Livrables

- [Notebook exécuté](notebooks/J4_Stats_Segmentation_Dashboard.ipynb)
- [Résultats numériques, paramètres et centres](reports/j4_first_half.json)
- [Dashboard Streamlit](app/dashboard.py) et son [cahier des charges](DASHBOARD-A-FAIRE.md)

Le dashboard n'est alimenté que par les tables Gold.

## Références méthodologiques

- [SciPy — Mann–Whitney](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.mannwhitneyu.html)
- [SciPy — Kruskal–Wallis](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.kruskal.html)
- [Scikit-learn — choix de K et silhouette](https://scikit-learn.org/stable/auto_examples/cluster/plot_kmeans_silhouette_analysis.html)
