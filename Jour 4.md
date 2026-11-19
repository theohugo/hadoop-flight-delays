# Rapport — Jour 4 (tests statistiques et segmentation)

**Date : 19 novembre 2026.** Cette partie a été exécutée sans erreur. Le dashboard n'est pas réalisé, conformément au périmètre demandé.

## Méthode des tests

H0 du test de Mann–Whitney : mêmes distributions du retard entre matin (06–11 h) et soir (18–23 h). H1 bilatérale : distributions différentes. H0 de Kruskal–Wallis : distributions de rangs identiques entre les quatre mois ; une interprétation en termes de médianes exige des formes comparables.

Un tri par hash fixe d'une clé de vol fournit un sous-ensemble reproductible de 80 000 observations maximum, sans choisir selon le retard. Les vols annulés et les retards manquants sont exclus des tests. Les quatre colonnes utiles sont sélectionnées dans Spark avant le transfert au driver. Les retards négatifs sont conservés. Les mêmes vols échantillonnés servent aux deux analyses ; les effectifs mensuels sont enregistrés dans le JSON.

Mann–Whitney utilise l'approximation asymptotique avec correction des ex æquo. Nous appliquons Holm aux deux p-values pour un seuil familial de 5 %. Les vols ne sont pas indépendants au sens strict (dates, aéroports, retards partagés) : les p-values restent nominales sous cette hypothèse, et non une certitude sur une population annuelle. Le bootstrap des jours est une vérification complémentaire avec une unité plus agrégée, pas une suppression de tous les biais.

## Résultats — tests et segmentation

- Échantillon déterministe : **80,000 vols** parmi **1,917,902** vols non annulés dont le retard est renseigné.
- Mann–Whitney : matin **30,945**, soir **17,399**. Médianes échantillonnées **-3 min** et **0 min**, delta de Cliff soir/matin **0.221**. H0 rejetée nominalement après Holm ; la p-value calculée est zéro par sous-flux numérique, ce qui ne signifie pas une probabilité exactement nulle. Le test compare les distributions, pas seulement leurs médianes.
- Kruskal–Wallis : **H = 421.77**, **p corrigée = 4.25e-91**, **epsilon² = 0.0052**. L'effet mensuel de rang est faible malgré la très petite p-value. Le test global ne désigne pas les paires de mois différentes.
- Sensibilité par jours : écart moyen des moyennes journalières soir − matin **8.66 min**, intervalle bootstrap 95 % **[7.44 ; 9.91]**, sur **122 jours**. Chaque jour a ici le même poids ; ce n'est pas l'écart de moyennes pondéré par le nombre de vols. L'indépendance entre jours reste une hypothèse.
- K-Means : **K = 3**, silhouette **0.298**, **304 aéroports entraînés**, **18 hors entraînement faute de 100 vols observés. Les **322 aéroports** et **1 943 809 vols** restent représentés dans Gold.

La séparation des groupes est modérée. K=3 n'est que légèrement devant K=5 : les clusters sont une aide descriptive, pas une preuve de trois catégories naturelles. Les mots « élevé » et « fréquent » sont relatifs à la médiane des aéroports éligibles, et non à une norme de ponctualité. Les échantillons de vols partagent des dates et des perturbations ; les résultats ne prouvent pas que l'heure ou le mois cause le retard.

**Suite volontairement laissée à Amine : le dashboard Streamlit (section 3).** Voir `DASHBOARD-A-FAIRE.md`.

## Segmentation : méthode et profils

L'agrégation PySpark calcule quatre caractéristiques par aéroport d'origine : volume de vols, taux de retard ≥ 15 minutes, minutes positives de retard par vol observé (les avances contribuent zéro) et taux d'annulation. Le dénominateur des deux premières mesures de retard exclut les annulations et les retards manquants ; le taux d'annulation porte sur tous les vols.

Nous appliquons `log1p` au volume et aux minutes positives moyennes, puis `StandardScaler` sur les quatre variables. Aucun logarithme n'est appliqué directement aux retards signés. Les coordonnées ne servent pas à l'entraînement et leurs valeurs manquantes n'excluent donc pas un aéroport.

Les aéroports d'au moins 100 vols observés sont entraînés. Ce seuil pragmatique limite les taux fondés sur de très petits effectifs ; il n'a pas été optimisé statistiquement. Les autres sont conservés avec `cluster_id = -1` et une mention explicite. Comparaison de K=2 à K=6, 30 initialisations par K et graine 42. Nous retenons la meilleure silhouette parmi les partitions dont chaque cluster comporte au moins dix aéroports.

| K | Silhouette | Plus petit groupe |
|---:|---:|---:|
| 2 | 0.273 | 108 |
| 3 | 0.298 | 74 |
| 4 | 0.294 | 43 |
| 5 | 0.295 | 29 |
| 6 | 0.260 | 18 |

Les valeurs ci-dessous sont les **médianes entre aéroports** du groupe, pas des indicateurs pondérés par le trafic de tous les vols.

| Profil | Aéroports | Vols médians | Taux de retard médian | Taux d'annulation médian |
|---|---:|---:|---:|---:|
| Profil 1 — trafic modeste, retards fréquents | 80 | 348.5 | 15.27 % | 3.58 % |
| Profil 2 — trafic modeste, retards moins fréquents | 74 | 622.5 | 10.47 % | 1.01 % |
| Profil 3 — trafic élevé, retards fréquents | 150 | 3368 | 16.01 % | 1.33 % |

La table `/lakehouse/gold/airport_segments` comporte une ligne par aéroport, le profil attribué, les indicateurs et les coordonnées. Les effectifs de vols sont conservés pour permettre au dashboard de distinguer volume et taux. Des assertions vérifient l'unicité des 322 aéroports, les 1 943 809 vols et l'absence de segment nul.

## Livrables et partie restante

- [Notebook exécuté](notebooks/J4_Stats_Segmentation_Dashboard.ipynb)
- [Résultats numériques, paramètres et centres](reports/j4_first_half.json)
- [Fiche du dashboard à réaliser](DASHBOARD-A-FAIRE.md)

Le rapport final et les graphiques du dashboard seront à compléter après réalisation de l'application. Les tables Gold seules doivent alimenter celle-ci.

## Références méthodologiques

- [SciPy — Mann–Whitney](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.mannwhitneyu.html)
- [SciPy — Kruskal–Wallis](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.kruskal.html)
- [Scikit-learn — choix de K et silhouette](https://scikit-learn.org/stable/auto_examples/cluster/plot_kmeans_silhouette_analysis.html)
