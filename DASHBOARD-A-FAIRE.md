# Travail restant — dashboard Streamlit (Amine)

Le Jour 4 est volontairement limité aux tests statistiques et à la segmentation. `app/dashboard.py` reste à développer.

## Tables Gold disponibles après exécution de J3 et J4

| Table | Granularité | Usage |
|---|---|---|
| `airport_delay_pareto` | Un aéroport, quatre mois regroupés | Carte, Pareto, classement |
| `daily_delay_trend` | Un jour | Courbe quotidienne, évolution intra-mois |
| `airline_monthly_performance` | Une compagnie et un mois | Classement des compagnies et filtres mensuels |
| `hourly_delay_profile` | Une heure prévue et un mois | Profil horaire, comparaison des créneaux |
| `airport_segments` | Un aéroport, quatre mois regroupés | Groupes K-Means, profils et effectifs |

Tous les chemins commencent par `/lakehouse/gold/`. Le dashboard ne doit pas charger Silver ni les CSV.

## Livrable demandé par le professeur

- Au moins un filtre et trois à quatre graphiques Plotly complémentaires.
- Pour chaque visualisation clé : un `st.expander` avec la requête SQL d'origine et la justification de l'indicateur.
- Une gestion lisible des sélections vides et des tables absentes.

## Points à respecter

- **Portée des filtres :** le mois peut filtrer les trois tables qui possèdent `month`. La carte, le Pareto et les segments portent sur les quatre mois regroupés : ne pas faire croire qu'ils répondent au filtre mensuel.
- Un filtre compagnie n'est directement applicable qu'à `airline_monthly_performance`. Les autres tables ne conservent pas cette dimension.
- Pour regrouper des mois, recalculer le taux par `somme(delayed_15_flights) / somme(observed_flights)`. Ne pas moyenner les taux sans pondération. Même principe pour les annulations.
- Les médianes, IQR et P90 ne se réagrègent pas par addition ou moyenne. Les afficher seulement à leur granularité d'origine.
- Les colonnes `delay_rate_15` et `cancellation_rate` sont des proportions (0 à 1) ; `delay_rate_change_pts` est une variation déjà exprimée en points de pourcentage.
- Séparer les mois sur les courbes : l'extrait est discontinu entre janvier, avril, juillet et novembre.
- Trois aéroports (ECP, PBG, UST) n'ont pas de coordonnées. Les conserver dans les classements et signaler les points absents de la carte.
- Les segments décrivent des profils sur cet extrait historique, pas des niveaux de qualité universels. La classe « Données limitées » n'est pas un cluster entraîné.
- Présenter les limites : données 2015, quatre mois seulement, associations statistiques sans démonstration causale.

Après réalisation : vérifier l'affichage et les filtres sur `http://localhost:8501`, tester les sélections vides, puis compléter le rapport du dashboard. Les tests et la segmentation du Jour 4 sont documentés dans `Jour 4.md`.
