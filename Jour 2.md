# Rapport — Jour 2 (première moitié)

## Résultats et interprétation — première moitié du Jour 2

**Exercices 1.1 et 2.1 exécutés le 17 novembre 2026.**

- Bronze et Silver : **1 943 809 vols**. Aucun doublon strict métier, aucune exclusion selon les règles de validité appliquées, aucune erreur de conversion sur les colonnes numériques contrôlées.
- Clés de jointure renseignées ; référentiels uniques : **14 compagnies**, **322 aéroports**.
- Trois jointures gauches en **BroadcastHashJoin**, avec **100 % de rapprochement** pour chacune ; effectif strictement conservé. Un taux de rapprochement de 100 % ne garantit pas que tous les attributs enrichis sont renseignés.
- **24 756 retards de départ** et **30 794 retards d'arrivée** sont manquants dans Bronze. Ils restent manquants après typage : on ne les assimile pas à une arrivée à l'heure. Les causes de retard et d'annulation sont des champs conditionnels, à interpréter selon le statut du vol.
- Les coordonnées sont absentes pour **3 aéroports** du référentiel ; filtrer les coordonnées manquantes pour une carte et signaler cette couverture.
- **1 104 197 retards au départ négatifs** sont conservés : ils représentent des avances. Aucun vol annulé n'a d'indicateur `departure_delay_15` renseigné.
- **57 colonnes Silver**, dont date, saison, créneau du départ prévu, indicateur de retard de 15 minutes, minutes positives de retard et indicateur de trajet entre États (`interstate_route`). Ce dernier combine la table de faits et les deux référentiels d'aéroports.
- Évaluation paresseuse : **0 job** lors de la construction du DataFrame de démonstration, **6 jobs** observés après l'action. `select`/`filter` sont narrow ; le `groupBy` introduit un échange de données dans le plan.

Les règles de filtrage contrôlent date, numéro de vol, indicateurs 0/1, distance positive, codes et horaire prévu HHMM. Elles ne prouvent pas l'absence de toute anomalie métier. Les horaires locaux sont conservés sans calculer de durée entre fuseaux. La saison n'est représentée que par un mois de l'extrait : pas de conclusion annuelle.

**Point d'arrêt demandé : la section 3 ci-dessous reste à faire.** Elle comprend le journal Delta, le rejet d'un schéma incompatible, `MERGE` et le Time Travel. Pour rejouer uniquement la partie terminée :

```bash
docker exec spark-jupyter python /workspace/scripts/run_j2_first_half.py
```

Preuves : `reports/j2_first_half.json` et `reports/j2_join_plan.txt`. Le script d'exécution arrête sa session Spark après validation ; l'interface Spark UI n'est disponible que pendant une session active.


Aucun barème chiffré n’a été trouvé dans le README et les notebooks du dépôt. Les contrôles et explications ci-dessus répondent aux attendus écrits des exercices, sans préjuger de leur pondération dans la note.
