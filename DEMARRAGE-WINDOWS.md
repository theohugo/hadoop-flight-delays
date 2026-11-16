# Environnement du cours

Le dépôt hadoop-kaggle est installé dans ce dossier. Chocolatey et mise sont installés dans le profil utilisateur. Mise fournit task, uv et Python 3.15.

Dans PowerShell, depuis ce dossier :

```powershell
.\cours.ps1 up           # Démarrer (Docker Desktop doit être lancé)
.\cours.ps1 status       # État du cluster
.\cours.ps1 test:docker  # Vérifier les services
.\cours.ps1 down         # Arrêter en conservant les données
```

Le lanceur ajoute les utilitaires Git nécessaires aux commandes shell du Taskfile pour cette session. Les contrôles de disponibilité ont été adaptés aux fins de ligne Windows et à curl sous Windows.

- JupyterLab : http://localhost:8888/lab?token=bigdata
- HDFS : http://localhost:9870
- Streamlit : http://localhost:8501

Validation effectuée : 3 DataNodes actifs, interfaces JupyterLab et Streamlit accessibles, écriture et lecture Delta Lake sur HDFS réussies (données temporaires supprimées).

Dataset retenu : usdot/flight-delays (vols 2015). Voir PROJET-VOLS.md pour le périmètre et la progression du TP.

Sources : https://github.com/aptitek/hadoop-kaggle et https://chocolatey.org/install


