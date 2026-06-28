# Guide d'installation et de déploiement

## apache-airflow-big-data-pipelines

---

Ce document détaille la procédure d'installation et de déploiement de l'environnement Apache Airflow utilisé dans le cadre du TP6 — Atelier Apache Airflow.

---

## 1. Prérequis système

Avant de commencer, assurez-vous que les outils suivants sont installés sur votre machine :

| Outil | Version minimale | Vérification |
|---|---|---|
| Docker | 24.x | `docker --version` |
| Docker Compose | 2.20.x | `docker compose version` |
| Git | 2.x | `git --version` |

> **Note :** Apache Airflow 2.8.1 requiert **Docker Engine 24+** et **Docker Compose v2+**.

---

## 2. Récupération du projet

```bash
git clone <url-du-depot> apache-airflow-big-data-pipelines
cd apache-airflow-big-data-pipelines
```

---

## 3. Structure du projet

```
apache-airflow-big-data-pipelines/
│
├── dags/                   # Définitions des DAGs Airflow
│   ├── __init__.py
│   ├── mon_premier_dag.py
│   ├── pipeline_big_data_python.py
│   ├── pipeline_big_data_parallele.py
│   └── pipeline_inscription_etudiants.py
│
├── data/                   # Données manipulées par les DAGs
├── logs/                   # Logs d'exécution (générés automatiquement)
├── plugins/                # Plugins personnalisés (non utilisé)
├── images/                 # Captures d'écran du rendu
│
├── docker-compose.yml      # Configuration des services Airflow
├── INSTALLATION.md         # Présent document
└── README.md               # Livrables et rendu académique
```

---

## 4. Lancement de l'environnement

### 4.1 Démarrer les services

```bash
docker compose up -d
```

Cette commande lance trois conteneurs :

| Service | Rôle | Port |
|---|---|---|
| `postgres` | Base de données relationnelle (backend Airflow) | — |
| `airflow-webserver` | Interface Web Airflow | `8080` |
| `airflow-scheduler` | Ordonnanceur des DAGs | — |

### 4.2 Vérifier l'état des conteneurs

```bash
docker ps
```

Vous devez voir les trois conteneurs avec le statut `Up`.

### 4.3 Initialiser la base de données Airflow (premier lancement uniquement)

```bash
docker compose run airflow-webserver airflow db init
```

### 4.4 Créer le utilisateur admin (premier lancement uniquement)

```bash
docker compose run airflow-webserver airflow users create \
    --username airflow \
    --password airflow \
    --firstname Admin \
    --lastname Airflow \
    --role Admin \
    --email admin@example.com
```

> **Note :** Les versions récentes d'Airflow (dont la 2.8.1) peuvent créer un utilisateur par défaut. Si l'interface est accessible sans cette étape, ignorez-la.

---

## 5. Accès à l'interface Web

1. Ouvrir un navigateur à l'adresse : [http://localhost:8080](http://localhost:8080)
2. Identifiants : `airflow` / `airflow`

---

## 6. Commandes utiles

| Commande | Action |
|---|---|
| `docker compose up -d` | Lancer Airflow en arrière-plan |
| `docker compose down` | Arrêter tous les services |
| `docker compose down --volumes --remove-orphans` | Arrêter et supprimer les volumes |
| `docker compose logs airflow-scheduler` | Consulter les logs du scheduler |
| `docker compose logs airflow-webserver` | Consulter les logs du webserver |
| `docker compose restart airflow-webserver` | Redémarrer le webserver |
| `docker compose run airflow-webserver airflow dags list` | Lister les DAGs depuis le CLI |

---

## 7. Vérification du déploiement

Une fois l'environnement lancé :

1. **Vérifier les conteneurs** : `docker ps` doit afficher 3 conteneurs actifs
2. **Accéder à l'UI** : `http://localhost:8080` doit afficher la page de connexion Airflow
3. **Vérifier les DAGs** : La liste des DAGs doit contenir les 4 DAGs du projet :
   - `mon_premier_dag`
   - `pipeline_big_data_python`
   - `pipeline_big_data_parallele`
   - `pipeline_inscription_etudiants`

---

## 8. Dépannage

### Erreur : "Connexion refusée" sur localhost:8080
```bash
# Vérifier que les conteneurs sont bien démarrés
docker compose ps

# Consulter les logs du webserver
docker compose logs airflow-webserver
```

### Erreur : "Base de données non initialisée"
```bash
docker compose run airflow-webserver airflow db init
```

### Erreur : "Permission denied" sur les volumes
```bash
# Les dossiers dags/, data/, logs/, plugins/ doivent exister
mkdir -p dags data logs plugins
```

### Réinitialisation complète
```bash
docker compose down --volumes --remove-orphans
docker compose up -d
docker compose run airflow-webserver airflow db init
```

---

## 9. Architecture technique

```
┌─────────────────────────────────────────────────────────┐
│                     docker-compose.yml                    │
│                                                           │
│  ┌──────────────┐    ┌──────────────────┐                │
│  │   PostgreSQL  │◄───│  Airflow DB (SQL) │                │
│  │     :5432     │    │                  │                │
│  └──────────────┘    └──────────────────┘                │
│         ▲                        ▲                        │
│         │                        │                        │
│  ┌──────┴──────┐         ┌──────┴──────────┐             │
│  │   Scheduler  │         │   Webserver      │             │
│  │  (port —)    │         │   (port 8080)    │             │
│  └──────┬──────┘         └──────────────────┘             │
│         │                        │                        │
│         └──────────┬─────────────┘                        │
│                    ▼                                      │
│           ┌────────────────┐                              │
│           │    dags/       │  (monté en volume)           │
│           └────────────────┘                              │
└─────────────────────────────────────────────────────────┘
```

---

*Document d'installation — TP6 Apache Airflow — ENSET Mohammedia — II-BDDC — 2026*
