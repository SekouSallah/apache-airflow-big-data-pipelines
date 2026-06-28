"""
Module : dev.sekousow.kafkastreams.airflow.dags.pipeline_big_data_python

Pipeline Big Data complet en 7 étapes séquentielles :
  Ingestion → Stockage brut → Validation → Transformation →
  Traitement analytique → Chargement → Rapport

Chaque étape illustre une phase réelle d'un pipeline data engineering
(Data Lake, nettoyage, analyse, reporting).

Auteur      : [Votre Nom]
Encadrant   : Abdelmajid BOUSSELHAM
Université  : ENSET Mohammedia — II-BDDC
Date        : 2026
"""

import csv
import json
import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_DIR = "/opt/airflow/data"
RAW_FILE = f"{DATA_DIR}/ventes_raw.csv"
CLEAN_FILE = f"{DATA_DIR}/ventes_clean.csv"
RESULT_FILE = f"{DATA_DIR}/resultats_ventes.json"
REPORT_FILE = f"{DATA_DIR}/rapport_pipeline.txt"


def ingestion_donnees():
    """
    Simule l'ingestion de données depuis une source externe.
    Dans un environnement de production, la source pourrait être une API,
    une base de données transactionnelle, un topic Kafka ou un fichier
    déposé sur un bucket S3 / HDFS.
    """
    os.makedirs(DATA_DIR, exist_ok=True)

    ventes = [
        ["id_vente", "ville", "produit", "prix", "quantite"],
        [1, "Casablanca", "PC", 8000, 2],
        [2, "Rabat", "Clavier", 300, 5],
        [3, "Marrakech", "Souris", 150, 10],
        [4, "Casablanca", "Ecran", 2500, 3],
        [5, "Tanger", "PC", 8500, 1],
        [6, "Rabat", "Ecran", 2300, 2],
    ]

    with open(RAW_FILE, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(ventes)

    print(f"Ingestion terminee. Fichier cree : {RAW_FILE}")


def stockage_zone_brute():
    """
    Simule le stockage des données dans une zone brute (landing zone)
    d'un Data Lake. Vérifie l'intégrité du fichier avant de valider l'étape.
    """
    if not os.path.exists(RAW_FILE):
        raise FileNotFoundError("Le fichier brut n'existe pas.")

    taille = os.path.getsize(RAW_FILE)

    print("Stockage zone brute termine.")
    print(f"Fichier brut : {RAW_FILE}")
    print(f"Taille : {taille} octets")


def validation_donnees():
    """
    Contrôle qualité en amont du pipeline :
      - vérification de l'existence du fichier
      - vérification du schéma (nom des colonnes)
    """
    if not os.path.exists(RAW_FILE):
        raise FileNotFoundError("Le fichier de donnees est introuvable.")

    with open(RAW_FILE, mode="r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)

    colonnes_attendues = ["id_vente", "ville", "produit", "prix", "quantite"]

    if header != colonnes_attendues:
        raise ValueError("Schema incorrect")

    print("Validation terminee avec succes.")
    print(f"Colonnes detectees : {header}")


def transformation_donnees():
    """
    Nettoie les données et calcule le montant (prix × quantité).
    Produit un fichier nettoyé exploitable pour l'analyse.
    """
    lignes = []

    with open(RAW_FILE, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            prix = float(row["prix"])
            quantite = int(row["quantite"])
            montant = prix * quantite

            lignes.append({
                "id_vente": row["id_vente"],
                "ville": row["ville"],
                "produit": row["produit"],
                "prix": prix,
                "quantite": quantite,
                "montant": montant,
            })

    with open(CLEAN_FILE, mode="w", newline="", encoding="utf-8") as f:
        fieldnames = ["id_vente", "ville", "produit", "prix", "quantite", "montant"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(lignes)

    print("Transformation terminee.")


def traitement_analytique():
    """
    Agrège les ventes par ville pour produire un indicateur de performance :
    le chiffre d'affaires total par zone géographique.
    """
    ca = {}

    with open(CLEAN_FILE, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            ville = row["ville"]
            montant = float(row["montant"])
            ca[ville] = ca.get(ville, 0) + montant

    with open(RESULT_FILE, mode="w", encoding="utf-8") as f:
        json.dump(ca, f, indent=4, ensure_ascii=False)

    print("Traitement analytique termine.")
    print(ca)


def chargement_resultats():
    """
    Simule le chargement des résultats dans une base analytique
    (Data Warehouse, OLAP cube, ou base PostgreSQL).
    """
    if not os.path.exists(RESULT_FILE):
        raise FileNotFoundError("Resultats introuvables")

    print("Chargement termine.")


def generation_rapport():
    """
    Génère un rapport texte récapitulatif à partir des résultats
    de l'étape analytique. Ce rapport constitue le livrable final
    exploitable par les équipes métier.
    """
    with open(RESULT_FILE, mode="r", encoding="utf-8") as f:
        resultats = json.load(f)

    with open(REPORT_FILE, mode="w", encoding="utf-8") as f:
        f.write("Rapport Big Data\n")
        f.write("================\n\n")

        for ville, ca in resultats.items():
            f.write(f"{ville} : {ca} DH\n")

    print("Rapport genere.")


with DAG(
    dag_id="pipeline_big_data_python",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["big-data", "python-operator"],
) as dag:

    ingestion = PythonOperator(
        task_id="ingestion_donnees",
        python_callable=ingestion_donnees,
    )

    stockage = PythonOperator(
        task_id="stockage_zone_brute",
        python_callable=stockage_zone_brute,
    )

    validation = PythonOperator(
        task_id="validation_donnees",
        python_callable=validation_donnees,
    )

    transformation = PythonOperator(
        task_id="transformation_donnees",
        python_callable=transformation_donnees,
    )

    traitement = PythonOperator(
        task_id="traitement_analytique",
        python_callable=traitement_analytique,
    )

    chargement = PythonOperator(
        task_id="chargement_resultats",
        python_callable=chargement_resultats,
    )

    rapport = PythonOperator(
        task_id="generation_rapport",
        python_callable=generation_rapport,
    )

    ingestion >> stockage >> validation >> transformation >> traitement >> chargement >> rapport
