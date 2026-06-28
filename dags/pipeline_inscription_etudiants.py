"""
Module : dev.sekousow.kafkastreams.airflow.dags.pipeline_inscription_etudiants

Mini-projet — Pipeline automatisé de traitement des inscriptions étudiantes.
Simule un workflow administratif avec parallélisme entre l'affectation aux
groupes et la génération de statistiques.

Étapes : Réception → Stockage → Validation → Nettoyage →
         [Affectation, Statistiques] → Rapport final

Auteur      : [Votre Nom]
Encadrant   : Abdelmajid BOUSSELHAM
Université  : ENSET Mohammedia — II-BDDC
Date        : 2026
"""

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def reception_fichier():
    """Simule la réception du fichier d'inscription des étudiants."""
    print("Reception du fichier des etudiants")


def stockage_zone_brute():
    """Simule le dépôt du fichier dans une zone de stockage brut."""
    print("Stockage du fichier dans la zone brute")


def validation_fichier():
    """Simule la vérification de conformité du fichier reçu."""
    print("Validation du fichier des etudiants")


def nettoyage_donnees():
    """Simule le nettoyage et la standardisation des données."""
    print("Nettoyage des donnees")


def affectation_groupes():
    """Simule l'affectation algorithmique des étudiants aux groupes de TP."""
    print("Affectation des etudiants aux groupes")


def generation_statistiques():
    """Simule le calcul d'indicateurs statistiques sur les inscriptions."""
    print("Generation des statistiques")


def rapport_final():
    """Génère le rapport récapitulatif du traitement des inscriptions."""
    print("Generation du rapport final")


with DAG(
    dag_id="pipeline_inscription_etudiants",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["mini-projet", "python-operator"],
) as dag:

    reception = PythonOperator(
        task_id="reception_fichier",
        python_callable=reception_fichier,
    )

    stockage = PythonOperator(
        task_id="stockage_zone_brute",
        python_callable=stockage_zone_brute,
    )

    validation = PythonOperator(
        task_id="validation_fichier",
        python_callable=validation_fichier,
    )

    nettoyage = PythonOperator(
        task_id="nettoyage_donnees",
        python_callable=nettoyage_donnees,
    )

    affectation = PythonOperator(
        task_id="affectation_groupes",
        python_callable=affectation_groupes,
    )

    statistiques = PythonOperator(
        task_id="generation_statistiques",
        python_callable=generation_statistiques,
    )

    rapport = PythonOperator(
        task_id="rapport_final",
        python_callable=rapport_final,
    )

    reception >> stockage >> validation >> nettoyage
    nettoyage >> [affectation, statistiques]
    [affectation, statistiques] >> rapport
