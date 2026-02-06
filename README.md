================================================================================
PROJET BIG DATA - ARCHITECTURE ETL & MACHINE LEARNING (NYC TAXI PIPELINE)
================================================================================

DESCRIPTION DU PROJET
---------------------
Ce projet met en oeuvre une architecture Big Data complete pour la collecte, le
stockage, le nettoyage, l'analyse et la prediction des prix des courses de taxis
new-yorkais.

L'architecture repose sur les composants suivants :
1. Orchestrateur : Apache Airflow
2. Traitement distribue : Apache Spark (Scala)
3. Data Lake : MinIO (Compatible S3)
4. Data Warehouse : PostgreSQL
5. Machine Learning : Python (Scikit-Learn)

FLUX DE DONNEES (PIPELINE)
--------------------------
1. Ingestion (Exercice 1) : Telechargement des donnees brutes (format Parquet)
   depuis la source officielle vers le Data Lake (MinIO - bucket 'nyc-raw').
2. Warehousing (Exercice 3) : Creation du schema en etoile/flocon dans
   PostgreSQL et insertion des tables de dimensions.
3. Traitement ETL (Exercice 2) : Nettoyage des donnees brutes via Spark,
   stockage des donnees propres dans MinIO ('nyc-processed') et ingestion des
   faits dans le Data Warehouse (PostgreSQL).
4. Machine Learning (Exercice 5) : Execution de tests unitaires et entrainement
   d'un modele de prediction de prix sur les donnees nettoyees.

--------------------------------------------------------------------------------
1. PREREQUIS TECHNIQUES
--------------------------------------------------------------------------------
- Docker et Docker Compose installes et fonctionnels.
- Un environnement de developpement Java/Scala (IntelliJ IDEA recommande).
- Git pour le versioning.
- Connexion Internet pour le telechargement des dependances (Images Docker,
  Librairies Spark/Python).

--------------------------------------------------------------------------------
2. INSTALLATION ET DEMARRAGE DE L'INFRASTRUCTURE
--------------------------------------------------------------------------------
Etape 1 : Cloner le depot du projet
git clone https://github.com/VOTRE_PSEUDO/NOM_DU_PROJET.git
cd NOM_DU_PROJET

Etape 2 : Lancer les conteneurs Docker
Cette commande demarre tous les services (Airflow, MinIO, Postgres, Spark).

docker compose up -d --build

Note : Attendez quelques minutes que tous les conteneurs soient marques
comme "Healthy".

--------------------------------------------------------------------------------
3. COMPILATION DES JOBS SPARK (ETAPE CRITIQUE)
--------------------------------------------------------------------------------
Avant de lancer le pipeline, il est imperatif de compiler les codes Scala et de
placer les fichiers .jar generes dans le dossier partage d'Airflow.

A. Compilation de l'Exercice 1 (Data Retrieval)
- Version Scala requise : 2.13
1. Ouvrez le dossier 'ex01_data_retrieval' dans votre IDE.
2. Executez les commandes sbt : clean, puis package.
3. Recuperez le fichier .jar genere dans 'target/scala-2.13/'.
4. Copiez ce fichier dans 'ex06_airflow/dags/files/' et renommez-le
   strictement en : exo1.jar

B. Compilation de l'Exercice 2 (Data Ingestion & ETL)
- Version Scala requise : 2.12 (Imperatif pour compatibilite Spark/Postgres)
1. Ouvrez le dossier 'ex02_data_ingestion' dans votre IDE.
2. Verifiez que le fichier build.sbt specifie bien scalaVersion := "2.12.18".
3. Executez les commandes sbt : clean, puis package.
4. Recuperez le fichier .jar genere dans 'target/scala-2.12/'.
5. Copiez ce fichier dans 'ex06_airflow/dags/files/' et renommez-le
   strictement en : exo2.jar

Verification :
Le dossier 'ex06_airflow/dags/files/' doit contenir :
- exo1.jar
- exo2.jar
- creation.sql
- insertion.sql
- Le dossier ex05_ml_prediction_service (contenant main.py et test_main.py)

--------------------------------------------------------------------------------
4. EXECUTION DU PIPELINE (VIA AIRFLOW - EXERCICE 6)
--------------------------------------------------------------------------------
C'est la methode recommandee pour executer l'ensemble du projet de maniere
sequentielle et automatisee.

1. Accedez a l'interface web Airflow : http://localhost:8080
    - Identifiant : airflow
    - Mot de passe : airflow

2. Localisez le DAG nomme : nyc_taxi_pipeline_final

3. Activez le DAG (bouton "Toggle" a gauche du nom) s'il est en pause.

4. Lancez le DAG :
    - Cliquez sur le bouton "Play" (triangle) a droite de la ligne.
    - Selectionnez "Trigger DAG".

5. Suivi de l'execution :
    - Cliquez sur le nom du DAG.
    - Allez dans l'onglet "Grid" ou "Graph".
    - Les taches doivent passer au vert succes dans l'ordre suivant :
      a. ex3_create_tables (Creation SQL)
      b. ex3_insert_reference_data (Insertion SQL)
      c. ex1_download_data (Spark Scala - Ingestion MinIO)
      d. ex2_spark_processing (Spark Scala - ETL & Postgres)
      e. ex5_run_tests (Python - Tests Unitaires)
      f. ex5_run_training (Python - Machine Learning)

--------------------------------------------------------------------------------
5. VERIFICATION MANUELLE ET ACCES AUX SERVICES
--------------------------------------------------------------------------------
Si vous souhaitez verifier les resultats ou debugger une etape specifique.

A. Acces au Data Lake (MinIO)
- URL : http://localhost:9001
- User : minio
- Pass : minio123
- Verification : Verifiez la presence des buckets 'nyc-raw' et
  'nyc-processed'.

B. Acces au Data Warehouse (PostgreSQL)
- Host : localhost
- Port : 5432
- User : admin
- Pass : password
- Database : taxi_warehouse

Commande pour compter les lignes inserees via Docker :
docker exec -it postgres-dw psql -U admin -d taxi_warehouse -c "SELECT count(*) FROM fact_trips;"

C. Execution manuelle du Machine Learning
Pour relancer uniquement l'entrainement sans tout le pipeline :

docker exec -it airflow-webserver bash
cd /opt/airflow/dags/files/ex05_ml_prediction_service
python main.py

--------------------------------------------------------------------------------
6. VISUALISATION DES DONNEES (DASHBOARD - EXERCICE 4)
--------------------------------------------------------------------------------
L'analyse visuelle des donnees a ete realisee en connectant un outil de BI
(Business Intelligence) au Data Warehouse PostgreSQL peuple par le pipeline.

Localisation des resultats :
Les captures d'ecran du tableau de bord (Dashboard) attestant de la realisation
de l'exercice 4 sont les suivants:

<img width="1855" height="919" alt="Capture d&#39;écran 2026-02-06 211557" src="https://github.com/user-attachments/assets/488658ae-2426-4d57-852a-60f63483dff1" />
<img width="1857" height="915" alt="Capture d&#39;écran 2026-02-06 211617" src="https://github.com/user-attachments/assets/6908b3dc-1363-4530-9a07-f465f147f5e6" />


Contenu du Dashboard :
Ce tableau de bord presente les indicateurs cles de performance (KPI) suivants :
1. Volume des courses : Evolution temporelle.
2. Revenus : Analyse du montant total et des pourboires.
3. Geographie : Zones de prise en charge et de depose les plus frequentes.

Pour reproduire ces visualisations, connectez votre outil (Tableau, PowerBI,
Metabase) a la base de donnees PostgreSQL en utilisant les identifiants fournis
dans la section 5.B.

--------------------------------------------------------------------------------
STRUCTURE DU PROJET
--------------------------------------------------------------------------------
/
|-- docker-compose.yaml          # Definition de l'infrastructure
|-- ex01_data_retrieval/         # Code source Scala (Exercice 1)
|-- ex02_data_ingestion/         # Code source Scala (Exercice 2)
|-- ex05_ml_prediction_service/  # Scripts Python ML (Exercice 5)
|   |-- main.py
|   |-- test_main.py
|-- ex06_airflow/                # Orchestration
|   |-- dags/
|       |-- pipeline_nyc.py      # Definition du DAG Airflow
|       |-- files/               # Dossier partage contenant les JARs et SQL
|           |-- creation.sql     # Script SQL (Exercice 3)
|           |-- insertion.sql    # Script SQL (Exercice 3)
|-- reports/
|   |-- figures/                 # Captures d'ecran du Dashboard (Exercice 4)
|-- README.txt                   # Ce fichier de documentation

================================================================================
AUTEURS : Saad BENNAI, Fares MOULOUDI, Ilyass SALHI, Ryad ZIOUCHE - CY Tech
================================================================================
