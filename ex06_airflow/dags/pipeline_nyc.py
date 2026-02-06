from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# --- CONFIGURATION ---
default_args = {
    'owner': 'cytech_student',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

FILES_PATH = "/opt/airflow/dags/files"
CLASS_NAME = "fr.cytech.integration.Main"
DATA_FILE = "yellow_tripdata_2024-01.parquet"

# --- INSTALLATION JAVA & SPARK (Version Scala 2.13) ---
INSTALL_ENV = """
    export INSTALL_DIR="/home/airflow/.local/bin_custom"
    mkdir -p $INSTALL_DIR
    
    # 1. JAVA
    if [ ! -d "$INSTALL_DIR/jdk-11" ]; then
        curl -L -o $INSTALL_DIR/java.tar.gz https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz
        tar -xzf $INSTALL_DIR/java.tar.gz -C $INSTALL_DIR
        mv $INSTALL_DIR/jdk-11.0.2 $INSTALL_DIR/jdk-11
        rm $INSTALL_DIR/java.tar.gz
    fi
    export JAVA_HOME="$INSTALL_DIR/jdk-11"
    export PATH="$JAVA_HOME/bin:$PATH"
    
    # 2. SPARK (VERSION SCALA 2.13)
    SPARK_DIR_NAME="spark-3.5.0-bin-hadoop3-scala2.13"
    
    if [ ! -d "$INSTALL_DIR/$SPARK_DIR_NAME" ]; then
        echo "Téléchargement de Spark (Scala 2.13)..."
        curl -L -o $INSTALL_DIR/spark.tgz https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3-scala2.13.tgz
        tar -xzf $INSTALL_DIR/spark.tgz -C $INSTALL_DIR
        rm $INSTALL_DIR/spark.tgz
    fi
    
    export SPARK_HOME="$INSTALL_DIR/$SPARK_DIR_NAME"
    export PATH="$SPARK_HOME/bin:$PATH"

    # 3. VERIF BUCKET MINIO
    pip install minio > /dev/null 2>&1
    python -c "
from minio import Minio
try:
    client = Minio('minio:9000', access_key='minio', secret_key='minio123', secure=False)
    if not client.bucket_exists('nyc-raw'): client.make_bucket('nyc-raw')
    if not client.bucket_exists('nyc-processed'): client.make_bucket('nyc-processed')
    print('Buckets OK.')
except: pass
"
"""

# Options Spark
SPARK_OPTS = "--packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0 --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.access.key=minio --conf spark.hadoop.fs.s3a.secret.key=minio123 --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2"

with DAG(
        dag_id='nyc_taxi_pipeline_final',
        default_args=default_args,
        schedule=None,
        catchup=False,
        template_searchpath=[FILES_PATH],
        description='Pipeline FINAL (Tasks Separated)'
) as dag:

    # 1. SQL Tables
    task_ex3_creation = SQLExecuteQueryOperator(
        task_id='ex3_create_tables',
        conn_id='postgres_dw',
        sql='creation.sql'
    )

    # 2. SQL Insert
    task_ex3_insertion = SQLExecuteQueryOperator(
        task_id='ex3_insert_reference_data',
        conn_id='postgres_dw',
        sql='insertion.sql'
    )

    # 3. EXO 1 (SCALA)
    task_ex1_download = BashOperator(
        task_id='ex1_download_data',
        bash_command=f"""
        {INSTALL_ENV}
        mkdir -p data/raw
        if [ -f {FILES_PATH}/{DATA_FILE} ]; then
            cp {FILES_PATH}/{DATA_FILE} data/raw/{DATA_FILE}
        else
            echo "ERREUR: Fichier manquant !"
            exit 1
        fi
        
        echo "Lancement Exo 1..."
        spark-submit {SPARK_OPTS} --class {CLASS_NAME} --master local[*] {FILES_PATH}/exo1.jar
        """
    )

    # 4. EXO 2 (SCALA)
    task_ex2_spark = BashOperator(
        task_id='ex2_spark_processing',
        bash_command=f"""
        {INSTALL_ENV}
        
        mkdir -p data/raw
        if [ -f {FILES_PATH}/{DATA_FILE} ]; then
            cp -n {FILES_PATH}/{DATA_FILE} data/raw/{DATA_FILE}
        fi
        
        echo "Lancement Exo 2..."
        spark-submit {SPARK_OPTS} --class {CLASS_NAME} --master local[*] {FILES_PATH}/exo2.jar
        """
    )

    # 5.1 EXO 5 - PARTIE 1 : TESTS UNITAIRES
    task_ex5_tests = BashOperator(
        task_id='ex5_run_tests',
        bash_command=f"""
        # Installation des dépendances (y compris pytest)
        pip install pandas scikit-learn pyarrow s3fs psycopg2-binary pytest
        
        cd {FILES_PATH}/ex05_ml_prediction_service
        
        echo "--- Lancement des Tests Unitaires ---"
        python -m pytest test_main.py
        """
    )

    # 5.2 EXO 5 - PARTIE 2 : ENTRAINEMENT MAIN
    task_ex5_main = BashOperator(
        task_id='ex5_run_training',
        bash_command=f"""
        # On s'assure que les libs sont là (chaque tâche est isolée)
        pip install pandas scikit-learn pyarrow s3fs psycopg2-binary
        
        cd {FILES_PATH}/ex05_ml_prediction_service
        
        echo "--- Lancement du Service ML (Entraînement) ---"
        python main.py
        """
    )

    # --- ORCHESTRATION ---
    # SQL -> Exo1 -> Exo2 -> Tests -> Main
    task_ex3_creation >> task_ex3_insertion >> task_ex1_download >> task_ex2_spark >> task_ex5_tests >> task_ex5_main