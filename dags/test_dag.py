from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging

# Fonction Python qui teste la connexion PostgreSQL
def test_postgres_connection():
    try:
        # Nom de la connexion définie dans Airflow UI (Admin > Connexions)
        hook = PostgresHook(postgres_conn_id="postgreSQL_connection")  
        
        # Test : on essaie de récupérer une connexion
        conn = hook.get_conn()
        
        # Test simple : exécuter une requête
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        result = cursor.fetchone()
        
        logging.info("Connexion réussie à PostgreSQL. Version : %s", result[0])
    
    except Exception as e:
        logging.error("Erreur lors de la connexion à PostgreSQL : %s", str(e))
        raise

# Définition du DAG
with DAG(
    dag_id="test_postgres_connection_dag",
    start_date=datetime(2023, 1, 1),
   # schedule_interval=None,  # Exécution manuelle uniquement
    catchup=False,
    tags=["test", "postgres", "connexion"],
) as dag:

    test_connection = PythonOperator(
        task_id="test_postgres_connection",
        python_callable=test_postgres_connection,
    )


