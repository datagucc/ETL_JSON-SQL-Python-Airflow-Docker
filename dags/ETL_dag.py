# 1) IMPORTATION DES MODULES

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import datetime
from datetime import timedelta


#on définit nos arguments par défaut.
default_args = {
    'owner': 'Augustin'
    ,'start_date': datetime(2025, 1, 1)  # The date when the DAG starts
    ,'retries':2
    ,'retry_delay': timedelta(minutes=3)
}

# Define the DAG
ETL_dag = DAG(
    dag_id = 'ETL_analytics_sport_shop',             # DAG ID
    default_args=default_args,     # Default arguments for the DAG
    description='Analytics ETL',
    tags=['Data Engineering courses',"Advanced"],
    catchup= False,              # Do not backfill past runs when DAG is created
    schedule='0 0 * * *'  # Schedule interval (run at midnight every day)
)





# The start point of the DAG, represented by a DummyOperator
# Cet opérateur Dummy permet de centraliser le point de départ.
start = DummyOperator(
    task_id='start',
    dag=ETL_dag
)

# The first task, extract and load data from JSON to Python, represented by a BashOperator
# A) On extrait les données de json. On le fait à partir de l'opérateur bash.
# Plutot que de lancer un code python, je vais plutot créer un code python à l'exterieur de ce DAG
# et ensuite mon opérateur bash va aller chercher ce script python et l'exécuter.
#on considère cette tâche comme critique donc on ajout un execution timeout:
extract_and_transform_JSON = BashOperator(
    task_id='extract_transform_big_table'
    ,bash_command='python /opt/airflow/scripts/extract_transform_big_table.py'
    #,execution_timeout = timedelta(minutes=1)
    ,dag=ETL_dag
)

# Ajout au DAG
check_table_task = BashOperator(
    task_id='check_big_table_exists'
    ,bash_command = 'python /opt/airflow/scripts/verify.py'
    ,dag=ETL_dag
)


transform_store_sales = BashOperator(
    task_id='transform_store_sales',
    bash_command='python /opt/airflow/scripts/transform_store_sales.py',
    dag=ETL_dag
)

transform_daily_sales = BashOperator(
    task_id='transform_daily_sales',
    bash_command='python /opt/airflow/scripts/transform_daily_sales.py',
    dag=ETL_dag
)

transform_payment_method = BashOperator(
    task_id='transform_payment_method',
    bash_command='python /opt/airflow/scripts/transform_payment_method.py',
    dag=ETL_dag
)

analysis_daily_sales = BashOperator(
    task_id='analysis_daily_sales',
    bash_command='python /opt/airflow/scripts/analysis_daily_sales.py',
    dag=ETL_dag
)

analysis_store_sales = BashOperator(
    task_id='analysis_store_sales',
    bash_command='python /opt/airflow/scripts/analysis_store_sales.py',
    dag=ETL_dag
)

analysis_payment_method = BashOperator(
    task_id='analysis_payment_method',
    bash_command='python /opt/airflow/scripts/analysis_payment_method.py',
    dag=ETL_dag
)

# The end point of the DAG, represented by a DummyOperator
# On crée une dernière tache qui nous permet de centraliser le stop. C'est pour centraliser le point d'arret.
stop = DummyOperator(
    task_id='stop',
    dag=ETL_dag
)

start >> extract_and_transform_JSON

extract_and_transform_JSON >> transform_store_sales
extract_and_transform_JSON >> transform_daily_sales
extract_and_transform_JSON >> transform_payment_method

transform_store_sales >> analysis_store_sales
transform_daily_sales >> analysis_daily_sales
transform_payment_method >> analysis_payment_method

[analysis_store_sales, analysis_daily_sales, analysis_payment_method] >> stop