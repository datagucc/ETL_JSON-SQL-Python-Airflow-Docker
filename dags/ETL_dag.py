# Import necessary libraries and modules

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import datetime
from datetime import timedelta

# 0) On doit commencer par expliquer le docker-compose, l'utilisation des volumes, etc.. (pour ensuite réexpliquer ici qu'on 
# reprend les volumes qui ont été montés dans le docker-compose)

# A) We define the default arguments for the DAG.
default_args = {
    'owner': 'Augustin'                  # owner of the DAG : the person who created it
    ,'start_date': datetime(2025, 1, 1)  # define when the DAG should start running
    ,'email_on_failure': False            # to send an email on failure : we do not use it here
    ,'email_on_success': False            # to send an email on success : we do not use it here
    ,'email_on_retry': False              # to send an email on retry : we do not use it here
    ,'retries':2                         # number of retries in case of failure 
    ,'retry_delay': timedelta(minutes=3)  # delay between retries 
}

# B) We define the DAG itself.
ETL_dag = DAG(
    dag_id = 'ETL_analytics_sport_shop',                               # DAG ID
    default_args=default_args,                                         # Default arguments for the DAG (defined above)
    description='ETL process for the analytics of the sport shop',     # Description of the DAG
    tags=['ETL','PostgreSQL','Analytics','Docker','Flatfile','JSON'],  # Tags for the DAG
    catchup= False,                                                    # Do not backfill past runs when DAG is created 
                                                                       # Backfill means that Airflow will try to run the DAG for all past dates
    schedule='0 0 * * *'                                               # Schedule interval (run at midnight every day)
                                                                        # https://crontab.guru/ : very useful website to understand cron expressions
)



# C) We define the tasks in the DAG.

# 1) The start point of the DAG, represented by a DummyOperator
# expliquer à quoi il sert
start = DummyOperator(
    task_id='start',
    dag=ETL_dag
)


# A good practice of Airflow is to separate the tasks into different scripts, and use a BashOperator to call these scripts.
# Indeed, it is easier to maintain and debug the code this way. 
# In our case, we will divide our ETL process into several scripts, each one responsible for a specific task:
# 1) Extract and transform the JSON file into a big table in PostgreSQL
# 2) Verify if the big table exists in PostgreSQL (this is just a check task) --> à supprimer?
# 3) From the big table, we will create several tables in PostgreSQL : store_sales, daily_sales and payment_method. 
# We will have a separate script for each table. Each script will retrieve the data from the big table in postgreSQL,
# transform it and load it into the corresponding table in PostgreSQL.
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

# 3) Once the tables are created, we will do some analysis and measurements on the data.
    # We will store these analysis into views in postgreSQL, to be used in further analysis or in reporting tools like PowerBI.
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

# 4) The end point of the DAG, represented by a DummyOperator
# expliquer à quoi il sert 
stop = DummyOperator(
    task_id='stop',
    dag=ETL_dag
)


# D) We define the dependencies between the tasks in the DAG.
start >> extract_and_transform_JSON

extract_and_transform_JSON >> transform_store_sales
extract_and_transform_JSON >> transform_daily_sales
extract_and_transform_JSON >> transform_payment_method

transform_store_sales >> analysis_store_sales
transform_daily_sales >> analysis_daily_sales
transform_payment_method >> analysis_payment_method

[analysis_store_sales, analysis_daily_sales, analysis_payment_method] >> stop