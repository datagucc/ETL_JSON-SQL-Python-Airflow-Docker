from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException

def check_big_table_exists():
    """
    Vérifie si la table 'big_table' existe dans la base PostgreSQL.
    Si elle n'existe pas, la tâche échoue.
    """
    hook = PostgresHook(postgres_conn_id="postgreSQL_connection")
    conn = hook.get_conn()
    cur = conn.cursor()
    print("📌 Base utilisée :", conn.get_dsn_parameters().get('dbname'))
    
    query = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_name = 'big_table'
    );
    """
    
    cur.execute(query)
    exists = cur.fetchone()[0]
    cur.close()
    conn.close()

    if exists:
        print("✅ La table 'big_table' existe bien dans la base PostgreSQL.")
    else:
        print("❌ La table 'big_table' n'existe pas.")
        raise AirflowFailException("Table 'big_table' non trouvée.")

if __name__ == "__main__": 
    check_big_table_exists()