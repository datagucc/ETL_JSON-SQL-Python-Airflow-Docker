from airflow.providers.postgres.hooks.postgres import PostgresHook
from io import StringIO
import pandas as pd
#on en fait une fonction pour ne pas avoir à le faire à chaque fois

def connection_to_postgresql(postgres_conn_id):
    """
    Fonction qui se connecte à une base de données PostgreSQL et renvoie un curseur.
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    cur = conn.cursor()
    print("📌 Base utilisée :", conn.get_dsn_parameters().get('dbname'))
    return {'cur':cur,'conn':conn}

def my_query_fct(query, cur):
    """
    Fonction qui exécute une requête SQL et renvoie le résultat sous forme de DataFrame pandas.
    """
    cur.execute(query)
    result = cur.fetchall()
    #cur.description renvoie une liste de tuples contenant des informations sur les colonnes dont la première valeur est le nom de la colonne
    columns = [desc[0] for desc in cur.description]
    #print(result)
    #print(cur.description)
    df = pd.DataFrame(result, columns=columns)
    return df


def create_postgres_table(df, table_name, postgres_conn_id):
    """
    Crée dynamiquement une table PostgreSQL à partir d'un DataFrame pandas,
    et insère les données via COPY.

    Args:
        df (pd.DataFrame): Données à insérer
        table_name (str): Nom de la table PostgreSQL cible
        postgres_conn_id (str): ID de la connexion Airflow (défaut: 'postgreSQL_connection')
    """
    # 1. Récupérer la connexion via le Hook
    my_connect_dict = connection_to_postgresql(postgres_conn_id)
    conn = my_connect_dict['conn']
    cur = my_connect_dict['cur']
    #hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    #conn = hook.get_conn()
    #cur = conn.cursor()
     #print("Connected to DB:", hook.get_uri())
    print("🧠 Connected to:", conn.get_dsn_parameters())


     # 2. Suppression de la table si elle existe
    drop_query = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
    cur.execute(drop_query)
    # 3. Creation de la table avec les types de données, récupérés dynamiquement
    # 3.1 Mapping pandas → PostgreSQL
    type_mapping = {
        'object': 'TEXT',
        'int64': 'INTEGER',
        'float64': 'DOUBLE PRECISION',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP'
    }

    # 3.2 Création de la requête de création de table
    columns_sql = []
    for col, dtype in df.dtypes.items():
        sql_type = type_mapping.get(str(dtype), 'TEXT')  # fallback = TEXT
        columns_sql.append(f"{col} {sql_type}")

    # on crée iune chaine de caracteres, qui correspond à la requete SQL
    create_query = f"CREATE TABLE {table_name} ({', '.join(columns_sql)});"   # CREATE TABLE ({COL1 TYPE1, COL2 TYPE2,...})
    cur.execute(create_query)
    #print(table_name,'   :   ', create_query)

    #print(f"My table name:  {table_name}{create_query}/n")
    #on exécute la requete 
 



    # 4. Insertion des données avec COPY
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    copy_query = f"COPY {table_name} ({', '.join(df.columns)}) FROM STDIN WITH CSV"
    cur.copy_expert(copy_query, buffer)

    #5. On cloture # ✅ Finaliser
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Table  {table_name} recréée et données insérées avec succès, via un  module./n")


