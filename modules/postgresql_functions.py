import psycopg2
import os
import pandas as pd
from io import StringIO
import configparser


my_current_loc = os.getcwd()
print(my_current_loc)
os.chdir('../')
print(os.getcwd())
os.chdir('Config/')
# Lire les identifiants depuis le fichier pipeline.conf
config = configparser.ConfigParser()
config.read('pipeline.conf')
db_config = config['postgresql']
os.chdir(my_current_loc)


def connection_to_postgresql():
    """
    Fonction qui se connecte à une base de données PostgreSQL et renvoie un curseur.
    """
    conn = psycopg2.connect(
        database=db_config['db'],
        user=db_config['user'],
        password=db_config['password'],
        host=db_config['host'],
        port=db_config['port']
    )
    cur = conn.cursor()
    return cur,conn
def close_connection_postgresql(cur,conn):
    """
    Fonction qui ferme la connexion à la base de données PostgreSQL.
    """
    cur.close()
    conn.close()
    print("Connection closed.")

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





#on en fait une fonction pour ne pas avoir à le faire à chaque fois
def create_postgres_table(df, table_name):

    # 1. Connexion à PostgreSQL
    """conn= psycopg2.connect(
	database =db_config['db']
	,user=db_config['user']
	,password=db_config['password']
	,host=db_config['host']
	,port=db_config['port']
	)"""
    cur=connection_to_postgresql()[0]
    conn =connection_to_postgresql()[1]
    #cur = conn.cursor()

    # 2. Suppression de la table si elle existe
    drop_query = f"DROP TABLE IF EXISTS {table_name};"
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
    #print(f"My table name:  {table_name}{create_query}/n")
    #on exécute la requete 
    cur.execute(create_query)

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
    print(f"✅ Table  {table_name} recréée et données insérées avec succès./n")

