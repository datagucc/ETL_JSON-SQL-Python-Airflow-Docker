#Import libraries
import sys
import os
import time
import json
import pandas as pd

sys.path.append("/opt/airflow/scripts/modules")  #on ajoute le dossier où est mon module

from postgres_utils import *


def create_views():

    views_sql = [
        """
        CREATE OR REPLACE VIEW store_sales.best_stores_in_total_sales AS
        SELECT *
            FROM public.store_sales_summary
            order by total_sales_amount desc
            limit 5;

        """
        , """
        CREATE OR REPLACE VIEW store_sales.best_city_in_total_sales AS
        select 
        store_city
        ,sum(total_sales_amount) as total_sales_per_city
        from store_sales_summary
        group by store_city
        order by total_sales_per_city desc
        limit 1;
       
        """,
        """
        CREATE OR REPLACE VIEW  store_sales.nb_store_per_country AS
            select
        store_country
        ,count(*) total_store
        from store_sales_summary
        group by store_country
        order by total_store desc;

        """
        ,
        """
        CREATE OR REPLACE VIEW  store_sales.total_nb_products_sold_per_country AS
    
        select 
        store_country
        ,sum(total_quantity_sold) as total_quantity_sold_per_country
        from store_sales_summary
        group by store_country
        order by total_quantity_sold_per_country desc;
"""
    ]


    my_connect_dict = connection_to_postgresql('postgreSQL_connection')
    conn = my_connect_dict['conn']
    cur = my_connect_dict['cur']

    #on s'assure que le schéma existe
    cur.execute("CREATE SCHEMA IF NOT EXISTS store_sales;")
    
    for sql in views_sql:
        cur.execute(sql)
        print("✅ Vue créée.")

    conn.commit()


if __name__ == "__main__": 
    create_views()
