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
        CREATE OR REPLACE VIEW daily_sales.CA_total_CAD AS
        select
        sum(daily_total_sales) CA_in_CAD
        from daily_sales_country_currency
        where currency = 'CAD';
        """,
        """
        CREATE OR REPLACE VIEW daily_sales.nb_days_where_dailySales_higher_6000_inFrance AS
        select
        count(*)
        from daily_sales_country_currency
        where store_country = 'France' and daily_total_sales > 6000;        
        """,
        """
        CREATE OR REPLACE VIEW  daily_sales.mean_value_per_transac AS
        select
        store_country
        ,daily_total_sales/number_of_transactions as mean_value_per_transac
        from
        (
        select
        store_country
        ,daily_total_sales
        ,number_of_transactions
        from daily_sales_country_currency
        group by store_country, daily_total_sales,number_of_transactions
        ) oo
        order by mean_value_per_transac desc;
        """
        ,
        """
        CREATE OR REPLACE VIEW  daily_sales.total_sales_per_currency AS
        select 
            sum(daily_total_sales) total_sales_per_currency
            from daily_sales_country_currency
            group by currency
            order by total_sales_per_currency desc;
"""
    ]


    my_connect_dict = connection_to_postgresql('postgreSQL_connection')
    conn = my_connect_dict['conn']
    cur = my_connect_dict['cur']

    #on s'assure que le schéma existe
    cur.execute("CREATE SCHEMA IF NOT EXISTS daily_sales;")
    
    for sql in views_sql:
        cur.execute(sql)
        print("✅ Vue créée.")

    conn.commit()


if __name__ == "__main__": 
    create_views()
