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
        CREATE OR REPLACE VIEW payment_method.best_payement_method_in_sales AS
        select
        payment_method
        ,sum(total_sales_amount) total_sales_per_payment
        from payment_method_analysis
        group by payment_method
        order by  total_sales_per_payment desc;

        """,
        """
        CREATE OR REPLACE VIEW payment_method.payment_method_with_return_rate AS
        select
        distinct payment_method
        from payment_method_analysis
        where return_rate > 0.1;   
        """,
        """
        CREATE OR REPLACE VIEW  payment_method.average_transac_per_payment_method AS
        select
        payment_method
        , average_items_per_transaction
        from payment_method_analysis
        order by average_items_per_transaction desc;
        """
        ,
        """
        CREATE OR REPLACE VIEW  payment_method.volume_transaction_per_payment_method AS
        select
        payment_method
        ,sum(total_sales_amount) total_sales_per_payment_method
        from payment_method_analysis
        group by payment_method
        order by total_sales_per_payment_method desc;
"""
    ]


    my_connect_dict = connection_to_postgresql('postgreSQL_connection')
    conn = my_connect_dict['conn']
    cur = my_connect_dict['cur']

    #on s'assure que le schéma existe
    cur.execute("CREATE SCHEMA IF NOT EXISTS payment_method;")
    
    for sql in views_sql:
        cur.execute(sql)
        print("✅ Vue créée.")

    conn.commit()


if __name__ == "__main__": 
    create_views()
