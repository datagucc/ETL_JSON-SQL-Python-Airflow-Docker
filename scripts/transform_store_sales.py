#Import libraries
import sys
import os
import time
import json
import pandas as pd

sys.path.append("/opt/airflow/scripts/modules")  #on ajoute le dossier où est mon module

from postgres_utils import *


def get_big_table():
    query = 'select * from big_table'
    cur = connection_to_postgresql('postgreSQL_connection')['cur']
    big_table_df = my_query_fct(query, cur)
    return big_table_df

def transform_store_sales_table(df):

    #   STORE SALES SUMMARY
    store_sales_summary = df.groupby(['store_id', 'store_name', 'store_country', 'store_city', 'store_type','currency']
                                ).agg(total_sales_amount=('total_amount','sum'), total_quantity_sold=('total_quantity_sold','sum'), average_discount=('discount_applied','mean'), average_quantity_sold_per_product=('quantity','mean'), average_price=('price','mean'))
    store_sales_summary= store_sales_summary.reset_index()
    return store_sales_summary

if __name__ == "__main__": 
    my_df =get_big_table()
    final_df = transform_store_sales_table(my_df)
    create_postgres_table(final_df, 'store_sales_summary','postgreSQL_connection')
    print("store_sales_summary table created successfully.")