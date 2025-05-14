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

def transform_payment_method_table(df):

    big_table = df
    big_table['return_status'] = big_table['return_status'].replace({'Returned':1, 'Not Returned':0})
 

    # PAYEMENT METHOD ANALYSIS
    payment_method_analysis = big_table.groupby(['payment_method','currency']
                                            ).agg(total_transactions=('transaction_id','nunique'),total_sales_amount=('total_amount','sum'), average_discount =('discount_applied','mean'),  return_rate=('return_status','mean'), average_items_per_transaction=('total_quantity_sold','mean'))

    # DELETE INDEXES
    payment_method_analysis = payment_method_analysis.reset_index()
    return payment_method_analysis

if __name__ == "__main__": 
    my_df =get_big_table()
    final_df = transform_payment_method_table(my_df)
    create_postgres_table(final_df, 'payment_method_analysis','postgreSQL_connection')
    print("payment_method_analysis table created successfully.")