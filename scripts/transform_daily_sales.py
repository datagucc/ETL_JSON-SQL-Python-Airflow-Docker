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

def transform_daily_sales_table(df):

 #  DAILY SALES COUNTRY CURRENCY 
    big_table = df
    big_table['purchase_timestamp'] = pd.to_datetime(big_table['purchase_timestamp'], format='%Y-%m-%d %H:%M:%S')
    big_table['purchase_date']= big_table['purchase_timestamp'].dt.date
 

    daily_sales_country_currency = big_table.groupby(['purchase_date','store_country','currency']
                                                 ).agg(daily_total_sales=('total_amount','sum'), number_of_transactions=('transaction_id','nunique'), average_transaction_value=('total_amount','mean'), average_quantity_sold_per_product=('quantity','mean'), average_price=('price','mean'))
    daily_sales_country_currency = daily_sales_country_currency.reset_index()

    return daily_sales_country_currency

if __name__ == "__main__": 
    my_df =get_big_table()
    final_df = transform_daily_sales_table(my_df)
    create_postgres_table(final_df, 'daily_sales_country_currency','postgreSQL_connection')
    print("daily_sales_country_currency table created successfully.")