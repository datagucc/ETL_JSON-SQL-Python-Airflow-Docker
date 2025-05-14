import sys
import json
import pandas as pd
from time import *
sys.path.append("/opt/airflow/scripts/modules")  #on ajoute le dossier où est mon module

from postgres_utils import *

def extracting_JSON():
        #Opening the JSON file
        my_source = '/opt/airflow/data/pos_sales_data_30k.json'
        with open(my_source,'r', encoding = 'utf-8') as f :
                d = json.load(f)

        #Storing the JSON file in a dataframe 
        df = pd.DataFrame(d)
        return df


def transform_DF(df):
    
        #Transformation of the big table 

            # first transfo
        my_df  = df.explode('products')

            #second trasnfo 
        my_df.loc[:,'product_id'] = my_df['products'].apply(lambda x:x['product_id'])
        my_df.loc[:,'product_name'] = my_df['products'].apply(lambda x:x['product_name'])
        my_df.loc[:,'products_category'] = my_df['products'].apply(lambda x:x['products_category'])
        my_df.loc[:,'quantity'] = my_df['products'].apply(lambda x:x['quantity'])
        my_df.loc[:,'price'] = my_df['products'].apply(lambda x:x['price'])
        my_df = my_df.drop(columns=['products'])

        #creation of big_table
        big_table = my_df.copy()

        #save in the postgresql DB :
        #create_postgres_table(big_table, 'big_table')
        #print("big_table table created successfully.")
        print(len(big_table))
        return big_table


if __name__ == "__main__": 
    my_df = extracting_JSON()
    #sleep(120)
    final_df = transform_DF(my_df)
    create_postgres_table(final_df, 'big_table','postgreSQL_connection')
    print("big table created successfully.")


