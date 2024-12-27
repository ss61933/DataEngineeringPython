import sys
import glob
import os
import json
import re
import pandas as pd
import numpy as np
from dotenv.main import load_dotenv
from sqlalchemy import VARCHAR, Integer

df_orders=pd.DataFrame()
df_order_items=pd.DataFrame()
df_departments=pd.DataFrame()
df_categories=pd.DataFrame()
df_products=pd.DataFrame()
df_customers=pd.DataFrame()


def get_column_names (schemas, ds_name, sorting_key='column_position'):
    column_details = schemas [ds_name]
    columns = sorted (column_details, key=lambda col: col [sorting_key])
    return [col['column_name'] for col in columns]

def read_csv(file, schemas):
    global df_orders
    global df_order_items
    global df_departments
    global df_categories
    global df_products
    global df_customers
    
    file_path_list = re.split('[/\\\]', file)
    ds_name = file_path_list[-2]
    columns= get_column_names (schemas, ds_name)
    
    if ds_name =='orders':
        df_orders=pd.read_csv(file, names=columns)
    elif ds_name == 'order_items':
           df_order_items=pd.read_csv(file, names=columns)
    elif ds_name == 'categories':
            df_categories=pd.read_csv(file, names=columns)
    elif ds_name == 'customers':
            df_customers=pd.read_csv(file, names=columns)
    elif ds_name == 'products':
            df_products=pd.read_csv(file, names=columns)
    elif ds_name=='departments':
            df_departments=pd.read_csv(file, names=columns)

def to_sql(df,df_conn_uri,ds_name,dtypes,index_label):
    df.to_sql(
    ds_name,
    df_conn_uri,
    if_exists='replace', # usually we use append
    index=False,
    chunksize=10000,
    dtype=dtypes,
    index_label=index_label)

def transform_def(db_conn_uri,df_categories,df_departments,df_products,df_orders,df_order_items) :       
     
    df_dept_cat =pd.merge(df_departments, df_categories, 
                           how='inner',
                           left_on='department_id', 
                           right_on='category_department_id',
                           left_index=False, 
                           right_index=False, 
                           sort=False)
    
    dept_filter=['Footwear']
    category_filter=['Cardio Equipment']
    df_dept_cat=df_dept_cat[['department_id','department_name',
                               'category_id','category_name']]\
                        .query('department_name in @dept_filter \
                               & category_name in @category_filter' )                           
    
   
    df_orderitems=pd.merge(df_orders, df_order_items, 
                           how='inner',
                           left_on='order_id', 
                           right_on='order_item_order_id',
                           left_index=False, 
                           right_index=False)
    
    df_orderitems=df_orderitems[['order_id','order_status','order_date','order_item_product_id',
                       'order_item_id','order_item_quantity','order_item_subtotal']]
   
    # Since we have used left outer join few order_item_id have null order_item_quantity
    #merged_df_orders.dropna(axis=0,inplace=True)
   
    df_orderitems['order_item_quantity']=df_orderitems['order_item_quantity'].astype(int)

    df_orders_pr=pd.merge(df_orderitems, df_products, 
                           how='inner',
                           left_on='order_item_product_id', 
                           right_on='product_id',
                           left_index=False, 
                           right_index=False).\
                           query('product_name.str.contains("Nike Women")')


    df_orders_pr=df_orders_pr.groupby(['order_id','order_status','order_date','order_item_product_id','product_category_id','product_name','product_price']).\
                    agg(
                         count_order_item_id=pd.NamedAgg(
                         column='order_item_id',aggfunc='count'),
                         sum_order_item_id=pd.NamedAgg(
                         column='order_item_quantity',aggfunc='sum'),
                         sum_order_item_subtotal=pd.NamedAgg(
                         column='order_item_subtotal',aggfunc='sum')
                        )
    
    df_report=pd.merge(df_dept_cat, df_orders_pr, 
                           how='inner',
                           left_on='category_id', 
                           right_on='product_category_id',
                           left_index=False, 
                           right_index=False)
    
    #write output to table_report
    dtypes = {'department_id': Integer(), 'department_name': VARCHAR(50),
               'category_id': Integer(),'category_name':VARCHAR(100),
              'count_order_item_id': Integer(),
              'sum_order_item_id': Integer() ,
              'sum_order_item_subtotal':VARCHAR(50)}
    
    #index_label='CHECK (department_id is not null)'

    to_sql(df_report, db_conn_uri, 'table_report',dtypes)

    q_table_report='select * from table_report'

    df_table_report=pd_query(q_table_report,db_conn_uri,h=5,t=5)

    return df_table_report
  

def db_loader(src_base_air, db_conn_uri, ds_name):
    schemas = json.load(open(f'{src_base_air}/schemas.json'))
    files = glob.glob(f'{src_base_air}/{ds_name}/part-**')
                      
    if len(files) == 0:
        raise NameError(f'No files found for {ds_name}')
    
    for file in files:
       #df_orders,df_order_items,df_categories,df_customers,df_products,df_departments=read_csv(file, schemas)
       read_csv(file, schemas)
        
def pd_query(c_query ,df_conn_uri):

    """
    Run a SQL query using pandas
   
    ----------
    query: SQL query
    h: no. of results from head - default=5
    t: no. of results from tail - default=5
    """
    df = pd.read_sql_query(c_query,df_conn_uri)
    
    return df
    print("df_report created Query executed--------")
    

    


#Default no file name is passed, process all files 
def process_files(ds_names=None):
    src_base_dir=os.environ.get('SRC_BASE_DIR')
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')
    db_name = os.environ.get('DB_NAME')
    db_user = os.environ.get('DB_USER')
    db_pass = os.environ.get('DB_PWD')
    
    db_conn_uri = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))

    if not ds_names: 
        ds_names = schemas.keys ()
   
    for ds_name in ds_names:
        try:
           print(f'----#processing file {ds_name}')
           db_loader(src_base_dir, db_conn_uri, ds_name)

   
        except NameError as ne:
            print(ne)
            print(f'-----Error processing {ds_name}')
            #Add below line if you want to pass the excpetion and continue with next file
            pass
        finally:
            print(f'---------Completed processing -----------')
    def_report=transform_def(db_conn_uri,df_categories,df_departments,df_products,df_orders,df_order_items)          
    print( def_report) 


if __name__=='__main__':
    load_dotenv()
    #Verify if file names are passed , if not prcess all the files 
    if len(sys.argv) == 2:
        ds_names=json.loads(sys.argv[1])
        process_files(ds_names)
    else:
        process_files()

"""
    To verify if data from all the source files are fetched 
    print(f'Shape of file df_departments is --{df_departments.shape}')
    print(f'Shape of file df_order_items is --{df_order_items.shape}')
    print(f'Shape of file df_orders is --{df_orders.shape}')
    print(f'Shape of file df_customers is --{df_customers.shape}')
    print(f'Shape of file df_products is --{df_products.shape}')
    print(f'Shape of file df_categories is --{df_categories.shape}')
"""







    
