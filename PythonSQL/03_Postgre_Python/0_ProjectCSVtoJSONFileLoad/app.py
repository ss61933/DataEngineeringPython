import sys
import glob
import os
import json
import re
import pandas as pd

# refer to readme.txt for details 

def get_column_names(schemas,ds_name,sorting_key='column_position'):
    column_Details=schemas[ds_name]
    columns=sorted(column_Details,key=lambda col:col[sorting_key])
    return [col['column_name'] for col in columns]

def read_csv(file, schemas):
    file_path_list = re.split('[/\\\]', file)
    ds_name = file_path_list[-2]
    file_name = file_path_list[-1]
    #Get the the column list for the file from schema 
    columns = get_column_names (schemas, ds_name)
    df = pd.read_csv(file, names=columns)
    return df

def to_json(df, tgt_base_dir, ds_name, file_name):
    json_file_path = f'{tgt_base_dir}/{ds_name}/{file_name}'
    os.makedirs(f'{tgt_base_dir}/{ds_name}', exist_ok=True)
    df.to_json(
    json_file_path,
    orient='records',
    lines=True
    )


def file_converter (src_base_dir, tgt_base_dir, ds_name):
    #Read the file/table column strcucture
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))

    #Read all the files in subdirectry with name format part-**
    files = glob.glob(f'{src_base_dir}/{ds_name}/part-**')

    if len(files)== 0:
        raise NameError(f'-----No file folder found for {ds_name}')

    for file in files:
        df = read_csv(file, schemas)
        #Get individual file name
        file_name = re.split('[/\\\]', file)[-1]
        to_json(df, tgt_base_dir, ds_name, file_name)


def process_files (ds_names=None):
    #Read the Source and Target dir as a environment variables
    src_base_dir=os.environ.get('SRC_BASE_DIR')
    tgt_base_dir=os.environ.get('TRG_BASE_DIR')
    #src_base_dir= 'F:/GitHub/PythonDataEngineering/PythonSQL/03_Postgre_Python/data-master/retail_db'
    #tgt_base_dir= 'F:/GitHub/PythonDataEngineering/PythonSQL/03_Postgre_Python/data-master/retail_db_json1'
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    if not ds_names: 
        ds_names = schemas.keys ()
    for ds_name in ds_names:
        try:
            print (f'Processing {ds_name}')
            file_converter(src_base_dir, tgt_base_dir, ds_name)
        except NameError as ne:
            print(ne)
            print(f'-----Error processing {ds_name}')
            #Add below line if you want to pass the excpetion and continue with next file
            pass

if __name__=='__main__':
    #Verify if file names are passed , if not prcess all the files 
    if len(sys.argv) == 2:
        ds_names=json.loads(sys.argv[1])
        process_files(ds_names)
    else:
        process_files()
