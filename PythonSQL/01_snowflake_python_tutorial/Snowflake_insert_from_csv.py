# Insert into stage and table from local file system

import snowflake.connector as sf
from config import config
from snowflake.connector import DictCursor
conn = sf.connect(user=config.username, password=config.password, account=config.account)


def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()


try:
    sql = 'use {}'.format(config.database)
    execute_query(conn, sql)
    sql = 'use warehouse {}'.format(config.warehouse)

    try:
        sql = 'alter warehouse {} resume'.format(config.warehouse)
        execute_query(conn, sql)
    except:
        pass


#// insert data to stage and then to table
    csv_file = 'C:\\Users\\swapnil\\PycharmProjects\\SnowFlakePythonSQL\\CSV_FOLDER\\snowflake_csv_load_1.csv'

    sql = 'put file://{0} @{1} auto_compress=true'.format(csv_file, config.stage)

    execute_query(conn, sql)

    #After this go to snowflake and check the stage list @STAGE_DB
    #Now load the data from stage to table
    #Note that beloyw copy is succesful it will not load the file again hence run with other file
    # snowflake_csv_load.csv snowflake_csv_load1.csv
    #monitor stage loading history as
    #select * from demo_db.information_Schema.load_history order by last_load_time desc;
    sql = "copy into {0} from @{1}/snowflake_csv_load_1.csv.gz FILE_FORMAT=(TYPE=csv field_delimiter=',' skip_header=0) " \
           " ON_ERROR = 'ABORT_STATEMENT' ".format(config.table,config.stage)

    execute_query(conn, sql)

    sql = 'select * from dummy'
    cursor = conn.cursor(DictCursor)
    cursor.execute(sql)

    for c in cursor:
      print(c)
    cursor.close();

except Exception as e:
    print(e)
finally:
    conn.close()
