# Create a table create table json_table(col variant);
# Create a .json file with data {"name":"A","Age":30}
# Load a table - create a json file format and put file to stage
# then copy file to table
# also create other table for nested json json_nested_table with data
#{  "Age": 30,  "address": {    "city": "Pune",      "pincode": 411027    },    "name": "A"}

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

    print('------JSON_TABLE DATA-----------')

    sql = 'select col:name::string as name, \
            col:Age::integer as age \
            from {}'.format(config.json_table)
    cursor = conn.cursor(DictCursor)
    cursor.execute(sql)

    for c in cursor:
      print(c)
    cursor.close();

    print('------JSON_NESTED_TABLE DATA-----------')

    sql = 'select col:name::string as name, \
            col:Age::integer as age, \
            col:address:pincode::String as pcode, \
            col:address:city::string as city \
            from {}'.format(config.json_nested_table)

    cursor = conn.cursor(DictCursor)
    cursor.execute(sql)

    for c in cursor:
      print(c)
    cursor.close();





except Exception as e:
    print(e)
finally:
    conn.close()
