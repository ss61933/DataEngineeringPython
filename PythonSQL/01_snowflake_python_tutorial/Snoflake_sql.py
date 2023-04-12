#//https://www.youtube.com/watch?v=KYdg9Bfi0X4&list=PLkb3p1b2TbNwMDT8ZJK6sT1qfKe1v2FU0&index=2
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

    sql = 'select * from dummy'
    cursor = conn.cursor(DictCursor)
    cursor.execute(sql)

    for c in cursor:
        print(c)

except Exception as e:
    print(e)
finally:
    conn.close()
