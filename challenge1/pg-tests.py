#######################################
### Challenge 3:
#######################################

import psycopg2 as pg

db_name = 'zubale'
hostname = '172.18.16.1'
user = 'postgres'
passw='mypass'
port = 5432

#### CREATING OBJECTS

connection = pg.connect(database = 'postgres', user=user, password=passw, host = hostname, port=port)
connection.set_isolation_level(pg.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cursor = connection.cursor()

# cursor.execute(f'create database {db_name};')

connection.commit()
connection.close()

connection = pg.connect(database = db_name, user=user, password=passw, host = hostname, port=port)
cursor = connection.cursor()

file_reader = open('pg-queries.sql','r')
sqlScript = file_reader.read()
file_reader.close()

scripts = sqlScript.split(';')

for script in scripts:
    if len(script) > 0:
        try:
            cursor.execute(script)
        except Exception as ex:
            print(f"Scripts failed: {str(ex)}")

connection.commit()
connection.close()

#### END OF CREATING OBJECTS

#### LOADING DATA
connection = pg.connect(database = db_name, user=user, password=passw, host = hostname, port=port)
cursor = connection.cursor()
cursor.execute("set schema 'tests'")

orders_file=open('orders.csv','r')
next(orders_file)
cursor.copy_from(file=orders_file, table='orders',sep=',',columns=('id','product_id','quantity','created_date'))

orders_file=open('products.csv','r')
next(orders_file)
cursor.copy_from(file=orders_file, table='products',sep=',',columns=('id','name','category','price'))

connection.commit()
connection.close()

#### END OF LOADING DATA