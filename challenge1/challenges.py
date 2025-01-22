import pandas as pd
import requests

products = pd.read_csv('products.csv')
orders = pd.read_csv('orders.csv')

#######################################
### Challenge 1: CSV File Manipulation:
#######################################

products.rename(columns={'id':'product_id','name':'product_name'},inplace=True)
orders.rename(columns={'id':'order_id','created_date':'order_created_date'},inplace=True)

product_orders = orders.merge(products, left_on='product_id', right_on='product_id', suffixes=['_orders', '_products'])
product_orders['total_price'] = product_orders['quantity'] * product_orders['price']

summary = product_orders[['order_created_date', 'order_id', 'product_name','quantity','total_price']]

summary.to_csv('order_full_information.csv',index=False)

#######################################
### Challenge 2.1: Currency Conversion
#######################################

try:
    response = requests.get(url='https://api.freecurrencyapi.com/v1/latest?apikey=api_key_not_shared_here')
    exchange_rate = response.json()['data']['BRL']

    data = summary.copy()

    data.rename(columns = {'total_price':'total_price_br'},inplace = True)

    data['total_price_us'] = data['total_price_br'] / exchange_rate

    data.to_csv('fixed_order_full_information.csv', index=False)

except Exception as e:
    print(f'Error on the API Call: {str(e)}' )


#######################################
### Challenge 2.2: Data Exploration with Python
#######################################

#### First KPI:
# Grouping
summary_grouped_by_date = summary.groupby(by='order_created_date',as_index=False)
#Aggregating
highest_orders_day = summary_grouped_by_date.count().sort_values(by = 'order_id' , ascending=False)[['order_created_date']].head(1)
#Renaming cols
highest_orders_day.rename(columns={'order_created_date':'##date_with_most_orders','order_id':'orders_amount'},inplace=True)
#Exporting
highest_orders_day.to_csv('kpi_product_orders.csv',index=False)


#### Second KPI:
# Grouping
summary_grouped_by_product = summary.groupby(by='product_name',as_index=False)
#Aggregating
most_sold_products = summary_grouped_by_product.sum().sort_values(by = 'quantity' , ascending=False)[['product_name','total_price']].head(1)
#Renaming cols
most_sold_products.rename(columns={'product_name':'##most_sold_product','total_price':'sales_value'},inplace=True)
#Exporting
most_sold_products.to_csv('kpi_product_orders.csv',index=False,mode='a')


#### Third KPI:
# Grouping
summary_grouped_by_category = product_orders.groupby(by='category',as_index=False)
#Aggregating
most_sold_categorys = summary_grouped_by_category.sum().sort_values(by = 'quantity' , ascending=False)[['category','quantity']].head(3)
#Renaming cols
most_sold_categorys.rename(columns={'category':'##top_3_most_demanded_categories','quantity':'demanded_quantity'},inplace=True)
#Exporting
most_sold_categorys.to_csv('kpi_product_orders.csv',index=False,mode='a')


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