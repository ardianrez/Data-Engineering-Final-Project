#!python3

import pandas as pd
from sqlalchemy import create_engine
from hdfs import InsecureClient
import connection

if __name__ == "__main__":

    engine = create_engine('postgresql://postgres:root@localhost:5432/dwh')

    conf_hadoop = connection.param_config("hadoop")["ip"]
    client = InsecureClient(conf_hadoop)

    with client.read(f'/DE Final Project/users.csv', encoding='utf-8') as reader:
        df_users = pd.read_csv(reader)
    
    df_users = df_users[["id","first_name","last_name","street_address","postal_code","state","city","country"]]
    df_users.to_sql("users", engine, if_exists='replace', index=False)
    
    #orders
    with client.read(f'/DE Final Project/orders.csv', encoding='utf-8') as reader:
        df_orders = pd.read_csv(reader)

    df_orders = df_orders[["order_id","status","shipped_at","delivered_at"]]
    df_orders['shipped_at'] = df_orders['shipped_at'].astype(str).str[0:10].astype('datetime64[ns]')
    df_orders['delivered_at'] = df_orders['delivered_at'].astype(str).str[0:10].astype('datetime64[ns]')
    df_orders['shipping_time'] = (df_orders['delivered_at'] - df_orders['shipped_at']).dt.days
    df_orders.to_sql("orders", engine, if_exists='replace', index=False)

    #order_items
    with client.read(f'/DE Final Project/order_items.csv', encoding='utf-8') as reader:
        df_order_items = pd.read_csv(reader)

    df_order_items = df_order_items[["id","order_id","user_id","product_id"]]
    df_order_items.to_sql("order_items", engine, if_exists='replace', index=False)

    #products
    with client.read(f'/DE Final Project/distribution_centers.csv', encoding='utf-8') as reader:
        df_dist = pd.read_csv(reader)

    df_dist = df_dist[["id","name"]]
    df_dist = df_dist.rename(columns={"id": "distribution_center_id", "name":"city_name"})

    with client.read(f'/DE Final Project/products.csv', encoding='utf-8') as reader:
        df_products = pd.read_csv(reader)
    df_products = df_products[["id","category","name","distribution_center_id"]]
    df_products = pd.merge(df_products, df_dist, how='left', on = 'distribution_center_id')
    df_products.to_sql("products", engine, if_exists='replace', index=False)





