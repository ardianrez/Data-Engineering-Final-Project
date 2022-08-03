#!python3

import os
import pandas as pd
import connection
from hdfs import InsecureClient
from datetime import datetime

if __name__ == "__main__":
    conf_hadoop = connection.param_config("hadoop")["ip"]

    client = InsecureClient(conf_hadoop)

    path = os.getcwd() + "\\" + "dataset" + "\\"

    list_tables = ['distribution_centers.csv', 'employees.csv', 'events.csv', 'inventory_items.csv', 'order_items.csv', 'orders.csv', 'products.csv', 'users.csv']
    for table in list_tables:
        df = pd.read_csv(path + table)
        with client.write(f'/DE Final Project/{table}', encoding='utf-8') as writer:
            df.to_csv(writer, index=False)




