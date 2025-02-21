from google.cloud import bigquery

#set up the client
client = bigquery.Client()
print("BigQuery client initialized !")

#set up the datasets reference
dataset_btc_ref = client.dataset("crypto_bitcoin", project = "bigquery-public-data")
dataset_eth_ref = client.dataset("crypto_ethereum", project = "bigquery-public-data")

# API request - fetch the datasets

dataset_btc = client.get_dataset(dataset_btc_ref)
dataset_eth = client.get_dataset(dataset_eth_ref)

# set up the tables reference

tables_btc_ref = list(client.list_tables(dataset_btc))
tables_eth_ref = list(client.list_tables(dataset_eth))

# print tables id

for tab_btc, tab_eth in zip(tables_btc_ref, tables_eth_ref):
    print("btc_id :",tab_btc.table_id, " eth_id :", tab_eth.table_id)