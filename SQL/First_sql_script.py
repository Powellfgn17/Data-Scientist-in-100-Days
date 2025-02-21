from google.cloud import bigquery

#set up the clent
client = bigquery.Client()
print("Client BigQuery initialisé !")

#set up the dataset reference
dataset_ref = client.dataset("hacker_news", project="bigquery-public-data")

#API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# list all the tables in the "hacker_news" dataset
tables = list(client.list_tables(dataset))

# print names of all tables in the dataset
for table in tables:
    print(table.table_id)

#set up the table reference
table_ref = dataset_ref.table("full")

#API request - fetch the table
table = client.get_table(table_ref)

# print the first 5 rows of the dataset
view = client.list_rows(table, max_results=6).to_dataframe()
print(view)

# set up the query
query = """
        SELECT EXTRACT (YEAR FROM timestamp) as year
        FROM `bigquery-public-data.hacker_news.full`
        GROUP BY year
        ORDER BY year
       """

# set up the query job
safe_job = bigquery.QueryJobConfig(maximum_bytes_billed = 10**10)
job_query = client.query(query, job_config = safe_job)

# API resquest - run the query and return a pandas DataFrame
query_result = job_query.to_dataframe()
print(query_result)