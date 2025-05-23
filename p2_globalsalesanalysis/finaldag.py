from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.providers.google.cloud.hooks.gcs import GCSHook  # type: ignore
from airflow.providers.mysql.hooks.mysql import MySqlHook  # type: ignore
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook  # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore
from airflow.utils.dates import days_ago  # type: ignore
from datetime import timedelta
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
import numpy as np

# Currency conversion rates
CURRENCY_RATES = {
    "India": 1,         # INR
    "Japan": 0.57,      # JPY
    "Norway": 8.21,     # NOK
    "SriLanka": 0.25,   # LKR
    "HongKong": 10.93,  # HKD
    "Oman": 215.54,     # OMR
    "Germany": 89.34,   # EUR
    "Qatar": 22.87      # QAR
}

# GCS buckets
GCS_BUCKET_MAIN = "remainingalldata"
GCS_BUCKET_SRILANKA = "srilankadata"

# Database connection mapping
DB_CONNECTIONS = {
    "Oman": ("mysql_Oman_db", "Oman_Sales"),
    "Norway": ("postgres_db", "Norway_Sales"),
    "India": ("mssql_India_db", "India_Sales"),
    "Germany": ("mysql_Germany_db", "Germany_Sales"),
    "Qatar": ("mysql_Qatar_db", "Qatar_Sales")
}

def read_and_combine_data(**kwargs):
    storage_client = storage.Client()
    
    # File sources by bucket
    gcs_files = {
        GCS_BUCKET_MAIN: {
            "csv": "Japan_Sales.csv",
            "xlsx": "HongKong_Sales.xlsx"
        },
        GCS_BUCKET_SRILANKA: {
            "json": "SriLanka_Sales.json"
        }
    }

    dataframes = []

    # Read GCS files
    for bucket_name, file_map in gcs_files.items():
        bucket = storage_client.bucket(bucket_name)

        for file_format, file_name in file_map.items():
            local_file = f"/tmp/{file_name}"
            blob = bucket.blob(file_name)
            blob.download_to_filename(local_file)

            if file_format == "csv":
                df = pd.read_csv(local_file)
                df["Country"] = "Japan"
            elif file_format == "json":
                df = pd.read_json(local_file)
                df["Country"] = "SriLanka"
            elif file_format == "xlsx":
                df = pd.read_excel(local_file)
                df["Country"] = "HongKong"
            else:
                raise ValueError(f"Unsupported format: {file_format}")

            dataframes.append(df)

    # Read from SQL databases
    db_dataframes = []
    for country, (conn_id, table) in DB_CONNECTIONS.items():
        if conn_id.startswith("mysql"):
            hook = MySqlHook(mysql_conn_id=conn_id)
            df = hook.get_pandas_df(f"SELECT * FROM {table}")
        elif conn_id.startswith("postgres"):
            hook = PostgresHook(postgres_conn_id=conn_id)
            df = hook.get_pandas_df(f'SELECT * FROM "{table}"')
        elif conn_id.startswith("mssql"):
            hook = MsSqlHook(mssql_conn_id=conn_id)
            df = hook.get_pandas_df(f"SELECT * FROM {table}")
        else:
            raise ValueError(f"Unsupported DB type: {conn_id}")
        
        df["Country"] = country
        db_dataframes.append(df)

    # Combine all data
    all_data = pd.concat(dataframes + db_dataframes, ignore_index=True)
    return all_data

def clean_and_transform_data(df, **kwargs):
    if df.empty:
        raise ValueError("Dataframe is empty, no data to process")

    # Clean and transform
    df = df.dropna()
    df["Amount"] = df["Qty"] * df["Sale"]
    df["INR_Amount"] = df["Amount"] * df["Country"].map(CURRENCY_RATES).fillna(1)
    df["Year"] = np.random.randint(1900, 2001, size=len(df))

    return df

def load_to_bigquery(df, **kwargs):
    if df.empty:
        raise ValueError("No data to load to BigQuery")

    client = bigquery.Client()
    dataset_id = "globalsalesanalysis.finaldata"  # Replace with your project.dataset
    table_id = f"{dataset_id}.final_sales"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("SaleId", "STRING"),
            bigquery.SchemaField("Category", "STRING"),
            bigquery.SchemaField("Product", "STRING"),
            bigquery.SchemaField("Qty", "INTEGER"),
            bigquery.SchemaField("Sale", "FLOAT"),
            bigquery.SchemaField("Amount", "FLOAT"),
            bigquery.SchemaField("INR_Amount", "FLOAT"),
            bigquery.SchemaField("Country", "STRING"),
            bigquery.SchemaField("Year", "INTEGER"),
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    client.load_table_from_dataframe(df, table_id, job_config=job_config)

def create_etl_task(**kwargs):
    df_combined = read_and_combine_data(**kwargs)
    df_cleaned = clean_and_transform_data(df_combined, **kwargs)
    load_to_bigquery(df_cleaned, **kwargs)

# DAG definition
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id='project-airflow_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Read from GCS/DB, clean, transform, and load to BigQuery'
)

etl_task = PythonOperator(
    task_id="etl_process_to_bigquery",
    python_callable=create_etl_task,
    provide_context=True,
    dag=dag
)

etl_task