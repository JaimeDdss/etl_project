from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('../src') 
from config.config import API_URL, CSV_FILE_PATH1, XLS_FILE_PATH2, OUTPUT_PATH
from src.pipeline import run_pipeline 

# Configuração padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definindo a DAG
with DAG(
    'commodity_data_pipeline',
    default_args=default_args,
    description='DAG para pipeline de ETL de commodities',
    schedule_interval=timedelta(days=3),  
    start_date=datetime(2024, 11, 6),
    catchup=False,
) as dag:

    # Task 1: Executar o pipeline principal
    def airflow_main():
        df_result = run_pipeline()
        print(f'Dados salvos com sucesso: \n {df_result.shape}')

        #output_path = './data/upsert_boi_gordo_Vfinal.parquet'  
        #pipeline(output_path=output_path)  
        

    run_pipeline_task = PythonOperator(
        task_id='run_pipeline',
        python_callable=airflow_main
    )

    # task
    run_pipeline_task
