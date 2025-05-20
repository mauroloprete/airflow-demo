from airflow import DAG
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta


RESOURCE_GROUP_NAME = "datafactory-rg134"
FACTORY_NAME = "mauroloprete-df"
PIPELINE_NAME = "pipeline1_helloworld"
AZURE_CONN_ID = "azuredf"


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='test_databricks',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule='@hourly',
    catchup=False,
    max_active_runs=1,
    description='Provisiona la capa landing desde múltiples orígenes',
    tags=['landing', 'adf', 'databricks', 'orquestación'],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # 1. Obtener lista de entidades a ejecutar desde Databricks
    fetch_entities = DatabricksSqlOperator(
        task_id='fetch_entities_to_run',
        databricks_conn_id='databricks_default',
        sql="""
        SELECT 'mauro' as name
        """,
        http_path="/sql/1.0/warehouses/a571a4e1c22220ac",
        do_xcom_push=True,
    )

    start >> fetch_entities >> end
    