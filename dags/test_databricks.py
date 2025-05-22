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
        WITH latest_success AS (
            SELECT
                id_entity,
                MAX(TO_TIMESTAMP_NTZ(CAST(date AS STRING), 'yyyyMMdd')) AS last_success_date
            FROM dev_ref_control.general.ref_log_execs
            WHERE status = 'success'
            GROUP BY id_entity
        ),
        to_run AS (
            SELECT 
                en.id_entity,
                en.grouper_name,
                en.layer_name,
                en.container_name,
                en.entity_name,
                en.defined_calendar,
                con.parameters,
                en.activation_state,
                ls.last_success_date
            FROM dev_ref_control.general.ref_master_entities en
            LEFT JOIN dev_ref_control.general.ref_master_parameters par 
                ON par.id_entity = en.id_entity
            LEFT JOIN dev_ref_control.general.ref_relation_ingest_connection relcon 
                ON relcon.id_entity = en.id_entity
            LEFT JOIN dev_ref_control.general.ref_connections con 
                ON con.id_connection = relcon.id_connection
            LEFT JOIN latest_success ls 
                ON ls.id_entity = en.id_entity
            WHERE en.active_state = 'Y'
        )
        SELECT *
        FROM to_run
        WHERE (
        (defined_calendar = 'hourly' AND (last_success_date IS NULL OR DATE_TRUNC('HOUR', last_success_date) < DATE_TRUNC('HOUR', CURRENT_TIMESTAMP)))
        OR
        (defined_calendar = 'daily' AND (last_success_date IS NULL OR DATE(last_success_date) < CURRENT_DATE))
        OR
        (defined_calendar = 'monthly' AND (last_success_date IS NULL OR DATE_TRUNC('MONTH', last_success_date) < DATE_TRUNC('MONTH', CURRENT_DATE)))
        OR
        (defined_calendar = 'quarterly' AND (last_success_date IS NULL OR DATE_TRUNC('QUARTER', last_success_date) < DATE_TRUNC('QUARTER', CURRENT_DATE)))
        OR
        (defined_calendar = 'yearly' AND (last_success_date IS NULL OR DATE_TRUNC('YEAR', last_success_date) < DATE_TRUNC('YEAR', CURRENT_DATE)))
        )
        """,
        http_path="/sql/1.0/warehouses/2eea0c5450238ddf",
        do_xcom_push=True,
    )

    start >> fetch_entities >> end
    