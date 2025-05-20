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
    dag_id='data_landing_provisioning',
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
        SELECT 1
        """,
        do_xcom_push=True,
    )

    start >> fetch_entities

    def build_origen_groups(**context):
        from airflow.models.xcom import XCom
        import json
        from airflow.models.taskinstance import TaskInstance

        ti: TaskInstance = context["ti"]
        entities = ti.xcom_pull(task_ids='fetch_entities_to_run')

        grouped = {}
        for row in entities:
            origen = row['origen']
            grouped.setdefault(origen, []).append(row)

        # Ordenar las entidades dentro de cada grupo por peso promedio
        for origen in grouped:
            grouped[origen] = sorted(grouped[origen], key=lambda x: x['peso_promedio_mb'])

        return grouped

    from airflow.decorators import task

    @task
    def get_origen_groups():
        return build_origen_groups()

    origen_groups = get_origen_groups()
    fetch_entities >> origen_groups

    # 2. Crear tareas dinámicamente agrupadas por origen
    
    from airflow.operators.python import get_current_context
    @task
    def create_tasks_per_origen():
        context = get_current_context()
        grouped = context['ti'].xcom_pull(task_ids='get_origen_groups')

        for origen, entidades in grouped.items():
            with TaskGroup(group_id=f"group_{origen}") as origen_group:
                last_task = None

                for idx, entidad in enumerate(entidades):
                    run_adf_pipeline = AzureDataFactoryRunPipelineOperator(
                        task_id=f"run_pipeline_{entidad['entidad_id']}",
                        azure_data_factory_conn_id=AZURE_CONN_ID,
                        pipeline_name=entidad['pipeline_adf'],
                        factory_name=FACTORY_NAME,
                        resource_group_name=RESOURCE_GROUP_NAME,
                        parameters=json.loads(entidad['parametros_adf']),
                        wait_for_termination=True,
                    )

                    if last_task:
                        last_task >> run_adf_pipeline
                    last_task = run_adf_pipeline

                origen_group >> end
                start >> origen_group

    trigger_tasks = create_tasks_per_origen()
    origen_groups >> trigger_tasks

