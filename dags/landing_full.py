from airflow import DAG
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from datetime import datetime, timedelta
import json

RESOURCE_GROUP_NAME = "datafactory-rg134"
FACTORY_NAME = "mauroloprete-df"
PIPELINE_NAME = "copy_landing"
AZURE_CONN_ID = "azuredf"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='landing_dynamic_expand',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule='@hourly',
    catchup=False,
    max_active_runs=1,
    tags=['landing', 'expand', 'adf'],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

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
            par.parameter_name as parameters,
            en.activation_state,
            ls.last_success_date
        FROM dev_ref_control.general.ref_master_entities en
        INNER JOIN dev_ref_control.general.ref_master_parameters par 
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
        output_format="json"
    )


    from airflow.exceptions import AirflowFailException
    import json
    import ast

    @task
    def extract_params(ti=None):
        columns_data, rows_data = ti.xcom_pull(task_ids='fetch_entities_to_run')
        columns = [col[0] for col in columns_data]
        results = []

        for row in rows_data:
            params_str = row[columns.index('parameters')]
            print("RAW parameters string:", params_str)
            parsed_parameters = {}

            if params_str:
                try:
                    fixed_str = params_str
                    parsed_parameters = json.loads(fixed_str)
                except json.JSONDecodeError as e:
                    print(f"Error decoding fixed JSON: {e}")
                    try:
                        parsed_parameters = ast.literal_eval(fixed_str)
                    except Exception as ex:
                        print(f"Fallback parsing also failed: {ex}")
                        parsed_parameters = {}

            if not parsed_parameters:
                raise AirflowFailException(
                    f"Invalid or empty parameters for entity ID {row[columns.index('id_entity')]}. Aborting DAG."
                )

            results.append({
                "task_id": f"run_pipeline_{row[columns.index('id_entity')]}",
                "parameters": {
                    "params_copy": parsed_parameters
                },
                "azure_data_factory_conn_id": AZURE_CONN_ID,
                "pipeline_name": PIPELINE_NAME,
                "factory_name": FACTORY_NAME,
                "resource_group_name": RESOURCE_GROUP_NAME,
                "wait_for_termination": True,
            })

        return results


    entity_configs = extract_params()

    run_pipelines = AzureDataFactoryRunPipelineOperator.partial(task_id = "run_pipeline").expand_kwargs(entity_configs)


    start >> fetch_entities >> entity_configs >> dag.get_task("run_pipeline") >> end
