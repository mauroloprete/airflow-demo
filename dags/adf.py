from airflow import DAG
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from datetime import datetime

RESOURCE_GROUP_NAME = "datafactory-rg134"
FACTORY_NAME = "mauroloprete-df"
PIPELINE_NAME = "pipeline1_helloworld"
AZURE_CONN_ID = "azuredf"  # ID de la conexión en Airflow

with DAG(
    dag_id="run_adf_pipeline_operator",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["azure", "adf"],
) as dag:

    run_pipeline = AzureDataFactoryRunPipelineOperator(
        task_id="run_adf_pipeline",
        pipeline_name=PIPELINE_NAME,
        azure_data_factory_conn_id=AZURE_CONN_ID,
        resource_group_name=RESOURCE_GROUP_NAME,
        factory_name=FACTORY_NAME,
        parameters={
            'mi_nombre': 'Mauro'
        },  # opcional: parámetros para el pipeline
        wait_for_termination=True,
    )

    run_pipeline
