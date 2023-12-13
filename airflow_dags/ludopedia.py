from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

with DAG(
    'ludopedia',
    start_date=datetime(2023, 12, 12),
    schedule_interval="0 9 * * *" #Todos os dias as 9 da manhã
    ) as dag_executando_notebook_extracao_ludo:

    extraindo_dados_ludo = DatabricksRunNowOperator(
    task_id = 'Extraindo-posts',
    databricks_conn_id = 'databricks_default',
    job_id = <DATA-BRICKS-JOB-ID>)

    transformando_dados_ludo = DatabricksRunNowOperator(
    task_id = 'Transformando-dados-posts',
    databricks_conn_id = 'databricks_default',
    job_id = <DATA-BRICKS-JOB-ID>)

    enviando_relatorio_ludo = DatabricksRunNowOperator(
    task_id = 'Enviando-relatorio-posts',
    databricks_conn_id = 'databricks_default',
    job_id = <DATA-BRICKS-JOB-ID>)

    extraindo_dados_ludo >> transformando_dados_ludo >> enviando_relatorio_ludo