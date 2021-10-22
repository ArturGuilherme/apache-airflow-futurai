from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import sys

sys.path.insert(0, "/opt/airflow/")

from infra.config.api import api_config
from infra.config.dags import load_dags_config
from infra.repo import MysqlRespositorio
import requests
import os

APIKEY = api_config["APIKEY"]
USER = api_config["USER"]
URL = api_config["URL"]

processo, subprocesso, tabela, col_date, colunas = load_dags_config(
    "fabrica_moinho.yaml"
)

data_ini = "2021-08-01 03:00"
data_fim = "2021-08-01 05:00"

data_ini_name = data_ini.replace(" ", "_")
data_fim_name = data_fim.replace(" ", "_")

data_ini_name = data_ini_name.replace(":", "-")
data_fim_name = data_fim_name.replace(":", "-")

nome_arquivo = (
    processo + "_" + subprocesso + "_" + data_ini_name + "_" + data_fim_name + ".csv"
)


def consulta_banco(nome_arquivo, data_ini, data_fim, tabela, col_date, colunas):
    mysql = MysqlRespositorio()
    results = mysql.find(tabela, colunas, col_date, data_ini, data_fim)
    results.to_csv("./storage/fila/" + nome_arquivo, sep=";", index=False)


def envia_arquivo(URL, APIKEY, USER, processo, subprocesso):
    with open("./storage/fila/" + nome_arquivo, "rb") as csv_file:
        files = {"arquivo": csv_file}
        headers = {"apikey": APIKEY}
        payload = {"empresa": USER, "processo": processo, "equipamento": subprocesso}

        response = requests.post(url=URL, files=files, data=payload, headers=headers)

    if response.status_code < 300:
        os.replace(
            "./storage/fila/" + nome_arquivo,
            "./storage/processados/" + nome_arquivo,
        )
    else:
        os.replace(
            "./storage/fila/" + nome_arquivo,
            "./storage/errors/" + nome_arquivo,
        )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["silvasaick@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG(
    "fabrica_moinho",
    default_args=default_args,
    description="Envio dos dados para a platforma predict",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = PythonOperator(
        task_id="consulta-banco",
        python_callable=consulta_banco,
        op_kwargs={
            "nome_arquivo": nome_arquivo,
            "data_ini": data_ini,
            "data_fim": data_fim,
            "tabela": tabela,
            "col_date": col_date,
            "colunas": colunas,
        },
    )

    t2 = PythonOperator(
        task_id="envia-file",
        python_callable=envia_arquivo,
        op_kwargs={
            "URL": URL,
            "APIKEY": APIKEY,
            "USER": USER,
            "processo": processo,
            "subprocesso": subprocesso,
        },
    )

    t1 >> t2
