import yaml
from yaml.loader import SafeLoader


def load_dags_config(nome_arquivo):
    """Função resposavel por carregar os valores
    de confituração de cada arquivo yaml"""

    # Open the file and load the file
    with open("./infra/config/dags/" + nome_arquivo + "") as f:
        dags_config = yaml.load(f, Loader=SafeLoader)

        processo = dags_config["PROCESSO"]
        subprocesso = dags_config["SUBPROCESSO"]
        tabela = dags_config["TABELA"]
        col_date = dags_config["COL_TIMESTAMP"]
        colunas = dags_config["COLUNAS"]

        return processo, subprocesso, tabela, col_date, colunas
