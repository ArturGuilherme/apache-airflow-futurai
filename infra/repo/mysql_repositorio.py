import sys

sys.path.insert(0, "/opt/airflow/")

from infra.config.db import DBConnectionHandler
import pandas as pd


class MysqlRespositorio:
    def __init__(self):
        self.db = DBConnectionHandler()

    def find(self, tabela: str, columns: str, col_timestamp: str, data_ini, data_fim):

        select_col = ""
        for col in columns:
            select_col = select_col + ", " + col

        select_col = select_col[2:]

        query = "SELECT " + select_col + " FROM " + tabela
        query = (
            query
            + " WHERE "
            + col_timestamp
            + " BETWEEN '"
            + data_ini
            + "' AND '"
            + data_fim
            + "'"
        )

        try:
            df = pd.read_sql(query, self.db.conn)
        except:
            raise

        return df
