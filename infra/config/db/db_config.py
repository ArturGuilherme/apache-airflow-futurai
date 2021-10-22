import MySQLdb
from infra.config.db.env_config import db_config


class DBConnectionHandler:
    """MySQL database connection"""

    def __init__(self) -> None:

        self.conn = MySQLdb.Connection(
            host=db_config["SERVER_DB"],
            user=db_config["USER_DB"],
            passwd=db_config["PASSWORD_DB"],
            port=db_config["PORT_DB"],
            db=db_config["DB"],
        )
