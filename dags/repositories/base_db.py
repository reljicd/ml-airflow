from typing import Union

from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine.base import Engine

from dags.config import settings


class BaseDatabase(object):
    """Parent class for all Database classes"""

    # All repository classes share this object, which is bind with engine only once
    metadata = MetaData()

    def __init__(self, engine: Engine = None):
        """ If engine is provided, binds engine to metadata.
        If it is not provided, creates it based on Airflow connection parameters and than binds it to metadata.
        """
        # Bind engine to metadata only if it is not bind already
        if self.metadata.bind is None:
            # Skip engine building if it is already provided
            if engine is None:
                connection = BaseHook.get_connection(conn_id=settings.DB_CONN_ID)

                engine = self.create_engine(login=connection.login,
                                            password=connection.password,
                                            host=connection.host,
                                            schema=connection.schema,
                                            conn_type=connection.conn_type)

            self.metadata.bind = engine

    @staticmethod
    def create_engine(login: Union[str, None],
                      password: Union[str, None],
                      host: str,
                      schema: Union[str, None],
                      conn_type: str = 'mysql') -> Engine:
        if conn_type == 'mysql':
            engine = create_engine(
                f'mysql+mysqlconnector://{login}:{password}@{host}/{schema}',
                echo=True)
        else:
            engine = create_engine(f'sqlite:///{host}')

        return engine
