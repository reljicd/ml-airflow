from airflow.hooks.base_hook import BaseHook
from sqlalchemy import MetaData
from sqlalchemy.engine.base import Engine

from dags.config import settings
from dags.utils import db_utils


class BaseRepository(object):
    # All repository classes share this object, which is bind with engine only once
    metadata = MetaData()

    def __init__(self, engine: Engine):
        # Bind engine to metadata only if it is not bind already
        if self.metadata.bind is None:
            # Skip engine building if it is already provided
            if engine is None:
                connection = BaseHook.get_connection(conn_id=settings.DB_CONN_ID)

                engine = db_utils.create_db_engine(login=connection.login,
                                                   password=connection.password,
                                                   host=connection.host,
                                                   schema=connection.schema,
                                                   conn_type=connection.conn_type)

            self.metadata.bind = engine
