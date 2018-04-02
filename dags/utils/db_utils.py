from typing import Union

from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine


def create_db_engine(login: Union[str, None],
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
