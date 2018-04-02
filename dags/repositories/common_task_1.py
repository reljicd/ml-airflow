from sqlalchemy import Column, Table, Integer, DateTime, ForeignKey
from sqlalchemy.engine.base import Engine

from dags.repositories.base import BaseRepository
from dags.repositories.task_mixin import TaskRepositoryMixin


class CommonTask1Repository(BaseRepository, TaskRepositoryMixin):
    _table_name = 'common_task_1'

    table = Table(_table_name, BaseRepository.metadata,
                  Column('id', Integer, primary_key=True),
                  Column('ml_dag_id', Integer, ForeignKey("ml_dag.id"), nullable=False, unique=True),
                  Column('datetime_started', DateTime),
                  Column('datetime_finished', DateTime))

    def __init__(self, engine: Engine = None):
        super().__init__(engine=engine)
