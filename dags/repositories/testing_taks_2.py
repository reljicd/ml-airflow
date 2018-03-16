from sqlalchemy import Column, Table, Integer, DateTime, ForeignKey
from sqlalchemy.engine.base import Engine

from dags.repositories.base_db import BaseDatabase
from dags.repositories.task_mixin import TaskTableMixin


class TestingTask2Table(BaseDatabase, TaskTableMixin):
    _table_name = 'testing_task_2'

    table = Table(_table_name, BaseDatabase.metadata,
                  Column('id', Integer, primary_key=True),
                  Column('ml_dag_id', Integer, ForeignKey("ml_dag.id"), nullable=False, unique=True),
                  Column('datetime_started', DateTime),
                  Column('datetime_finished', DateTime))

    def __init__(self, engine: Engine = None):
        super().__init__(engine=engine)
