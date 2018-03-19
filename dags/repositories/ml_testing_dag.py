import datetime
from collections import namedtuple

from sqlalchemy import Column, Table, Integer, DateTime, ForeignKey, and_, String, UniqueConstraint
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import IntegrityError

from dags.exceptions.db_exception import DBException
from dags.repositories.base_db import BaseDatabase
from dags.repositories.ml_dag import MLDagTable, MLDagRowTuple

MLTestingDagRowTuple = namedtuple('MLTestingDagRowTuple', ['id', 'ml_dag', 'parameter_3'])


class MLTestingDagTable(BaseDatabase):
    _table_name = 'ml_testing_dag'

    table = Table(_table_name, BaseDatabase.metadata,

                  Column('id', Integer, primary_key=True),
                  Column('ml_dag_id', Integer, ForeignKey("ml_dag.id"), nullable=False),
                  Column('parameter_3', String, nullable=False),
                  Column('datetime_created', DateTime, default=datetime.datetime.utcnow),

                  UniqueConstraint('ml_dag_id', 'parameter_3'),

                  extend_existing=True)

    def __init__(self, engine: Engine = None):
        super().__init__(engine=engine)

    def insert_ml_testing_dag(self, ml_testing_dag: MLTestingDagRowTuple) -> MLTestingDagRowTuple:
        """ Inserts new ml_testing_dag row in DB

        Args:
            ml_testing_dag: MLTestingDagRowTuple for insertion

        Returns: Inserted MLTestingDagRowTuple

        """
        try:
            self.table.insert().values(ml_dag_id=ml_testing_dag.ml_dag.id,
                                       parameter_3=ml_testing_dag.parameter_3,
                                       datetime_created=datetime.datetime.utcnow()).execute()
        except IntegrityError:
            raise DBException(
                f'ml_testing_dag with [ml_dag_id: {ml_testing_dag.ml_dag.id}] '
                f'and [parameter_3: {ml_testing_dag.parameter_3}] already exists in DB')

        return self.select_ml_testing_dag_for_parameters(
            parameter_1=ml_testing_dag.ml_dag.parameter_1,
            parameter_3=ml_testing_dag.parameter_3)

    def select_ml_testing_dag_for_parameters(self,
                                             parameter_1: str,
                                             parameter_3: str) -> MLTestingDagRowTuple:
        """ Returns MLTestingDagRowTuple for parameters

        Args:
            parameter_1:
            parameter_3:

        Returns: MLTestingDagRowTuple with parameters

        Raises:
            DBException: If ml_testing_dag with parameters does not exist in db

        """
        ml_testing_dag_dag_join = self.table.join(MLDagTable.table).select().where(
            and_(MLDagTable.table.c.parameter_1 == parameter_1,
                 self.table.c.parameter_3 == parameter_3)
        ).execute().first()

        if ml_testing_dag_dag_join:
            return MLTestingDagRowTuple(
                id=ml_testing_dag_dag_join[self.table.c.id],
                ml_dag=MLDagRowTuple(id=ml_testing_dag_dag_join[MLDagTable.table.c.id],
                                     parameter_1=ml_testing_dag_dag_join[MLDagTable.table.c.parameter_1]),
                parameter_3=ml_testing_dag_dag_join[self.table.c.parameter_3])
        else:
            raise DBException(
                f'ml_testing_dag with [parameter_1: {parameter_1}] and '
                f'[parameter_3: {parameter_3}] does not exists')
