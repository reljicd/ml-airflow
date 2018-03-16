import datetime
from collections import namedtuple

from sqlalchemy import Column, Table, Integer, DateTime, ForeignKey, and_, String
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import IntegrityError

from dags.exceptions.db_exception import DBException
from dags.repositories.base_db import BaseDatabase
from dags.repositories.ml_dag import MLDagTable, MLDagRowTuple

MLTrainingDagRowTuple = namedtuple('MLTrainingDagRowTuple', ['id', 'ml_dag', 'parameter_2'])


class MLTrainingDagTable(BaseDatabase):
    _table_name = 'ml_training_dag'

    table = Table(_table_name, BaseDatabase.metadata,

                  Column('id', Integer, primary_key=True),
                  Column('ml_dag_id', Integer, ForeignKey("ml_dag.id"), nullable=False, unique=True),
                  Column('parameter_2', String, nullable=False),
                  Column('datetime_created', DateTime, default=datetime.datetime.utcnow),

                  extend_existing=True)

    def __init__(self, engine: Engine = None):
        super().__init__(engine=engine)

    def insert_ml_training_dag(self, ml_training_dag: MLTrainingDagRowTuple) -> MLTrainingDagRowTuple:
        """ Inserts new ml_training_dag row in DB

        Args:
            ml_training_dag: MLTrainingDagRowTuple for insertion

        Returns: Inserted MLTrainingDagRowTuple

        """
        try:
            self.table.insert().values(ml_dag_id=ml_training_dag.ml_dag.id,
                                       parameter_2=ml_training_dag.parameter_2,
                                       datetime_created=datetime.datetime.utcnow()).execute()
        except IntegrityError:
            raise DBException(
                f'ml_training_dag with [ml_dag_id: {ml_training_dag.ml_dag.id}] already exists in DB')

        return self.select_ml_training_dag_for_parameters(
            parameter_1=ml_training_dag.ml_dag.parameter_1,
            parameter_2=ml_training_dag.parameter_2)

    def select_ml_training_dag_for_parameters(self,
                                              parameter_1: str,
                                              parameter_2: str) -> MLTrainingDagRowTuple:
        """ Returns MLTrainingDagRowTuple for parameters

        Args:
            parameter_1:
            parameter_2:

        Returns: MLTrainingDagRowTuple with parameters

        Raises:
            DBException: If ml_training_dag with parameters does not exist in db

        """
        ml_training_dag_dag_join = self.table.join(MLDagTable.table).select().where(
            and_(MLDagTable.table.c.parameter_1 == parameter_1,
                 self.table.c.parameter_2 == parameter_2)
        ).execute().first()

        if ml_training_dag_dag_join:
            return MLTrainingDagRowTuple(
                id=ml_training_dag_dag_join[self.table.c.id],
                ml_dag=MLDagRowTuple(id=ml_training_dag_dag_join[MLDagTable.table.c.id],
                                     parameter_1=ml_training_dag_dag_join[MLDagTable.table.c.parameter_1]),
                parameter_2=ml_training_dag_dag_join[self.table.c.parameter_2])
        else:
            raise DBException(
                f'ml_training_dag with [parameter_1: {parameter_1}] and '
                f'[parameter_2: {parameter_2}] does not exists')
