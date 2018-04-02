import datetime
from collections import namedtuple

from sqlalchemy import Column, Table, Integer, DateTime, ForeignKey, and_, String, UniqueConstraint
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import IntegrityError

from dags.exceptions.db_exception import DBException
from dags.repositories.base import BaseRepository
from dags.repositories.ml_dag import MLDagRepository, MLDagRow

MLTrainingDagRow = namedtuple('MLTrainingDagRow', ['id', 'ml_dag', 'parameter_2'])


class MLTrainingDagRepository(BaseRepository):
    _table_name = 'ml_training_dag'

    table = Table(_table_name, BaseRepository.metadata,

                  Column('id', Integer, primary_key=True),
                  Column('ml_dag_id', Integer, ForeignKey("ml_dag.id"), nullable=False),
                  Column('parameter_2', String, nullable=False),
                  Column('datetime_created', DateTime, default=datetime.datetime.utcnow),

                  UniqueConstraint('ml_dag_id', 'parameter_2'),

                  extend_existing=True)

    def __init__(self, engine: Engine = None):
        super().__init__(engine=engine)

    def save(self, ml_training_dag: MLTrainingDagRow) -> MLTrainingDagRow:
        """ Inserts new ml_training_dag row in DB

        Args:
            ml_training_dag: MLTrainingDagRow for insertion

        Returns: Inserted MLTrainingDagRow

        """
        try:
            self.table.insert().values(ml_dag_id=ml_training_dag.ml_dag.id,
                                       parameter_2=ml_training_dag.parameter_2,
                                       datetime_created=datetime.datetime.utcnow()).execute()
        except IntegrityError:
            raise DBException(
                f'ml_testing_dag with [ml_dag_id: {ml_training_dag.ml_dag.id}] '
                f'and [parameter_3: {ml_training_dag.parameter_2}] already exists in DB')

        return self.find_by_parameters(
            parameter_1=ml_training_dag.ml_dag.parameter_1,
            parameter_2=ml_training_dag.parameter_2)

    def find_by_parameters(self,
                           parameter_1: str,
                           parameter_2: str) -> MLTrainingDagRow:
        """ Returns MLTrainingDagRow for parameters

        Args:
            parameter_1:
            parameter_2:

        Returns: MLTrainingDagRow with parameters

        Raises:
            DBException: If ml_training_dag with parameters does not exist in db

        """
        ml_training_dag_dag_join = self.table.join(MLDagRepository.table).select().where(
            and_(MLDagRepository.table.c.parameter_1 == parameter_1,
                 self.table.c.parameter_2 == parameter_2)
        ).execute().first()

        if ml_training_dag_dag_join:
            return MLTrainingDagRow(
                id=ml_training_dag_dag_join[self.table.c.id],
                ml_dag=MLDagRow(id=ml_training_dag_dag_join[MLDagRepository.table.c.id],
                                parameter_1=ml_training_dag_dag_join[MLDagRepository.table.c.parameter_1]),
                parameter_2=ml_training_dag_dag_join[self.table.c.parameter_2])
        else:
            raise DBException(
                f'ml_training_dag with [parameter_1: {parameter_1}] and '
                f'[parameter_2: {parameter_2}] does not exists')
