import datetime
from collections import namedtuple

from sqlalchemy import Column, Table, Integer, DateTime, String
from sqlalchemy.engine.base import Engine

from dags.exceptions.db_exception import DBException
from dags.repositories.base import BaseRepository

MLDagRow = namedtuple('MLDagRow', ['id', 'parameter_1'])


class MLDagRepository(BaseRepository):
    _table_name = 'ml_dag'

    table = Table(_table_name, BaseRepository.metadata,

                  Column('id', Integer, primary_key=True),
                  Column('datetime_created', DateTime, default=datetime.datetime.utcnow),
                  Column('parameter_1', String, nullable=False),

                  extend_existing=True)

    def __init__(self, engine: Engine = None):
        super().__init__(engine=engine)

    def save(self, ml_dag: MLDagRow) -> MLDagRow:
        """ Inserts new ml_dag row into DB

        Args:
            ml_dag:

        Returns: inserted MLDagRow

        """
        self.table.insert().values(parameter_1=ml_dag.parameter_1,
                                   datetime_created=datetime.datetime.utcnow()).execute()

        return self.find_by_parameter_1(parameter_1=ml_dag.parameter_1)

    def find_by_id(self, id: int) -> MLDagRow:
        """ Returns MLDagRow for row with id = ml_dag_id

        Returns: MLDagRow with ml_dag_id

        Raises:
            DBException: If ml_dag with ml_dag_id does not exist in db

        """
        ml_dag = self.table.select().where(self.table.c.id == id).execute().first()
        if ml_dag:
            return MLDagRow(id=ml_dag.id, parameter_1=ml_dag.parameter_1)
        else:
            raise DBException(
                f'ml_dag with [id: {id}] does not exists')

    def find_by_parameter_1(self, parameter_1: str) -> MLDagRow:
        """ Returns MLDagRow for row with parameter_1 = parameter_1

        Returns: MLDagRow with parameter_1

        Raises:
            DBException: If ml_dag with parameter_1 does not exist in db

        """
        ml_dag = self.table.select().where(self.table.c.parameter_1 == parameter_1).execute().first()
        if ml_dag:
            return MLDagRow(id=ml_dag.id, parameter_1=ml_dag.parameter_1)
        else:
            raise DBException(
                f'ml_dag with [parameter_1: {parameter_1}] does not exists')

    def check_ml_dag_id(self, ml_dag_id: int) -> None:
        """ Checks if ml_dag run with ml_dag_id exists in db

        Raises:
            DBException: If ml_dag with ml_dag_id does not exist in db
        """
        first_row = self.table.select().where(self.table.c.id == ml_dag_id).execute().first()

        if first_row is None:
            raise DBException(f'ml_dag with id[{ml_dag_id}] does not exists')
