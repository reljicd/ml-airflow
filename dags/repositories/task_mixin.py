import datetime

from sqlalchemy.exc import IntegrityError

from dags.exceptions.db_exception import DBException
from dags.repositories.ml_dag import MLDagTable


class TaskTableMixin(object):
    """ Mixin for Task database classes. """

    def insert_task_with_ml_dag_id(self, ml_dag_id: int) -> None:
        """ Inserts new sample with ml_dag_id and sample_id into DB

        Args:
            ml_dag_id:

        Raises:
            DBException - if task with ml_dag_id already exists in DB

        """
        try:
            self.table.insert().values(ml_dag_id=ml_dag_id).execute()
        except IntegrityError:
            raise DBException(
                f'task with [ml_dag_id: {ml_dag_id}] already in DB')

    def is_task_finished(self, ml_dag_id: int) -> bool:
        """ Checks if task is finished, based on the value of datetime_finished field.

        Args:
            ml_dag_id:

        Returns: bool

        """
        MLDagTable().check_ml_dag_id(ml_dag_id=ml_dag_id)
        self._check_task_with_ml_dag_id(ml_dag_id=ml_dag_id)

        datetime_finished = self.table.select().where(
            self.table.c.ml_dag_id == ml_dag_id).execute().first().datetime_finished

        return True if datetime_finished else False

    def start_task(self, ml_dag_id: int) -> None:
        """ Starts the task by writing datetime_started field in db.

        Args:
            ml_dag_id:

        """
        MLDagTable().check_ml_dag_id(ml_dag_id=ml_dag_id)
        self._check_task_with_ml_dag_id(ml_dag_id=ml_dag_id)

        self.table.update().where(self.table.c.ml_dag_id == ml_dag_id).values(
            datetime_started=datetime.datetime.utcnow()).execute()

    def finish_task(self, ml_dag_id: int) -> None:
        """ Finishes the task by writing datetime_finished field in db.

        Args:
            ml_dag_id:

        """
        MLDagTable().check_ml_dag_id(ml_dag_id=ml_dag_id)
        self._check_task_with_ml_dag_id(ml_dag_id=ml_dag_id)

        self.table.update().where(self.table.c.ml_dag_id == ml_dag_id).values(
            datetime_finished=datetime.datetime.utcnow()).execute()

    def _check_task_with_ml_dag_id(self, ml_dag_id: int) -> None:
        """ Checks if task with task with ml_dag_id exists in db

        Args:
            ml_dag_id:

        Raises:
            DBException: If task with ml_dag_id does not exist in db

        """
        first_row = self.table.select().where(self.table.c.ml_dag_id == ml_dag_id).execute().first()

        if first_row is None:
            raise DBException(f'task with [ml_dag_id: {ml_dag_id}] doesnt exist')
