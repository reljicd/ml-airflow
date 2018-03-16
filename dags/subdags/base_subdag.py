import abc
from typing import TypeVar, Dict

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from sqlalchemy.engine.base import Engine

from dags.exceptions.db_exception import DBException
from dags.logging.logging_mixin import LoggingMixin
from dags.operators.parametrized_bash_operator import ParametrizedBashOperator
from dags.repositories.task_mixin import TaskTableMixin
from dags.utils import dag_utils


class MLTaskSubDag(LoggingMixin):
    """ Class for Epi Tasks subDAGs """

    def __init__(self,
                 args: Dict,
                 parent_dag_id: str,
                 child_dag_id: str,
                 repository_class: TypeVar(TaskTableMixin),
                 engine: Engine = None):
        """ Defines subDAG tasks """

        self._parent_dag_id = parent_dag_id
        self._child_dag_id = child_dag_id
        self._repository_class = repository_class
        self._engine = engine

        self._subdag = DAG(
            dag_id=f'{self._parent_dag_id}.{self._child_dag_id}',
            default_args=args,
            schedule_interval=None)

        self._initialize_task_operator = PythonOperator(
            task_id=f'initialize_{self._child_dag_id}',
            provide_context=True,
            python_callable=self._initialize_task,
            dag=self._subdag)

        self._conditional_operator = BranchPythonOperator(
            task_id=f'conditional_{self._child_dag_id}',
            provide_context=True,
            python_callable=self._execute_or_skip_task,
            dag=self._subdag)

        self._dummy_operator = DummyOperator(
            task_id=f'skip_{self._child_dag_id}',
            dag=self._subdag)

        self._start_task_in_db_operator = PythonOperator(
            task_id=f'start_task_in_db_{self._child_dag_id}',
            provide_context=True,
            python_callable=self._start_task_in_db,
            dag=self._subdag)

        self._parametrized_bash_operator = ParametrizedBashOperator(
            task_id=f'qubole_{self._child_dag_id}',
            parameters_provider=self._parameters_provider,
            bash_command='echo',
            dag=self._subdag)

        self._finish_task_in_db_operator = PythonOperator(
            task_id=f'finish_task_in_db_{self._child_dag_id}',
            provide_context=True,
            python_callable=self._finish_task_in_db,
            dag=self._subdag)

        self._join_operator = DummyOperator(
            task_id=f'join_{self._child_dag_id}',
            trigger_rule='one_success',
            dag=self._subdag)

    def _initialize_task(self,
                         **kwargs) -> None:
        """ Inserts task with ml_dag_id into DB, if it doesn't already exists in DB

        Args:
            **kwargs: Airflow context

        """
        self.log.debug(f'kwargs: {kwargs}')

        ml_dag_id = dag_utils.get_ml_dag_id(parent_dag_id=self._parent_dag_id, **kwargs)

        try:
            self._repository_class(engine=self._engine).insert_task_with_ml_dag_id(ml_dag_id=ml_dag_id)
        except DBException:
            pass

    def _execute_or_skip_task(self,
                              **kwargs) -> str:
        """ Conditional that chooses task that should be executed after branching based on presence of datetime_finished
        in repository for task (based on repository_class).

        Args:
            **kwargs: Airflow context

        Returns: Name of the task that should be executed after branching

        """
        self.log.debug(f'kwargs: {kwargs}')

        ml_dag_id = dag_utils.get_ml_dag_id(parent_dag_id=self._parent_dag_id, **kwargs)

        if self._repository_class(engine=self._engine).is_task_finished(ml_dag_id=ml_dag_id):
            return 'skip_{}'.format(self._child_dag_id)
        else:
            return 'start_task_in_db_{}'.format(self._child_dag_id)

    def _start_task_in_db(self,
                          **kwargs) -> None:
        """ Writes datetime_started to task table (based on repository_class) for ml_dag_id

        Args:
            **kwargs: Airflow context

        """
        self.log.debug(f'kwargs: {kwargs}')

        ml_dag_id = dag_utils.get_ml_dag_id(parent_dag_id=self._parent_dag_id, **kwargs)

        self._repository_class(engine=self._engine).start_task(ml_dag_id=ml_dag_id)

    def _finish_task_in_db(self,
                           **kwargs) -> None:
        """ Writes datetime_finished to task table (based on repository_class) for ml_dag_id

        Args:
            **kwargs: Airflow context

        """
        self.log.debug(f'kwargs: {kwargs}')

        ml_dag_id = dag_utils.get_ml_dag_id(parent_dag_id=self._parent_dag_id, **kwargs)

        self._repository_class(engine=self._engine).finish_task(ml_dag_id=ml_dag_id)

    @abc.abstractmethod
    def _parameters_provider(self,
                             **kwargs) -> str:
        """ Abstract Callable that provides additional parameters for Bash calls.

        Returns: If not overridden returns empty string

        """
        return ''

    def build(self) -> DAG:
        """ Constructs and returns subDAG

        Returns: Initialized subDAG

        """
        # DAG edges definitions
        self._conditional_operator.set_upstream(self._initialize_task_operator)
        self._start_task_in_db_operator.set_upstream(self._conditional_operator)
        self._parametrized_bash_operator.set_upstream(self._start_task_in_db_operator)
        self._finish_task_in_db_operator.set_upstream(self._parametrized_bash_operator)

        self._dummy_operator.set_upstream(self._conditional_operator)

        self._join_operator.set_upstream([self._dummy_operator, self._finish_task_in_db_operator])

        return self._subdag
