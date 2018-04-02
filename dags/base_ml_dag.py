import abc

import sqlalchemy.engine
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.dates import days_ago

from dags.config import settings
from dags.logging.logging_mixin import LoggingMixin
from dags.repositories.common_task_1 import CommonTask1Repository
from dags.repositories.common_task_2 import CommonTask2Repository
from dags.subdags.base_subdag import MLTaskSubDag
from dags.subdags.common_task_1_subdag import CommonTask1SubDag
from dags.utils import db_utils


class BaseMLDAG(DAG, LoggingMixin):
    """ Base BaseMLDAG class """

    _args = {
        'owner': 'reljicd',
        'start_date': days_ago(3)
    }

    def __init__(self, dag_name: str):
        super().__init__(dag_id=dag_name,
                         default_args=self._args,
                         schedule_interval=None)

        self._initializer = PythonOperator(task_id='initializer',
                                           provide_context=True,
                                           python_callable=self._initialize,
                                           dag=self)

        self._common_task_1_subdag = SubDagOperator(task_id='common_task_1',
                                                    subdag=CommonTask1SubDag(args=self._args,
                                                                             parent_dag_id=dag_name,
                                                                             child_dag_id='common_task_1',
                                                                             repository_class=CommonTask1Repository).build(),
                                                    default_args=self._args,
                                                    dag=self)

        self._common_task_2_subdag = SubDagOperator(task_id='common_task_2',
                                                    subdag=MLTaskSubDag(args=self._args,
                                                                        parent_dag_id=dag_name,
                                                                        child_dag_id='common_task_2',
                                                                        repository_class=CommonTask2Repository).build(),
                                                    default_args=self._args,
                                                    dag=self)

    def _initialize(self, **kwargs) -> None:
        """ Python callable that initializes the whole pipeline.
        It extracts csv_path, batch_name and parameters from dag_run dict,
        gets dag for batch_name if exists, or insert new dag with batch_name,
        gets dag_run for dag_id and parameter if exists, or inserts new dag with dag_id and parameter,
        and than passes dag_run.id as XCom

        Args:
            get_ml_dag_id:
            **kwargs: Airflow context

        """
        self.log.debug(kwargs)

        if kwargs['dag_run']:
            connection = BaseHook.get_connection(settings.DB_CONN_ID)
            self.log.debug(f'Connection conn_type: {connection.conn_type}')
            self.log.debug(f'Connection login: {connection.login}')
            self.log.debug(f'Connection password: {connection.password}')
            self.log.debug(f'Connection port: {connection.port}')
            self.log.debug(f'Connection host: {connection.host}')

            engine = db_utils.create_db_engine(login=connection.login,
                                               password=connection.password,
                                               host=connection.host,
                                               schema=connection.schema,
                                               conn_type=connection.conn_type)

            ml_dag_id = self._get_ml_dag_id(engine=engine, **kwargs)

            # Push as XCOM ml_dag_id for downstream tasks to use
            kwargs['task_instance'].xcom_push(key='ml_dag_id', value=ml_dag_id)

            self.log.info(f'ml_dag_id: {ml_dag_id}')

    @staticmethod
    @abc.abstractmethod
    def _get_ml_dag_id(dag_id: str, engine: sqlalchemy.engine.Engine, **kwargs) -> str:
        pass
