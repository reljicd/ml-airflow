from typing import Dict

from sqlalchemy.engine.base import Engine

from dags.repositories.common_task_1 import CommonTask1Repository
from dags.subdags.base_subdag import MLTaskSubDag
from dags.utils import dag_utils


class CommonTask1SubDag(MLTaskSubDag):
    """ Class for Common Task 1 subDAGs """

    def __init__(self,
                 args: Dict,
                 parent_dag_id: str,
                 child_dag_id: str,
                 repository_class=CommonTask1Repository,
                 engine: Engine = None):
        """ Defines subDAG tasks """
        super(CommonTask1SubDag, self).__init__(args=args,
                                                parent_dag_id=parent_dag_id,
                                                child_dag_id=child_dag_id,
                                                repository_class=repository_class,
                                                engine=engine)

    def _parameters_provider(self, **kwargs) -> str:
        """ Callable that provides additional parameters for Parametrized Bash Operator related to this subdag

        Args:
            **kwargs: Airflow context

        Returns: additional parameters for Parametrized Bash Operator

        """
        self.log.info(f'kwargs: {kwargs}')
        ml_dag_id = dag_utils.get_ml_dag_id(self._parent_dag_id, **kwargs)

        self.log.info(f'ml_dag_id: {ml_dag_id}')

        parameters = [str(ml_dag_id),
                      'common_task_1_param_1',
                      'common_task_1_param_2']

        return ' '.join(parameters)
