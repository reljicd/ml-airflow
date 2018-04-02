# airflow <- this word needs to be present in order for this file to be parsed
import sqlalchemy.engine
from airflow.operators.subdag_operator import SubDagOperator

from dags.base_ml_dag import BaseMLDAG
from dags.exceptions.db_exception import DBException
from dags.repositories.ml_dag import MLDagRepository, MLDagRow
from dags.repositories.ml_testing_dag import MLTestingDagRepository, MLTestingDagRow
from dags.repositories.testing_taks_1 import TestingTask1Repository
from dags.repositories.testing_taks_2 import TestingTask2Repository
from dags.subdags.base_subdag import MLTaskSubDag


class MLTestingDag(BaseMLDAG):
    DAG_NAME = 'ml_testing_dag'

    def __init__(self):
        super().__init__(dag_name=self.DAG_NAME)

        self._testing_task_1_subdag = SubDagOperator(task_id='testing_task_1',
                                                     subdag=MLTaskSubDag(args=self._args,
                                                                         parent_dag_id=self.DAG_NAME,
                                                                         child_dag_id='testing_task_1',
                                                                         repository_class=TestingTask1Repository).build(),
                                                     default_args=self._args,
                                                     dag=self)

        self._testing_task_2_subdag = SubDagOperator(task_id='testing_task_2',
                                                     subdag=MLTaskSubDag(args=self._args,
                                                                         parent_dag_id=self.DAG_NAME,
                                                                         child_dag_id='testing_task_2',
                                                                         repository_class=TestingTask2Repository).build(),
                                                     default_args=self._args,
                                                     dag=self)

        (self._initializer >>
         self._common_task_1_subdag >>
         self._common_task_2_subdag >>
         self._testing_task_1_subdag >>
         self._testing_task_2_subdag)

    @staticmethod
    def _get_ml_dag_id(engine: sqlalchemy.engine.Engine, **kwargs) -> int:
        parameter_1 = kwargs['dag_run'].conf['parameter_1']
        parameter_3 = kwargs['dag_run'].conf['parameter_3']

        # Get ml_testing_dag for parameter_1 and parameter_3 if exists,
        # or insert new ml_dag (if it doesnt exist for parameter_1) and ml_testing_dag
        try:
            ml_testing_dag = MLTestingDagRepository(engine=engine).find_by_parameters(
                parameter_1=parameter_1,
                parameter_3=parameter_3)
        except DBException:
            try:
                ml_dag = MLDagRepository(engine=engine).find_by_parameter_1(parameter_1=parameter_1)
            except DBException:
                ml_dag = MLDagRepository(engine=engine).save(MLDagRow(id=None,
                                                                      parameter_1=parameter_1))

            ml_testing_dag = MLTestingDagRepository(engine=engine).save(
                MLTestingDagRow(id=None,
                                ml_dag=ml_dag,
                                parameter_3=parameter_3))

        return ml_testing_dag.ml_dag.id


dag = MLTestingDag()
