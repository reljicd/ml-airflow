# airflow <- this word needs to be present in order for this file to be parsed
import sqlalchemy.engine
from airflow.operators.subdag_operator import SubDagOperator

from dags.base_ml_dag import BaseMLDAG
from dags.exceptions.db_exception import DBException
from dags.repositories.ml_dag import MLDagTable, MLDagRowTuple
from dags.repositories.ml_training_dag import MLTrainingDagRowTuple, MLTrainingDagTable
from dags.repositories.training_task_1 import TrainingTask1Table
from dags.subdags.base_subdag import MLTaskSubDag


class MLTrainingDag(BaseMLDAG):
    DAG_NAME = 'ml_training_dag'

    def __init__(self):
        super().__init__(dag_name=self.DAG_NAME)

        self._training_task_2_subdag = SubDagOperator(task_id='training_task_2',
                                                      subdag=MLTaskSubDag(args=self._args,
                                                                          parent_dag_id=self.DAG_NAME,
                                                                          child_dag_id='training_task_2',
                                                                          repository_class=TrainingTask1Table).build(),
                                                      default_args=self._args,
                                                      dag=self)

        (self._initializer >>
         self._common_task_1_subdag >>
         self._common_task_2_subdag >>
         self._training_task_2_subdag)

    @staticmethod
    def _get_ml_dag_id(engine: sqlalchemy.engine.Engine, **kwargs) -> str:
        parameter_1 = kwargs['dag_run'].conf['parameter_1']
        parameter_2 = kwargs['dag_run'].conf['parameter_2']

        # Get ml_testing_dag for parameter_1 and parameter_2 if exists,
        # or insert new ml_dag (if it doesnt exist for parameter_1) and ml_testing_dag
        try:
            ml_training_dag = MLTrainingDagTable(engine=engine).select_ml_training_dag_for_parameters(
                parameter_1=parameter_1,
                parameter_2=parameter_2)
        except DBException:
            try:
                ml_dag = MLDagTable(engine=engine).select_ml_dag_for_parameter_1(parameter_1=parameter_1)
            except DBException:
                ml_dag = MLDagTable(engine=engine).insert_ml_dag(MLDagRowTuple(id=None,
                                                                               parameter_1=parameter_1))

            ml_training_dag = MLTrainingDagTable(engine=engine).insert_ml_training_dag(
                MLTrainingDagRowTuple(id=None,
                                      ml_dag=ml_dag,
                                      parameter_2=parameter_2))

        return ml_training_dag.ml_dag.id


dag = MLTrainingDag()
