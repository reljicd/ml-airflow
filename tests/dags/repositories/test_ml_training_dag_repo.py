import datetime

import pytest
from sqlalchemy import and_

from dags.exceptions.db_exception import DBException
from dags.repositories.ml_dag import MLDagRepository, MLDagRow
from dags.repositories.ml_training_dag import MLTrainingDagRepository, MLTrainingDagRow
from dags.utils import db_utils

INSERTED_ML_DAG_ID = 1
PARAMETER_1 = 'test_parameter_1'
PARAMETER_2 = 'test_parameter_2'
NON_EXISTENT_ML_DAG_ID = 100
NON_EXISTENT_PARAMETER_1 = 'non_existent_test_parameter_1'
NON_EXISTENT_PARAMETER_2 = 'non_existent_test_parameter_1'
DB_NAME = 'test.db'


# Service should be stateless, so widest scope is appropriate
@pytest.fixture(scope='module')
def ml_training_dag_repository() -> MLTrainingDagRepository:
    """ Fixture that makes MLTrainingDag using local sqlite db """
    engine = db_utils.create_db_engine(login=None,
                                       password=None,
                                       host=DB_NAME,
                                       schema=None,
                                       conn_type='sqlite')

    return MLTrainingDagRepository(engine=engine)


@pytest.fixture()
def reset_db(ml_training_dag_repository: MLTrainingDagRepository) -> None:
    """ Resets DB before each test to initial testing state """
    ml_training_dag_repository.metadata.drop_all()
    ml_training_dag_repository.metadata.create_all()

    MLDagRepository().table.insert().values(id=INSERTED_ML_DAG_ID, parameter_1=PARAMETER_1).execute()


def test_save(ml_training_dag_repository: MLTrainingDagRepository, reset_db: None):
    ml_training_dag = ml_training_dag_repository.table.select().where(
        and_(ml_training_dag_repository.table.c.ml_dag_id == INSERTED_ML_DAG_ID,
             ml_training_dag_repository.table.c.parameter_2 == PARAMETER_2)).execute().first()
    assert ml_training_dag is None

    ml_training_dag_repository.save(
        MLTrainingDagRow(id=None,
                         ml_dag=MLDagRow(
                             id=INSERTED_ML_DAG_ID,
                             parameter_1=PARAMETER_1),
                         parameter_2=PARAMETER_2))

    ml_training_dag = ml_training_dag_repository.table.select().where(
        and_(ml_training_dag_repository.table.c.ml_dag_id == INSERTED_ML_DAG_ID,
             ml_training_dag_repository.table.c.parameter_2 == PARAMETER_2)).execute().first()
    assert ml_training_dag is not None

    # Should raise exception for existing INSERTED_ML_DAG_ID
    with pytest.raises(DBException) as e_info:
        ml_training_dag_repository.save(
            MLTrainingDagRow(id=None,
                             ml_dag=MLDagRow(
                                 id=INSERTED_ML_DAG_ID,
                                 parameter_1=PARAMETER_1),
                             parameter_2=PARAMETER_2))
    assert str(INSERTED_ML_DAG_ID) in str(e_info.value)


def test_find_by_parameters(ml_training_dag_repository: MLTrainingDagRepository, reset_db: None):
    ml_training_dag_repository.table.insert().values(ml_dag_id=INSERTED_ML_DAG_ID,
                                                     parameter_2=PARAMETER_2,
                                                     datetime_created=datetime.datetime.utcnow()).execute()

    ml_training_dag_tuple = ml_training_dag_repository.find_by_parameters(
        parameter_1=PARAMETER_1,
        parameter_2=PARAMETER_2)

    assert ml_training_dag_tuple.ml_dag.parameter_1 == PARAMETER_1
    assert ml_training_dag_tuple.parameter_2 == PARAMETER_2

    # Should raise exception for NON_EXISTENT_PARAMETER_1
    with pytest.raises(DBException) as e_info:
        ml_training_dag_repository.find_by_parameters(
            parameter_1=NON_EXISTENT_PARAMETER_1,
            parameter_2=PARAMETER_2)
    assert str(NON_EXISTENT_PARAMETER_1) in str(e_info.value)

    # Should raise exception for NON_EXISTENT_PARAMETER_2
    with pytest.raises(DBException) as e_info:
        ml_training_dag_repository.find_by_parameters(
            parameter_1=PARAMETER_1,
            parameter_2=NON_EXISTENT_PARAMETER_2)
    assert str(NON_EXISTENT_PARAMETER_2) in str(e_info.value)
