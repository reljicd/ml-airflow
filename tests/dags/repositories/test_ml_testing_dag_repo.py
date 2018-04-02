import datetime

import pytest
from sqlalchemy import and_

from dags.exceptions.db_exception import DBException
from dags.repositories.ml_dag import MLDagRepository, MLDagRow
from dags.repositories.ml_testing_dag import MLTestingDagRepository, MLTestingDagRow
from dags.utils import db_utils

INSERTED_ML_DAG_ID = 1
PARAMETER_1 = 'test_parameter_1'
PARAMETER_3 = 'test_parameter_3'
NON_EXISTENT_ML_DAG_ID = 100
NON_EXISTENT_PARAMETER_1 = 'non_existent_test_parameter_1'
NON_EXISTENT_PARAMETER_3 = 'non_existent_test_parameter_1'
DB_NAME = 'test.db'


# Service should be stateless, so widest scope is appropriate
@pytest.fixture(scope='module')
def ml_testing_dag_repository() -> MLTestingDagRepository:
    """ Fixture that makes MLTestingDag using local sqlite db """
    engine = db_utils.create_db_engine(login=None,
                                       password=None,
                                       host=DB_NAME,
                                       schema=None,
                                       conn_type='sqlite')

    return MLTestingDagRepository(engine=engine)


@pytest.fixture()
def reset_db(ml_testing_dag_repository: MLTestingDagRepository) -> None:
    """ Resets DB before each test to initial testing state """
    ml_testing_dag_repository.metadata.drop_all()
    ml_testing_dag_repository.metadata.create_all()

    MLDagRepository().table.insert().values(id=INSERTED_ML_DAG_ID, parameter_1=PARAMETER_1).execute()


def test_save(ml_testing_dag_repository: MLTestingDagRepository, reset_db: None):
    ml_testing_dag = ml_testing_dag_repository.table.select().where(
        and_(ml_testing_dag_repository.table.c.ml_dag_id == INSERTED_ML_DAG_ID,
             ml_testing_dag_repository.table.c.parameter_3 == PARAMETER_3)).execute().first()
    assert ml_testing_dag is None

    ml_testing_dag_repository.save(
        MLTestingDagRow(id=None,
                        ml_dag=MLDagRow(
                            id=INSERTED_ML_DAG_ID,
                            parameter_1=PARAMETER_1),
                        parameter_3=PARAMETER_3))

    ml_testing_dag = ml_testing_dag_repository.table.select().where(
        and_(ml_testing_dag_repository.table.c.ml_dag_id == INSERTED_ML_DAG_ID,
             ml_testing_dag_repository.table.c.parameter_3 == PARAMETER_3)).execute().first()
    assert ml_testing_dag is not None

    # Should raise exception for existing INSERTED_ML_DAG_ID
    with pytest.raises(DBException) as e_info:
        ml_testing_dag_repository.save(
            MLTestingDagRow(id=None,
                            ml_dag=MLDagRow(
                                id=INSERTED_ML_DAG_ID,
                                parameter_1=PARAMETER_1),
                            parameter_3=PARAMETER_3))
    assert str(INSERTED_ML_DAG_ID) in str(e_info.value)


def test_find_by_parameters(ml_testing_dag_repository: MLTestingDagRepository, reset_db: None):
    ml_testing_dag_repository.table.insert().values(ml_dag_id=INSERTED_ML_DAG_ID,
                                                    parameter_3=PARAMETER_3,
                                                    datetime_created=datetime.datetime.utcnow()).execute()

    ml_testing_dag_tuple = ml_testing_dag_repository.find_by_parameters(
        parameter_1=PARAMETER_1,
        parameter_3=PARAMETER_3)

    assert ml_testing_dag_tuple.ml_dag.parameter_1 == PARAMETER_1
    assert ml_testing_dag_tuple.parameter_3 == PARAMETER_3

    # Should raise exception for NON_EXISTENT_PARAMETER_1
    with pytest.raises(DBException) as e_info:
        ml_testing_dag_repository.find_by_parameters(
            parameter_1=NON_EXISTENT_PARAMETER_1,
            parameter_3=PARAMETER_3)
    assert str(NON_EXISTENT_PARAMETER_1) in str(e_info.value)

    # Should raise exception for NON_EXISTENT_PARAMETER_3
    with pytest.raises(DBException) as e_info:
        ml_testing_dag_repository.find_by_parameters(
            parameter_1=PARAMETER_1,
            parameter_3=NON_EXISTENT_PARAMETER_3)
    assert str(NON_EXISTENT_PARAMETER_3) in str(e_info.value)
