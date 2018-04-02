import datetime

import pytest

from dags.exceptions.db_exception import DBException
from dags.repositories.ml_dag import MLDagRepository, MLDagRow
from dags.utils import db_utils

ML_DAG_1 = MLDagRow(id=1, parameter_1='test_parameter_1')
NON_EXISTENT_ML_DAG_ID = 100
NON_EXISTENT_PARAMETER_1 = 'non_existent_parameter_1'
DB_NAME = 'test.db'


# Service should be stateless, so widest scope is appropriate
@pytest.fixture(scope='module')
def ml_dag_repository() -> MLDagRepository:
    """ Fixture that makes MLDagTable using local sqlite db """
    engine = db_utils.create_db_engine(login=None,
                                       password=None,
                                       host=DB_NAME,
                                       schema=None,
                                       conn_type='sqlite')

    return MLDagRepository(engine=engine)


@pytest.fixture()
def reset_db(ml_dag_repository: MLDagRepository) -> None:
    """ Resets DB before each test to initial testing state """
    ml_dag_repository.metadata.drop_all()
    ml_dag_repository.metadata.create_all()


def test_insert_dag(ml_dag_repository: MLDagRepository, reset_db: None):
    ml_dag = ml_dag_repository.table.select().where(
        ml_dag_repository.table.c.parameter_1 == ML_DAG_1.parameter_1).execute().first()
    assert ml_dag is None

    dag_tuple = ml_dag_repository.save(
        MLDagRow(id=None, parameter_1=ML_DAG_1.parameter_1))
    assert dag_tuple == ML_DAG_1

    ml_dag = ml_dag_repository.table.select().where(
        ml_dag_repository.table.c.parameter_1 == ML_DAG_1.parameter_1).execute().first()
    assert ml_dag is not None


def test_find_by_id(ml_dag_repository: MLDagRepository, reset_db: None):
    ml_dag_repository.table.insert().values(id=ML_DAG_1.id,
                                            parameter_1=ML_DAG_1.parameter_1,
                                            datetime_created=datetime.datetime.utcnow()).execute()

    ml_dag_tuple = ml_dag_repository.find_by_id(id=ML_DAG_1.id)

    assert ml_dag_tuple.id == ML_DAG_1.id

    # Should raise exception for NON_EXISTENT_ML_DAG_ID
    with pytest.raises(DBException) as e_info:
        ml_dag_repository.find_by_id(id=NON_EXISTENT_ML_DAG_ID)
    assert str(NON_EXISTENT_ML_DAG_ID) in str(e_info.value)


def test_find_by_parameter_1(ml_dag_repository: MLDagRepository, reset_db: None):
    ml_dag_repository.table.insert().values(parameter_1=ML_DAG_1.parameter_1,
                                            datetime_created=datetime.datetime.utcnow()).execute()

    ml_dag_tuple = ml_dag_repository.find_by_parameter_1(parameter_1=ML_DAG_1.parameter_1)

    assert ml_dag_tuple.parameter_1 == ML_DAG_1.parameter_1

    # Should raise exception for NON_EXISTENT_PARAMETER_1
    with pytest.raises(DBException) as e_info:
        ml_dag_repository.find_by_parameter_1(parameter_1=NON_EXISTENT_PARAMETER_1)
    assert NON_EXISTENT_PARAMETER_1 in str(e_info.value)


def test_check_ml_dag_id(ml_dag_repository: MLDagRepository, reset_db: None):
    # Should raise exception for NON_EXISTENT_ML_DAG_ID
    with pytest.raises(DBException) as e_info:
        assert ml_dag_repository.check_ml_dag_id(ml_dag_id=NON_EXISTENT_ML_DAG_ID)
    assert str(NON_EXISTENT_ML_DAG_ID) in str(e_info.value)
