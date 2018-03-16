import datetime

import pytest

from dags.exceptions.db_exception import DBException
from dags.repositories.base_db import BaseDatabase
from dags.repositories.ml_dag import MLDagTable, MLDagRowTuple

ML_DAG_1 = MLDagRowTuple(id=1, parameter_1='test_parameter_1')
NON_EXISTENT_ML_DAG_ID = 100
NON_EXISTENT_PARAMETER_1 = 'non_existent_parameter_1'
DB_NAME = 'test.db'


# Service should be stateless, so widest scope is appropriate
@pytest.fixture(scope='module')
def ml_dag_repository_fixture() -> MLDagTable:
    """ Fixture that makes MLDagTable using local sqlite db

    Returns: MLDagTable

    """
    engine = BaseDatabase.create_engine(login=None,
                                        password=None,
                                        host=DB_NAME,
                                        schema=None,
                                        conn_type='sqlite')

    return MLDagTable(engine=engine)


@pytest.fixture()
def reset_db(ml_dag_repository_fixture: MLDagTable) -> None:
    """ Resets DB before each test to initial testing state """
    ml_dag_repository_fixture.metadata.drop_all()
    ml_dag_repository_fixture.metadata.create_all()


def test_insert_dag(ml_dag_repository_fixture: MLDagTable, reset_db: None):
    ml_dag = ml_dag_repository_fixture.table.select().where(
        ml_dag_repository_fixture.table.c.parameter_1 == ML_DAG_1.parameter_1).execute().first()
    assert ml_dag is None

    dag_tuple = ml_dag_repository_fixture.insert_ml_dag(
        MLDagRowTuple(id=None, parameter_1=ML_DAG_1.parameter_1))
    assert dag_tuple == ML_DAG_1

    ml_dag = ml_dag_repository_fixture.table.select().where(
        ml_dag_repository_fixture.table.c.parameter_1 == ML_DAG_1.parameter_1).execute().first()
    assert ml_dag is not None


def test_select_ml_dag_for_id(ml_dag_repository_fixture: MLDagTable, reset_db: None):
    ml_dag_repository_fixture.table.insert().values(id=ML_DAG_1.id,
                                                    parameter_1=ML_DAG_1.parameter_1,
                                                    datetime_created=datetime.datetime.utcnow()).execute()

    ml_dag_tuple = ml_dag_repository_fixture.select_ml_dag_for_id(ml_dag_id=ML_DAG_1.id)

    assert ml_dag_tuple.id == ML_DAG_1.id

    # Should raise exception for NON_EXISTENT_ML_DAG_ID
    with pytest.raises(DBException) as e_info:
        ml_dag_repository_fixture.select_ml_dag_for_id(ml_dag_id=NON_EXISTENT_ML_DAG_ID)
    assert str(NON_EXISTENT_ML_DAG_ID) in str(e_info.value)


def test_select_dag_for_parameter_1(ml_dag_repository_fixture: MLDagTable, reset_db: None):
    ml_dag_repository_fixture.table.insert().values(parameter_1=ML_DAG_1.parameter_1,
                                                    datetime_created=datetime.datetime.utcnow()).execute()

    ml_dag_tuple = ml_dag_repository_fixture.select_ml_dag_for_parameter_1(parameter_1=ML_DAG_1.parameter_1)

    assert ml_dag_tuple.parameter_1 == ML_DAG_1.parameter_1

    # Should raise exception for NON_EXISTENT_PARAMETER_1
    with pytest.raises(DBException) as e_info:
        ml_dag_repository_fixture.select_ml_dag_for_parameter_1(parameter_1=NON_EXISTENT_PARAMETER_1)
    assert NON_EXISTENT_PARAMETER_1 in str(e_info.value)


def test_check_ml_dag_id(ml_dag_repository_fixture: MLDagTable, reset_db: None):
    # Should raise exception for NON_EXISTENT_ML_DAG_ID
    with pytest.raises(DBException) as e_info:
        assert ml_dag_repository_fixture.check_ml_dag_id(ml_dag_id=NON_EXISTENT_ML_DAG_ID)
    assert str(NON_EXISTENT_ML_DAG_ID) in str(e_info.value)
