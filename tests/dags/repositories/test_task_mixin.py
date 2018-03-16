import pytest

from dags.exceptions.db_exception import DBException
from dags.repositories.base_db import BaseDatabase
from dags.repositories.common_task_1 import CommonTask1Table
from dags.repositories.ml_dag import MLDagTable

INSERTED_ML_DAG_ID = 1
NON_EXISTENT_ML_DAG_RUN_ID = 100
DB_NAME = 'test.db'


# Service should be stateless, so widest scope is appropriate
@pytest.fixture(scope='module')
def common_task_1_repository_fixture() -> CommonTask1Table:
    """ Fixture that makes CommonTask1Table Repository using engine_fixture

    Returns: bed_to_fa_repository

    """
    engine = BaseDatabase.create_engine(login=None,
                                        password=None,
                                        host=DB_NAME,
                                        schema=None,
                                        conn_type='sqlite')

    return CommonTask1Table(engine=engine)


@pytest.fixture
def reset_db(common_task_1_repository_fixture: CommonTask1Table) -> None:
    """ Fixture that make another dag_run and bed_to_fa for it

    Args:
        common_task_1_repository_fixture:

    Returns: ml_dag_id

    """
    common_task_1_repository_fixture.metadata.drop_all()
    common_task_1_repository_fixture.metadata.create_all()

    MLDagTable().table.insert().values(id=INSERTED_ML_DAG_ID, parameter_1='test_parameter_1').execute()
    common_task_1_repository_fixture.table.insert().values(ml_dag_id=INSERTED_ML_DAG_ID).execute()


def test_insert_task_with_ml_dag_id(common_task_1_repository_fixture: CommonTask1Table, reset_db: None):
    common_task_1 = common_task_1_repository_fixture.table.select().where(
        common_task_1_repository_fixture.table.c.ml_dag_id == NON_EXISTENT_ML_DAG_RUN_ID).execute().first()
    assert common_task_1 is None

    common_task_1_repository_fixture.insert_task_with_ml_dag_id(ml_dag_id=NON_EXISTENT_ML_DAG_RUN_ID)

    common_task_1 = common_task_1_repository_fixture.table.select().where(
        common_task_1_repository_fixture.table.c.ml_dag_id == NON_EXISTENT_ML_DAG_RUN_ID).execute().first()
    assert common_task_1 is not None

    # Should raise exception for UNIQUE (ml_dag_id) constraint
    with pytest.raises(DBException) as e_info:
        common_task_1_repository_fixture.insert_task_with_ml_dag_id(ml_dag_id=NON_EXISTENT_ML_DAG_RUN_ID)
    assert str(NON_EXISTENT_ML_DAG_RUN_ID) in str(e_info.value)


def test_is_task_finished(common_task_1_repository_fixture: CommonTask1Table, reset_db: None):
    assert common_task_1_repository_fixture.is_task_finished(ml_dag_id=INSERTED_ML_DAG_ID) is False
    common_task_1_repository_fixture.finish_task(ml_dag_id=INSERTED_ML_DAG_ID)
    assert common_task_1_repository_fixture.is_task_finished(ml_dag_id=INSERTED_ML_DAG_ID) is True


def test_start_task(common_task_1_repository_fixture: CommonTask1Table, reset_db: None):
    # Make sure task doesn't have datetime_started
    datetime_started = common_task_1_repository_fixture.table.select().where(
        common_task_1_repository_fixture.table.c.ml_dag_id == INSERTED_ML_DAG_ID).execute().first().datetime_started

    assert datetime_started is None

    # Start task
    common_task_1_repository_fixture.start_task(ml_dag_id=INSERTED_ML_DAG_ID)

    # Make sure task now has datetime_started
    datetime_started = common_task_1_repository_fixture.table.select().where(
        common_task_1_repository_fixture.table.c.ml_dag_id == INSERTED_ML_DAG_ID).execute().first().datetime_started

    assert datetime_started is not None


def test_finish_task(common_task_1_repository_fixture: CommonTask1Table, reset_db: None):
    assert common_task_1_repository_fixture.is_task_finished(ml_dag_id=INSERTED_ML_DAG_ID) is False
    common_task_1_repository_fixture.finish_task(ml_dag_id=INSERTED_ML_DAG_ID)
    assert common_task_1_repository_fixture.is_task_finished(ml_dag_id=INSERTED_ML_DAG_ID) is True


def test_check_task_with_ml_dag_id(common_task_1_repository_fixture: CommonTask1Table):
    # Should raise exception for NON_EXISTENT_ML_DAG_RUN_ID
    with pytest.raises(DBException) as e_info:
        assert common_task_1_repository_fixture._check_task_with_ml_dag_id(ml_dag_id=NON_EXISTENT_ML_DAG_RUN_ID)
    assert str(NON_EXISTENT_ML_DAG_RUN_ID) in str(e_info.value)
