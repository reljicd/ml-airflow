import pytest
from airflow.utils.dates import days_ago

from dags.repositories.base_db import BaseDatabase
from dags.repositories.common_task_1 import CommonTask1Table
from dags.repositories.ml_dag import MLDagTable
from dags.subdags.base_subdag import MLTaskSubDag

INSERTED_ML_DAG_ID = 1
CHILD_DAG_NAME = 'bed_to_fa'
PARENT_DAG_NAME = 'test_dag'
DB_NAME = 'test.db'


# Service should be stateless, so widest scope is appropriate
@pytest.fixture(scope='module')
def common_task_1_repository_fixture() -> CommonTask1Table:
    """ Fixture that makes CommonTask1Table Repository using local sqlite db

    Returns: CommonTask1Table

    """
    engine = BaseDatabase.create_engine(login=None,
                                        password=None,
                                        host=DB_NAME,
                                        schema=None,
                                        conn_type='sqlite')

    return CommonTask1Table(engine=engine)


@pytest.fixture
def reset_db(common_task_1_repository_fixture: CommonTask1Table) -> None:
    """ Resets DB before each test to initial testing state """
    common_task_1_repository_fixture.metadata.drop_all()
    common_task_1_repository_fixture.metadata.create_all()

    MLDagTable().table.insert().values(id=INSERTED_ML_DAG_ID, parameter_1='test_parameter_1').execute()

    common_task_1_repository_fixture.table.insert().values(ml_dag_id=INSERTED_ML_DAG_ID).execute()


@pytest.fixture(scope='module')
def subdag(common_task_1_repository_fixture: CommonTask1Table) -> MLTaskSubDag:
    """ Fixture that make MLTaskSubDag

    Returns: MLTaskSubDag
    """
    return MLTaskSubDag(args={'owner': 'reljicd',
                              'start_date': days_ago(3)},
                        parent_dag_id=PARENT_DAG_NAME,
                        child_dag_id=CHILD_DAG_NAME,
                        repository_class=CommonTask1Table,
                        engine=common_task_1_repository_fixture.metadata.bind)


def test_execute_or_skip_task(common_task_1_repository_fixture: CommonTask1Table, subdag: MLTaskSubDag, reset_db: None):
    # Make sure task doesn't have datetime_finished
    datetime_finished = common_task_1_repository_fixture.table.select().where(
        common_task_1_repository_fixture.table.c.ml_dag_id == INSERTED_ML_DAG_ID).execute().first().datetime_finished

    assert datetime_finished is None

    # Conditional for task without datetime_finished should return 'start_task_in_db_{}'
    return_value = subdag._execute_or_skip_task(ml_dag_id=INSERTED_ML_DAG_ID)

    assert return_value == f'start_task_in_db_{CHILD_DAG_NAME}'

    # Let's finish task
    common_task_1_repository_fixture.finish_task(ml_dag_id=INSERTED_ML_DAG_ID)

    # Conditional for task with datetime_finished should return 'skip_{}'
    return_value = subdag._execute_or_skip_task(ml_dag_id=INSERTED_ML_DAG_ID)

    assert return_value == f'skip_{CHILD_DAG_NAME}'


def test_start_task_in_db(common_task_1_repository_fixture: CommonTask1Table, subdag: MLTaskSubDag, reset_db: None):
    # Make sure task doesn't have datetime_started
    datetime_started = common_task_1_repository_fixture.table.select().where(
        common_task_1_repository_fixture.table.c.ml_dag_id == INSERTED_ML_DAG_ID).execute().first().datetime_started

    assert datetime_started is None

    # Let's start task through start_task_in_db callable
    subdag._start_task_in_db(ml_dag_id=INSERTED_ML_DAG_ID)

    # Now assert task has datetime_started
    datetime_started = common_task_1_repository_fixture.table.select().where(
        common_task_1_repository_fixture.table.c.ml_dag_id == INSERTED_ML_DAG_ID).execute().first().datetime_started

    assert datetime_started is not None


def test_finish_task_in_db(common_task_1_repository_fixture: CommonTask1Table, subdag: MLTaskSubDag, reset_db: None):
    # Make sure task doesn't have datetime_finished
    datetime_finished = common_task_1_repository_fixture.table.select().where(
        common_task_1_repository_fixture.table.c.ml_dag_id == INSERTED_ML_DAG_ID).execute().first().datetime_finished

    assert datetime_finished is None

    # Let's finish task through start_task_in_db callable
    subdag._finish_task_in_db(ml_dag_id=INSERTED_ML_DAG_ID)

    # Now assert task has datetime_finished
    datetime_finished = common_task_1_repository_fixture.table.select().where(
        common_task_1_repository_fixture.table.c.ml_dag_id == INSERTED_ML_DAG_ID).execute().first().datetime_finished

    assert datetime_finished is not None
