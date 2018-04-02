import pytest
from airflow.utils.dates import days_ago

from dags.repositories.common_task_1 import CommonTask1Repository
from dags.repositories.ml_dag import MLDagRepository
from dags.subdags.base_subdag import MLTaskSubDag
from dags.utils import db_utils

INSERTED_ML_DAG_ID = 1
CHILD_DAG_NAME = 'bed_to_fa'
PARENT_DAG_NAME = 'test_dag'
DB_NAME = 'test.db'


# Service should be stateless, so widest scope is appropriate
@pytest.fixture(scope='module')
def common_task_1_repository() -> CommonTask1Repository:
    """ Fixture that makes CommonTask1Table Repository using local sqlite db """
    engine = db_utils.create_db_engine(login=None,
                                       password=None,
                                       host=DB_NAME,
                                       schema=None,
                                       conn_type='sqlite')

    return CommonTask1Repository(engine=engine)


@pytest.fixture
def reset_db(common_task_1_repository: CommonTask1Repository) -> None:
    """ Resets DB before each test to initial testing state """
    common_task_1_repository.metadata.drop_all()
    common_task_1_repository.metadata.create_all()

    MLDagRepository().table.insert().values(id=INSERTED_ML_DAG_ID, parameter_1='test_parameter_1').execute()

    common_task_1_repository.table.insert().values(ml_dag_id=INSERTED_ML_DAG_ID).execute()


@pytest.fixture(scope='module')
def subdag(common_task_1_repository: CommonTask1Repository) -> MLTaskSubDag:
    """ Fixture that make MLTaskSubDag

    Returns: MLTaskSubDag
    """
    return MLTaskSubDag(args={'owner': 'reljicd',
                              'start_date': days_ago(3)},
                        parent_dag_id=PARENT_DAG_NAME,
                        child_dag_id=CHILD_DAG_NAME,
                        repository_class=CommonTask1Repository,
                        engine=common_task_1_repository.metadata.bind)


def test_execute_or_skip_task(common_task_1_repository: CommonTask1Repository, subdag: MLTaskSubDag, reset_db: None):
    # Make sure task doesn't have datetime_finished
    datetime_finished = common_task_1_repository.table.select().where(
        common_task_1_repository.table.c.ml_dag_id == INSERTED_ML_DAG_ID).execute().first().datetime_finished

    assert datetime_finished is None

    # Conditional for task without datetime_finished should return 'start_task_in_db_{}'
    return_value = subdag._execute_or_skip_task(ml_dag_id=INSERTED_ML_DAG_ID)

    assert return_value == f'start_task_in_db_{CHILD_DAG_NAME}'

    # Let's finish task
    common_task_1_repository.finish_task(ml_dag_id=INSERTED_ML_DAG_ID)

    # Conditional for task with datetime_finished should return 'skip_{}'
    return_value = subdag._execute_or_skip_task(ml_dag_id=INSERTED_ML_DAG_ID)

    assert return_value == f'skip_{CHILD_DAG_NAME}'


def test_start_task(common_task_1_repository: CommonTask1Repository, subdag: MLTaskSubDag, reset_db: None):
    # Make sure task doesn't have datetime_started
    datetime_started = common_task_1_repository.table.select().where(
        common_task_1_repository.table.c.ml_dag_id == INSERTED_ML_DAG_ID).execute().first().datetime_started

    assert datetime_started is None

    # Let's start task through start_task_in_db callable
    subdag._start_task(ml_dag_id=INSERTED_ML_DAG_ID)

    # Now assert task has datetime_started
    datetime_started = common_task_1_repository.table.select().where(
        common_task_1_repository.table.c.ml_dag_id == INSERTED_ML_DAG_ID).execute().first().datetime_started

    assert datetime_started is not None


def test_finish_task(common_task_1_repository: CommonTask1Repository, subdag: MLTaskSubDag, reset_db: None):
    # Make sure task doesn't have datetime_finished
    datetime_finished = common_task_1_repository.table.select().where(
        common_task_1_repository.table.c.ml_dag_id == INSERTED_ML_DAG_ID).execute().first().datetime_finished

    assert datetime_finished is None

    # Let's finish task through start_task_in_db callable
    subdag._finish_task(ml_dag_id=INSERTED_ML_DAG_ID)

    # Now assert task has datetime_finished
    datetime_finished = common_task_1_repository.table.select().where(
        common_task_1_repository.table.c.ml_dag_id == INSERTED_ML_DAG_ID).execute().first().datetime_finished

    assert datetime_finished is not None
