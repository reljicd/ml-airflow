import datetime
from collections import namedtuple

import pytest
from sqlalchemy import and_

from dags.ml_testing_dag import MLTestingDag
from dags.repositories.base_db import BaseDatabase
from dags.repositories.ml_dag import MLDagTable
from dags.repositories.ml_testing_dag import MLTestingDagTable

ConfTuple = namedtuple('ConfTuple', 'conf')

INSERTED_ML_DAG_ID = 1
INSERTED_ML_TESTING_DAG_ID = 1
PARAMETER_1 = 'test_parameter_1'
PARAMETER_3 = 'test_parameter_3'
NON_EXISTENT_ML_DAG_ID = 100
NON_EXISTENT_PARAMETER_3 = 'non_existent_parameter_3'
NON_EXISTENT_PARAMETER_1 = 'non_existent_parameter_1'
DB_NAME = 'test.db'


# Service should be stateless, so widest scope is appropriate
@pytest.fixture(scope='module')
def ml_testing_dag_repository_fixture() -> MLTestingDagTable:
    """ Fixture that makes MLTestingDagTable using local sqlite db

    Returns: MLTestingDagTable

    """
    engine = BaseDatabase.create_engine(login=None,
                                        password=None,
                                        host=DB_NAME,
                                        schema=None,
                                        conn_type='sqlite')

    return MLTestingDagTable(engine=engine)


@pytest.fixture()
def reset_db(ml_testing_dag_repository_fixture: MLTestingDagTable) -> None:
    """ Resets DB before each test to initial testing state """
    ml_testing_dag_repository_fixture.metadata.drop_all()
    ml_testing_dag_repository_fixture.metadata.create_all()


def test_get_ml_dag_id(ml_testing_dag_repository_fixture: MLTestingDagTable, reset_db: None):
    MLDagTable().table.insert().values(id=INSERTED_ML_DAG_ID, parameter_1=PARAMETER_1).execute()

    ml_testing_dag_repository_fixture.table.insert().values(id=INSERTED_ML_TESTING_DAG_ID,
                                                            ml_dag_id=INSERTED_ML_DAG_ID,
                                                            parameter_3=PARAMETER_3,
                                                            datetime_created=datetime.datetime.utcnow()).execute()

    ml_dag_id = MLTestingDag._get_ml_dag_id(engine=ml_testing_dag_repository_fixture.metadata.bind,
                                            dag_run=ConfTuple(conf={'parameter_1': PARAMETER_1,
                                                                    'parameter_3': PARAMETER_3}))

    assert ml_dag_id == INSERTED_ML_DAG_ID


def test_get_dag_run_id_insert_dag_run_and_testing_dag_run(ml_testing_dag_repository_fixture: MLTestingDagTable,
                                                           reset_db: None):
    """ Test creation of both MLDagTable and MLTestingDagTable """
    ml_dag = MLDagTable().table.select().where(
        and_(MLDagTable.table.c.id == INSERTED_ML_DAG_ID,
             MLDagTable.table.c.parameter_1 == PARAMETER_1)).execute().first()
    assert ml_dag is None

    ml_testing_dag = ml_testing_dag_repository_fixture.table.select().where(
        ml_testing_dag_repository_fixture.table.c.parameter_3 == PARAMETER_3).execute().first()
    assert ml_testing_dag is None

    MLTestingDag._get_ml_dag_id(engine=ml_testing_dag_repository_fixture.metadata.bind,
                                dag_run=ConfTuple(conf={'parameter_1': PARAMETER_1,
                                                        'parameter_3': PARAMETER_3}))

    ml_dag = MLDagTable().table.select().where(
        and_(MLDagTable.table.c.id == INSERTED_ML_DAG_ID,
             MLDagTable.table.c.parameter_1 == PARAMETER_1)).execute().first()
    assert ml_dag is not None

    ml_testing_dag = ml_testing_dag_repository_fixture.table.select().where(
        ml_testing_dag_repository_fixture.table.c.parameter_3 == PARAMETER_3).execute().first()
    assert ml_testing_dag is not None


def test_get_dag_run_id_insert_testing_dag_run(ml_testing_dag_repository_fixture: MLTestingDagTable, reset_db: None):
    """ Test creation of MLTestingDagTable for existing MLDagTable """
    MLDagTable().table.insert().values(id=INSERTED_ML_DAG_ID, parameter_1=PARAMETER_1).execute()

    ml_testing_dag = ml_testing_dag_repository_fixture.table.select().where(
        ml_testing_dag_repository_fixture.table.c.parameter_3 == PARAMETER_3).execute().first()
    assert ml_testing_dag is None

    MLTestingDag._get_ml_dag_id(engine=ml_testing_dag_repository_fixture.metadata.bind,
                                dag_run=ConfTuple(conf={'parameter_1': PARAMETER_1,
                                                        'parameter_3': PARAMETER_3}))

    ml_testing_dag = ml_testing_dag_repository_fixture.table.select().where(
        ml_testing_dag_repository_fixture.table.c.parameter_3 == PARAMETER_3).execute().first()
    assert ml_testing_dag is not None
