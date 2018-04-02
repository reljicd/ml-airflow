import datetime
from collections import namedtuple

import pytest
from sqlalchemy import and_

from dags.ml_testing_dag import MLTestingDag
from dags.repositories.ml_dag import MLDagRepository
from dags.repositories.ml_testing_dag import MLTestingDagRepository
from dags.utils import db_utils

Conf = namedtuple('Conf', 'conf')

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
def ml_testing_dag_repository() -> MLTestingDagRepository:
    """ Fixture that makes MLTestingDagTable using local sqlite db """
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


def test_get_ml_dag_id(ml_testing_dag_repository: MLTestingDagRepository, reset_db: None):
    MLDagRepository().table.insert().values(id=INSERTED_ML_DAG_ID, parameter_1=PARAMETER_1).execute()

    ml_testing_dag_repository.table.insert().values(id=INSERTED_ML_TESTING_DAG_ID,
                                                    ml_dag_id=INSERTED_ML_DAG_ID,
                                                    parameter_3=PARAMETER_3,
                                                    datetime_created=datetime.datetime.utcnow()).execute()

    ml_dag_id = MLTestingDag._get_ml_dag_id(engine=ml_testing_dag_repository.metadata.bind,
                                            dag_run=Conf(conf={'parameter_1': PARAMETER_1,
                                                               'parameter_3': PARAMETER_3}))

    assert ml_dag_id == INSERTED_ML_DAG_ID


def test_get_ml_dag_id_insert_dag_run_and_testing_dag_run(ml_testing_dag_repository: MLTestingDagRepository,
                                                          reset_db: None):
    """ Test creation of both MLDagTable and MLTestingDagTable """
    ml_dag = MLDagRepository().table.select().where(
        and_(MLDagRepository.table.c.id == INSERTED_ML_DAG_ID,
             MLDagRepository.table.c.parameter_1 == PARAMETER_1)).execute().first()
    assert ml_dag is None

    ml_testing_dag = ml_testing_dag_repository.table.select().where(
        ml_testing_dag_repository.table.c.parameter_3 == PARAMETER_3).execute().first()
    assert ml_testing_dag is None

    MLTestingDag._get_ml_dag_id(engine=ml_testing_dag_repository.metadata.bind,
                                dag_run=Conf(conf={'parameter_1': PARAMETER_1,
                                                   'parameter_3': PARAMETER_3}))

    ml_dag = MLDagRepository().table.select().where(
        and_(MLDagRepository.table.c.id == INSERTED_ML_DAG_ID,
             MLDagRepository.table.c.parameter_1 == PARAMETER_1)).execute().first()
    assert ml_dag is not None

    ml_testing_dag = ml_testing_dag_repository.table.select().where(
        ml_testing_dag_repository.table.c.parameter_3 == PARAMETER_3).execute().first()
    assert ml_testing_dag is not None


def test_get_ml_dag_id_insert_testing_dag_run(ml_testing_dag_repository: MLTestingDagRepository, reset_db: None):
    """ Test creation of MLTestingDagTable for existing MLDagTable """
    MLDagRepository().table.insert().values(id=INSERTED_ML_DAG_ID, parameter_1=PARAMETER_1).execute()

    ml_testing_dag = ml_testing_dag_repository.table.select().where(
        ml_testing_dag_repository.table.c.parameter_3 == PARAMETER_3).execute().first()
    assert ml_testing_dag is None

    MLTestingDag._get_ml_dag_id(engine=ml_testing_dag_repository.metadata.bind,
                                dag_run=Conf(conf={'parameter_1': PARAMETER_1,
                                                   'parameter_3': PARAMETER_3}))

    ml_testing_dag = ml_testing_dag_repository.table.select().where(
        ml_testing_dag_repository.table.c.parameter_3 == PARAMETER_3).execute().first()
    assert ml_testing_dag is not None
