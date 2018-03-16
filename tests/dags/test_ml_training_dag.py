import datetime
from collections import namedtuple

import pytest
from sqlalchemy import and_

from dags.ml_training_dag import MLTrainingDag
from dags.repositories.base_db import BaseDatabase
from dags.repositories.ml_dag import MLDagTable
from dags.repositories.ml_training_dag import MLTrainingDagTable

ConfTuple = namedtuple('ConfTuple', 'conf')

INSERTED_ML_DAG_ID = 1
INSERTED_ML_TESTING_DAG_ID = 1
PARAMETER_1 = 'test_parameter_1'
PARAMETER_2 = 'test_parameter_2'
NON_EXISTENT_ML_DAG_ID = 100
NON_EXISTENT_PARAMETER_2 = 'non_existent_parameter_2'
NON_EXISTENT_PARAMETER_1 = 'non_existent_parameter_1'
DB_NAME = 'test.db'


# Service should be stateless, so widest scope is appropriate
@pytest.fixture(scope='module')
def ml_training_dag_repository_fixture() -> MLTrainingDagTable:
    """ Fixture that makes MLTrainingDagTable using local sqlite db

    Returns: MLTrainingDagTable

    """
    engine = BaseDatabase.create_engine(login=None,
                                        password=None,
                                        host=DB_NAME,
                                        schema=None,
                                        conn_type='sqlite')

    return MLTrainingDagTable(engine=engine)


@pytest.fixture()
def reset_db(ml_training_dag_repository_fixture: MLTrainingDagTable) -> None:
    """ Resets DB before each test to initial training state """
    ml_training_dag_repository_fixture.metadata.drop_all()
    ml_training_dag_repository_fixture.metadata.create_all()


def test_get_ml_dag_id(ml_training_dag_repository_fixture: MLTrainingDagTable, reset_db: None):
    MLDagTable().table.insert().values(id=INSERTED_ML_DAG_ID, parameter_1=PARAMETER_1).execute()

    ml_training_dag_repository_fixture.table.insert().values(id=INSERTED_ML_TESTING_DAG_ID,
                                                             ml_dag_id=INSERTED_ML_DAG_ID,
                                                             parameter_2=PARAMETER_2,
                                                             datetime_created=datetime.datetime.utcnow()).execute()

    ml_dag_id = MLTrainingDag._get_ml_dag_id(engine=ml_training_dag_repository_fixture.metadata.bind,
                                             dag_run=ConfTuple(conf={'parameter_1': PARAMETER_1,
                                                                     'parameter_2': PARAMETER_2}))

    assert ml_dag_id == INSERTED_ML_DAG_ID


def test_get_dag_run_id_insert_dag_run_and_training_dag_run(ml_training_dag_repository_fixture: MLTrainingDagTable,
                                                            reset_db: None):
    """ Test creation of both MLDagTable and MLTrainingDagTable """
    ml_dag = MLDagTable().table.select().where(
        and_(MLDagTable.table.c.id == INSERTED_ML_DAG_ID,
             MLDagTable.table.c.parameter_1 == PARAMETER_1)).execute().first()
    assert ml_dag is None

    ml_training_dag = ml_training_dag_repository_fixture.table.select().where(
        ml_training_dag_repository_fixture.table.c.parameter_2 == PARAMETER_2).execute().first()
    assert ml_training_dag is None

    MLTrainingDag._get_ml_dag_id(engine=ml_training_dag_repository_fixture.metadata.bind,
                                 dag_run=ConfTuple(conf={'parameter_1': PARAMETER_1,
                                                         'parameter_2': PARAMETER_2}))

    ml_dag = MLDagTable().table.select().where(
        and_(MLDagTable.table.c.id == INSERTED_ML_DAG_ID,
             MLDagTable.table.c.parameter_1 == PARAMETER_1)).execute().first()
    assert ml_dag is not None

    ml_training_dag = ml_training_dag_repository_fixture.table.select().where(
        ml_training_dag_repository_fixture.table.c.parameter_2 == PARAMETER_2).execute().first()
    assert ml_training_dag is not None


def test_get_dag_run_id_insert_training_dag_run(ml_training_dag_repository_fixture: MLTrainingDagTable, reset_db: None):
    """ Test creation of MLTrainingDagTable for existing MLDagTable """
    MLDagTable().table.insert().values(id=INSERTED_ML_DAG_ID, parameter_1=PARAMETER_1).execute()

    ml_training_dag = ml_training_dag_repository_fixture.table.select().where(
        ml_training_dag_repository_fixture.table.c.parameter_2 == PARAMETER_2).execute().first()
    assert ml_training_dag is None

    MLTrainingDag._get_ml_dag_id(engine=ml_training_dag_repository_fixture.metadata.bind,
                                 dag_run=ConfTuple(conf={'parameter_1': PARAMETER_1,
                                                         'parameter_2': PARAMETER_2}))

    ml_training_dag = ml_training_dag_repository_fixture.table.select().where(
        ml_training_dag_repository_fixture.table.c.parameter_2 == PARAMETER_2).execute().first()
    assert ml_training_dag is not None
