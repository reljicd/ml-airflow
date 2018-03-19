def get_ml_dag_id(parent_dag_id: str, **kwargs) -> int:
    """ Extracts ml_dag_id either from kwargs or from XCom

    Args:
        parent_dag_id:
        **kwargs:

    Returns: ml_dag_id

    """
    if 'ml_dag_id' in kwargs:
        ml_dag_id = kwargs['ml_dag_id']
    else:
        ml_dag_id = kwargs['task_instance'].xcom_pull(task_ids='initializer',
                                                      key='ml_dag_id',
                                                      dag_id=parent_dag_id)

    return ml_dag_id
