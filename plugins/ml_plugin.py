from airflow.plugins_manager import AirflowPlugin

from dags.operators.parametrized_bash_operator import ParametrizedBashOperator


class MLPlugin(AirflowPlugin):
    name = "ml_plugin"
    operators = [ParametrizedBashOperator]