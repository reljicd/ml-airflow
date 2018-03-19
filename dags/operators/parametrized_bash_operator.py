from typing import Callable

from airflow import LoggingMixin
from airflow.operators.bash_operator import BashOperator
from airflow.utils.decorators import apply_defaults


class ParametrizedBashOperator(BashOperator, LoggingMixin):
    """ Customized BashOperator, for additional parametrization of Spark Bash calls
    """
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(self, parameters_provider: Callable, *args, **kwargs):
        super(ParametrizedBashOperator, self).__init__(*args, **kwargs)
        self.parameters_provider = parameters_provider

    def execute(self, context):
        """ Overridden BashOperator execute method """

        additional_parameters = self.parameters_provider(**context)

        self.log.info(f'additional_parameters: {additional_parameters}')

        self.bash_command += f' {additional_parameters}'

        return super(ParametrizedBashOperator, self).execute(context)
