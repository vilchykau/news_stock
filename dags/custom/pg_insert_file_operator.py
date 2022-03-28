'''Operator for creating file contains SQL script that will refresh data in Postgress'''
from airflow.models import BaseOperator

class HelloOperator(BaseOperator):
    '''Operator for creating file contains SQL script that will refresh data in Postgress'''
    template_fields = ['input_file', 'output_file']

    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        message = f"Hello from {self.name}"
        print(message)
        return message