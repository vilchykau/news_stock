'''An operator like PostgressOperator supports templates'''
from airflow.models import BaseOperator
from airflow.providers.postgres.operators.postgres import PostgresHook


class PostgressTempOperator(BaseOperator):
    '''An operator like PostgressOperator supports templates'''
    template_fields = ['source']

    def __init__(self, source: str, conn_id: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.source = source
        self.conn_id = conn_id

    def __read_file(self) -> str:
        seq = ''
        with open(self.source, 'r', encoding="utf-8") as f:
            seq = '\n'.join(f.readlines())
        return seq

    def execute(self, context):
        seq = self.source
        if self.source.lower().endswith('.sql') is True:
            seq = self.__read_file()

        hook = PostgresHook(postgres_conn_id=self.conn_id)
        hook.run(sql=seq)
