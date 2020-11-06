from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTableOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query=[],
                 *args,**kwargs):

        super(CreateTableOperator,self).__init__(*args,**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query

    def execute(self,context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for create_query in self.sql_query:
            redshift_hook.run(create_query)
        self.log.info("Tables created successfully!!!")