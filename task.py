import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class UrltoPostgresOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,
                url,
                target_schema,
                target_table,
                *args,
                **kwargs
                ):
        super().__init__(*args, **kwargs)
        self.url = url
        self.target_schema = target_schema
        self.target_table = target_table

    def get_data_from_api(self):

        response = requests.get(self.url)

        if response.status_code == 200:
            data = response.json()
            for item in data: #creating generator
                yield item
        else:
            print('Error: ' + str(response.status_code))
        
    def load_data_to_postgres(self):

        source = PostgresHook(postgres_conn_id='my_postgres_connection')

        cursor = conn.get_cursor()

        for item in get_data_from_api():
            query = "INSERT INTO {}.{} {} VALUES {}".format(self.target_schema, \
                                                            self.target_table,[i for i in item.keys()], \
                                                            [item[i] for i in item.keys()]) #creating query for inserting
            query = query.replace("[", "(").replace("]", ")") 

            cursor.execute(query)

        cursor.close()
        source.commit()
        
     def execute(self, context):
        self.load_data_to_postgres()
