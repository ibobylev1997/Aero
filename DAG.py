import datetime
from airflow.decorators import dag
from task import UrltoPostgresOperator

@dag(start_date=datetime.datetime(2023, 3, 16), scheduler_interval='0 0 */12 * *')
def generate_dag():
    UrltoPostgresOperator(
        url = 'https://random-data-api.com/api/cannabis/random_cannabis?size=10'
        target_schema = 'public'
        target_table = 'task'
    )

generate_dag()