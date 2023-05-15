from src.DataIngestor import DataIngestor
from src.DataPreprocessor import DataPreprocessor
from src.DataVisualizer import DataVisualizer
from src.DataAnalyzer import DataAnalyzer
from src.DataIngestor import DataIngestor
from src.DB_Handler import DB_Handler

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

di = DataIngestor()
dp = DataPreprocessor()
dv = DataVisualizer(library="seaborn", style='darkgrid', show=False, save=True) 
da = DataAnalyzer()
#dh = DB_Handler(database='postgres', user='postgres', password='c', host='5434', database_name='googleplaystore')

default_args = {
    'start_date': datetime(2023, 4, 28),
    'schedule_interval': "0 0 * * *",
    'catchup': False,
}

with DAG("dag_progetto_Team_2", default_args=default_args) as dag:
    
    def data_processor():

        df = di.load_file(path='airflow/dags/database/raw/googleplaystore.csv')
        df = dp.pipeline(df)
        di.save_file(df, 'airflow/dags/database/output/processed_googleplaystore.csv')

        df_reviews = di.load_file(path='airflow/dags/database/raw/googleplaystore_user_reviews.csv')
        df_reviews = dp.pipeline_reviews(df_reviews) 
        di.save_file(df_reviews, 'airflow/dags/database/output/processed_reviews.csv')

    def data_analyzer():

        df = di.load_file(path='airflow/dags/database/output/processed_googleplaystore.csv')
        df_reviews = di.load_file('airflow/dags/database/output/processed_reviews.csv')
        negative_words = di.load_file('airflow/dags/database/raw/n.xlsx')
        positive_words = di.load_file('airflow/dags/database/raw/p.xlsx')
        df_reviews, df_sentiment, df_all = da.pipeline(df, df_reviews, n_words= negative_words, p_words= positive_words)
        di.save_file(df_all, 'airflow/dags/database/output/googleplaystore_sentiment.csv')

    def data_visualizer():
        '''df = dh.read_table('categories')
        df_all = dh.read_table('categories')
        dv.pipeline()'''

    data_processing_task = PythonOperator(
        task_id='data_processor',
        python_callable=data_processor,
    )

    data_ananyzing_task = PythonOperator(
        task_id='data_analyzer',
        python_callable=data_analyzer
    )

    data_visualization_task = PythonOperator(
        task_id='data_visualizer',
        python_callable=data_visualizer,
    )

    data_processing_task >> data_ananyzing_task
