
from src.DataIngestor import DataIngestor
from src.DataPreprocessor import DataPreprocessor
from src.DataVisualizer import DataVisualizer
from src.DataAnalyzer import DataAnalyzer
from src.DataIngestor import DataIngestor
from src.DbHandler import DbHandler

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

di = DataIngestor()
dp = DataPreprocessor()
dv = DataVisualizer(library="seaborn", style='darkgrid', show=False, save=True) 
da = DataAnalyzer()  # Any list of words formatted in one column
dh = DbHandler(user='postgres', password='postgres', dbname='googleplaystore', cloud=True)

#print(df.head())

import pendulum
import pandas as pd

@dag(
    dag_id="project_dag",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2023, 4, 28, tz="UTC"),
    catchup=False,
    #dagrun_timeout=datetime.timedelta(minutes=60),
)


def prj2_main():
    
    df=pd.read_csv(r'./plugins/database/raw/googleplaystore.csv')
    
    '''@task
    def ETL():
        df = di.load_file('./plugins/database/raw/googleplaystore.csv')
        df = dp.pipeline(df)
        dh.upload(df, host='cloud')'''

    #ETL()


    @task
    def upload_from_csv():
        dh.upload_from(df,host='cloud')

    @task
    def data_processor():
        df=dh.download(host='cloud')
        dp.pipeline(df)
        dh.upload(df,host='cloud')

    @task
    def data_visualizer():
        df=dh.download(host='cloud')
        df_all=dh.download('cloud')
        dv.pipeline()


    upload_from_csv()>>data_processor>>data_visualizer


dag = prj2_main()