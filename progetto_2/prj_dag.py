
from src.DataIngestor import DataIngestor
from src.DataPreprocessor import DataPreprocessor
from src.DataVisualizer import DataVisualizer
from src.DataAnalyzer import DataAnalyzer
from src.DataIngestor import DataIngestor
from src.DB_Handler import DB_Handler

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

di = DataIngestor()
dp = DataPreprocessor()
dv = DataVisualizer(library="seaborn", style='darkgrid', show=False, save=True) 
da = DataAnalyzer()  # Any list of words formatted in one column
#dh = DB_Handler(database = 'postgres', user = 'postgres', password='c', host='localhost', database_name = 'postgres')

import pendulum

@dag(
    dag_id="project_dag",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2023, 4, 28, tz="UTC"),
    catchup=False,
    #dagrun_timeout=datetime.timedelta(minutes=60),
)


def prj2_main():
    
    @task
    def data_processor():
        df=di.load_from_file(path=r'./plugins/database/raw/googleplaystore.csv')
        dp.pipeline(df)

        table_query = """
        CREATE TABLE categories (
        "Category ID" SERIAL PRIMARY KEY,
        Name VARCHAR(256) NOT NULL
        )"""

        '''table_query = """
        CREATE TABLE Main (
        "Index" INT,
        "App ID" INT REFERENCES apps("App ID"),
        "Category ID" INT REFERENCES categories("Category ID"),
        Rating VARCHAR(10),
        Reviews VARCHAR(50),
        Size VARCHAR(50),
        Installs VARCHAR(50),
        Type VARCHAR(10),
        Price VARCHAR(50),
        "Content Rating" VARCHAR(50),
        Genres VARCHAR(50),
        "Last Updated" VARCHAR(50),
        "Age Restriction" VARCHAR(50)
        )"""'''

        insert_categoryID_query = """
        INSERT INTO categories (Name)
        SELECT %s
        WHERE NOT EXISTS (
        SELECT 1 FROM categories WHERE Name = %s
        )"""

        '''insert_values_query = """INSERT INTO Main (
        Index, "App ID", "Category ID", Rating, Reviews, Size, Installs, Type, Price, 
        "Content Rating", Genres, "Last Updated", "Age Restriction") 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """'''
        
        dh.create_table(table_query)

        dh.insert_values_categories(path=r'./plugins/database/output/processed_googleplaystore.csv',query=insert_categoryID_query)

    @task
    def data_visualizer():
        df=dh.download(host='cloud')
        df_all=dh.download('cloud')
        dv.pipeline()


    data_processor>>data_visualizer


dag = prj2_main()