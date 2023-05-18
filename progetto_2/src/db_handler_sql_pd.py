import os
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy import exists, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship

Base = declarative_base()

class Category(Base):
    __tablename__ = 'categories'

    id = Column(Integer, primary_key=True)
    name = Column(String(256), nullable=False)

class App(Base):
    __tablename__ = 'apps'

    id = Column(Integer, primary_key=True)
    name = Column(String(256), nullable=False)

class Main(Base):
    __tablename__ = 'main'

    index = Column(Integer, primary_key=True)
    app_id = Column(Integer, ForeignKey('apps.id'), nullable=False)
    category_id = Column(Integer, ForeignKey('categories.id'), nullable=False)
    rating = Column(String(10))
    reviews = Column(String(50))
    size = Column(String(50))
    installs = Column(String(50))
    app_type = Column(String(10))
    price = Column(String(50))
    content_rating = Column(String(50))
    genres = Column(String(50))
    last_updated = Column(String(50))
    age_restriction = Column(String(50))

    app = relationship("App")
    category = relationship("Category")


class DB_Handler_cloud:
    """A class for handling database operations."""
    def __init__(self, database_url, file_path):
        self.engine = create_engine(database_url)
        self.Session = sessionmaker(bind=self.engine)
        self.file_path = file_path

    def create_table(self, table_class):
        inspector = inspect(self.engine)
        table_name = table_class.__tablename__
        if not inspector.has_table(table_name):
            table_class.__table__.create(self.engine)
            print(f"{table_class.__name__} created successfully")
        else:
            print(f"{table_class.__name__} already exists")

    def insert_values(self, session, data, model, column_names, table):
        if table == 'main':
            for _, row in data.iterrows():
                conditions = [getattr(model, column) == row[column] for column in column_names]
                record_exists = session.query(exists().where(and_(*conditions))).scalar()
                if not record_exists:
                    obj = model(**{column: row[column] for column in column_names})
                    session.add(obj)
            session.commit()
            print(f"Data inserted successfully in {table}")
        else:
            for _, row in data.iterrows():
                record_exists = session.query(exists().where(model.name == row[column_names[0]])).scalar()
                if not record_exists:
                    obj = model(**row)
                    session.add(obj)
            session.commit()
            print(f"Data inserted successfully in {table}")

    def run_data_pipeline(self):
        session = self.Session()
        Base.metadata.create_all(self.engine)
        path = os.path.join(self.file_path, 'database/output/processed_googleplaystore.csv')
        

        # Create tables
        self.create_table(Category)
        self.create_table(App)
        self.create_table(Main)

        # Insert categories
        categories_data = pd.read_csv(path, usecols=[2])  # Adjust column index
        categories_data = categories_data.drop_duplicates()
        self.insert_values(session, categories_data, Category, ['Category'], 'categories')
        print(categories_data.head(10))

        # Insert apps
        apps_data = pd.read_csv(path, usecols=[1])  # Adjust column index
        apps_data = apps_data.drop_duplicates()
        self.insert_values(session, apps_data, App, ['App'], 'apps')
        print(apps_data.head(10))


        # Insert main data
        main_data = pd.read_csv(path)
        main_data = main_data.drop_duplicates()
        main_data = main_data.merge(categories_data, how='left', left_on='Category', right_on='Category')
        main_data = main_data.merge(apps_data, how='left', left_on='App', right_on='App')
        main_data = main_data.drop(columns=['Unnamed: 0'])
        print(main_data.head(10))
        #self.insert_values(session, main_data, Main, ['App', 'Category'], 'main')
        df = self.read_table(App)
        print(df.head(3))
        df = self.read_table(Category)
        print(df.head(3))
        #df = self.read_table(Main)
        #print(df.head(3))

        print("Data pipeline executed successfully")
        session.close()

    def read_table(self, table_name):
        with self.Session() as session:
            data = session.query(table_name).limit(10).all()
            df = pd.DataFrame([row.__dict__ for row in data])
            df = df.drop('_sa_instance_state', axis=1)
        return df

    def execute_query(self, query):
        with self.Session() as session:
            session.execute(query)
            session.commit()