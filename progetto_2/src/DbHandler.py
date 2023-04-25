import psycopg2
import pandas as pd
from typing import Literal

class DbHandler():
    def __init__(self, user, password, dbname, cloud: bool=False):
        self.local_db = {
            'dbname': dbname, # the database name (database is a deprecated alias)
            'user': user, # user name used to authenticate
            'password': password, # password used to authenticate
            'host': 'localhost', # database host address (defaults to UNIX socket if not provided)
            'port': '5432' # connection port number
        }
        self.cloud_db = {
            'dbname':'xvglexze',
            'user': 'xvglexze',
            'password': 'zRNmK2sgNOUDF4aqfgPI-lyy59obRG2b',
            'host':'snuffleupagus.db.elephantsql.com',
            'port': '5432',
            }
        self.tables_schemas = {
            "categories": """
                "category_id" INT PRIMARY KEY,
                "category_name" VARCHAR(256) NOT NULL
            """,
            "app_names": """
                "app_id" INT PRIMARY KEY,
                "app_name" VARCHAR(256) NOT NULL
            """,
            "main": """
                "app_id" INT UNIQUE PRIMARY KEY, 
                "category_id" INT,
                "rating" REAL,
                "reviews" INT,
                "size" INT,
                "installs" INT,
                "type" VARCHAR(10),
                "price" REAL,
                "content_rating" VARCHAR(50),
                "genres" VARCHAR(50),
                "last_updated" DATE,
                "age_restriction" INT,
                FOREIGN KEY(app_id) REFERENCES app_names(app_id),
                FOREIGN KEY(category_id) REFERENCES categories(category_id)
            """,
        }
        self.dtypes = {} # Stores column dtypes in order to ripristinate them when importing database
        self.local_conn = psycopg2.connect(**self.local_db)
        self.local_cur = self.local_conn.cursor()
        if cloud:
            self.cloud_conn = psycopg2.connect(**self.cloud_db)
            self.cloud_cur = self.cloud_conn.cursor()

    def upload(self, df, host: Literal['local', 'cloud']='local'):

        if host == 'local':
            conn = self.local_conn
            cur = self.local_cur
        else:
            conn = self.cloud_conn
            cur = self.cloud_cur

        df = df.copy()
        df['app_id'] = df.index
        self.dtypes = {**self.dtypes, **df.dtypes.to_dict()}

        categories = list(df['category'].unique())
        catgories_df = pd.DataFrame(categories, columns=['categories'])
        catgories_df['category_id'] = catgories_df.index

        for table in self.tables_schemas.keys():
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
            cur.execute(f"CREATE TABLE {table} ({self.tables_schemas[table]});")

        for _, row in catgories_df.iterrows():
            print(
                """
                INSERT INTO categories(category_id, category_name)
                VALUES (%s, %s);
                """ % (
                row['category_id'],
                row['categories']
                )
            )
            cur.execute("""
                INSERT INTO categories(category_id, category_name)
                VALUES (%s, %s);
                """, (
                row['category_id'],
                row['categories']
                )
            )

        for _, row in df.iterrows():
            print("""
                INSERT INTO app_names(app_id, app_name)
                VALUES (%s, %s);
                """ % (
                row['app_id'],
                row['app']
                )
            )
            cur.execute("""
                INSERT INTO app_names(app_id, app_name)
                VALUES (%s, %s);
                """, (
                row['app_id'],
                row['app']
                )
            )
            
            conn.commit()

            print("""
                INSERT INTO main(app_id, category_id, rating, reviews, size, installs, type, price, content_rating, genres, last_updated, age_restriction)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """ % (
                row['app_id'],
                categories.index(row['category']),
                row['rating'],
                row['reviews'],
                row['size'],
                row['installs'],
                row['type'],
                row['price'],
                row['content_rating'],
                row['genres'],
                row['last_updated'],
                row['age_restriction']
                )
            )

            cur.execute("""
                INSERT INTO main(app_id, category_id, rating, reviews, size, installs, type, price, content_rating, genres, last_updated, age_restriction)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (
                row['app_id'],
                categories.index(row['category']),
                row['rating'],
                row['reviews'],
                row['size'],
                row['installs'],
                row['type'],
                row['price'],
                row['content_rating'],
                row['genres'],
                row['last_updated'],
                row['age_restriction']
                )
            )
            
            conn.commit()

    def read_table(self, table_name, host: Literal['local', 'cloud']):

        if host == 'local':
            conn = self.local_conn
            cur = self.local_cur
        else:
            conn = self.cloud_conn
            cur = self.cloud_cur

        cur.execute(f"SELECT * FROM {table_name};")
        data = cur.fetchall()

        cols = []
        for elt in cur.description:
            cols.append(elt[0])

        return pd.DataFrame(data=data, columns=cols)
    
    def download(self, host: Literal['local', 'cloud']):

        if host == 'local':
            conn = self.local_conn
            cur = self.local_cur
        else:
            conn = self.cloud_conn
            cur = self.cloud_cur

        cur.execute("""
        SELECT
            app_id as app_id,
            (SELECT an.app_name FROM app_names an WHERE an.app_id = m.app_id) as app,
            (SELECT c.category_name FROM categories c WHERE c.category_id = m.category_id) as category,
            "rating",
            "reviews",
            "size",
            "installs",
            "type",
            "price",
            "content_rating",
            "genres",
            "last_updated",
            "age_restriction"
        FROM
            main m
        ;
        """)

        data = cur.fetchall()

        cols = []
        for elt in cur.description:
            cols.append(elt[0])

        df = pd.DataFrame(data=data, columns=cols)

        df = df.astype(self.dtypes)

        df.set_index(keys='app_id', inplace = True)

        return df
    