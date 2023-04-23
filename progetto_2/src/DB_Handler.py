import psycopg2
import csv
import pandas as pd

class db_handler():
    def __init__(self, database, user, password, host, database_name):
        self.database = database 
        self.user = user # user name used to authenticate
        self.password = password # password used to authenticate
        self.host = host # database host address (defaults to UNIX socket if not provided)
        self.database_name = database_name # the database name (database is a deprecated alias)
        self.cloud_db = {
            'host':'snuffleupagus.db.elephantsql.com',
            'port': '5432',
            'dbname':'xvglexze',
            'user': 'xvglexze',
            'password': 'zRNmK2sgNOUDF4aqfgPI-lyy59obRG2b',
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
                "rating" VARCHAR(10),
                "reviews" VARCHAR(50),
                "size" VARCHAR(50),
                "installs" VARCHAR(50),
                "type" VARCHAR(10),
                "price" VARCHAR(50),
                "content_rating" VARCHAR(50),
                "genres" VARCHAR(50),
                "last_updated" VARCHAR(50),
                "age_restriction" VARCHAR(50),
                FOREIGN KEY(app_id) REFERENCES app_names(app_id),
                FOREIGN KEY(category_id) REFERENCES categories(category_id)
            """,
        }
        self.tables_columns = {
            "main": ('rating', 'size', 'installs', 'type', 'price', 'content_rating', 'genres', 'last_updates', 'age_restriction'),
            "categories": ('name'),
            "apps": ('name')
        }
        try:
            # Connect to the PostgreSQL server
            self.conn = psycopg2.connect(database=database, user=user, password=password, host=host)
            # Open a cursor to perform database operations
            self.cur = self.conn.cursor()
            # Rollback any open transaction
            self.cur.execute("ROLLBACK")
            # Drop the database if it already exists
            self.cur.execute(f"DROP DATABASE IF EXISTS {database_name};")
            # Execute a CREATE DATABASE command
            self.cur.execute(f"CREATE DATABASE {database_name};")
            # Commit the transaction
            self.conn.commit()
            print("Database created successfully")
        except psycopg2.Error as e:
            print("Error creating database:", e)
        finally:
            if self.conn is not None:
                self.cur.close()
                self.conn.close()

    def create_table(self, query):
        try:
            # Connect to the PostgreSQL server and create a new database
            self.conn = psycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host)
            # Open a cursor to perform database operations
            self.cur = self.conn.cursor()
            # Execute the query to create the table
            self.cur.execute(query)
            # Commit the transaction
            self.conn.commit()
            print("Table created successfully")
        except psycopg2.Error as e:
            print("Error creating table:", e)
        finally:
            if self.conn is not None:
                self.cur.close()
                self.conn.close()

    def insert_values_categories(self, path, query):
        try:
            # Connect to the PostgreSQL server
            self.conn = psycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host)
            # Open a cursor to perform database operations
            self.cur = self.conn.cursor()
            # Read in the CSV file and insert data into the categories table
            with open(path, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                next(reader)  # skip the header row
                for row in reader:
                    category = row[2]
                    self.cur.execute(query, (category, category))
            # Commit the transaction
            self.conn.commit()
            print("Data inserted successfully")
        except psycopg2.Error as e:
            print("Error inserting data:", e)
        finally:
            if self.conn is not None:
                self.cur.close()
                self.conn.close()
    
    def app(self, path, query):
        # Connect to the PostgreSQL server
        self.conn = psycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host)
        # Open a cursor to perform database operations
        self.cur = self.conn.cursor()
        with open(path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            next(reader)  # skip the header row
            num_rows = 0
            for row in reader:
                app_name = row[1]
                self.cur.execute(query, (app_name, app_name))
                num_rows += 1
            self.conn.commit()
        print(f"{num_rows} rows inserted into the database.")


    def insert_values_apps(self, path, query):
        try:
            # Connect to the PostgreSQL server
            self.conn = psycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host)
            # Open a cursor to perform database operations
            self.cur = self.conn.cursor()
            # Read in the CSV file and insert data into the apps table
            with open(path, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                next(reader)  # skip the header row
                for row in reader:
                    Index, app_id, category_id, rating, reviews, size, installs, app_type, price, content_rating, genres, last_updated, age_restriction = row
                    app_id_query = """SELECT "id" FROM Apps WHERE "name" = %s"""
                    category_id_query = """SELECT "Category ID" FROM categories WHERE Name = %s"""
                    self.cur.execute(category_id_query, (category_id,))
                    category_id = self.cur.fetchone()[0]
                    self.cur.execute(app_id_query, (app_id,))
                    app_id = self.cur.fetchone()[0]
                    app_values = (Index, app_id, category_id, rating, reviews, size, installs, app_type, price, content_rating, genres, last_updated, age_restriction)
                    self.cur.execute(query, app_values)
            # Commit the transaction
            self.conn.commit()
            print("Data inserted successfully")
        except psycopg2.Error as e:
            print("Error inserting data:", e)
        finally:
            if self.conn is not None:
                self.cur.close()
                self.conn.close()

    
    def test_query(self, table_name, limit=False):
        try:
            # Connect to the PostgreSQL server
            conn = psycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host)
            # Open a cursor to perform database operations
            cur = conn.cursor()
            # Execute a SELECT query on the table
            if limit==True:
                cur.execute(f"SELECT * FROM {table_name} LIMIT 10;")
            else:
                cur.execute(f"SELECT * FROM {table_name};")
            rows = cur.fetchall()
            # Print the rows returned by the query
            for row in rows:
                print(row)
        except psycopg2.Error as e:
            print("Error executing SELECT query:", e)
        finally:
            if conn is not None:
                cur.close()
                conn.close()

    def read_table(self, table_name):
        try:
            conn = psycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host)
            cur = conn.cursor()
        except psycopg2.Error as e:
            print(f'Unable to connect with Postgres: {e}')

        try:
            cur.execute(f"SELECT * FROM {table_name};")
            data = cur.fetchall()
        except psycopg2.Error as e:
            print(f'Error executing SELECT query: {e}')

        cols = []
        for elt in cur.description:
            cols.append(elt[0])

        cur.close()
        conn.close()

        return pd.DataFrame(data=data, columns=cols)
    
    def upload_cloud(self, df):
        
        df = df.copy()
        df['app_id'] = df.index

        apps_names_df = df.loc[:, ['app_id','app']]
        #print(apps_names_df)

        categories = list(df['category'].unique())
        catgories_df = pd.DataFrame(categories, columns=['categories'])
        catgories_df['category_id'] = catgories_df.index
        #print(catgories_df)

        conn = psycopg2.connect(**self.cloud_db)
        cur = conn.cursor()
        conn.autocommit = True

        for table in self.tables_schemas.keys():
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
            cur.execute(f"CREATE TABLE {table} ({self.tables_schemas[table]});")

        for _, row in catgories_df.iterrows():
            print("""
                INSERT INTO categories(category_id, category_name)
                VALUES (%s, %s);
                """ % (row['category_id'], row['categories'])
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
                """ % (row['app_id'], row['app'])
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
            
        cur.close()
        conn.close()
            
if __name__ == '__main__':
    db = db_handler('postgres', 'postgres', 'postgres', 'localhost', 'prova_db')

    # CATEGORY TABLE
    table_query = """
        CREATE TABLE categories (
            "Category ID" SERIAL PRIMARY KEY,
            Name VARCHAR(256) NOT NULL
        )
    """
    db.create_table(table_query)
    insert_query = """
        INSERT INTO categories (Name)
        SELECT %s
        WHERE NOT EXISTS (
            SELECT 1 FROM categories WHERE Name = %s
        )
    """
    db.insert_values_categories('./database/output/processed_googleplaystore.csv', insert_query)
    db.test_query('categories')


    # APP TABLE
    # Define the table creation query
    table_query = """
        CREATE TABLE apps (
            id SERIAL PRIMARY KEY,
            name VARCHAR(256) NOT NULL
        )
    """
    # Create the table
    db.create_table(table_query)
    # Define the query for inserting data into the table
    insert_query = """
        INSERT INTO apps (name)
        SELECT %s
        WHERE NOT EXISTS (
            SELECT 1 FROM apps WHERE name = %s
        )
    """
    # Insert data from CSV file into the table
    db.app('./database/output/processed_googleplaystore.csv', insert_query)
    db.test_query('apps', True)

    # APPS TABLE
    table_query = """
        CREATE TABLE Main (
        "Index" INT,
        "App ID" INT REFERENCES apps("id"),
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
        )
    """
    db.create_table(table_query)
    query = """INSERT INTO Main (
                    "Index", "App ID", "Category ID", Rating, Reviews, Size, Installs, Type, Price, 
                    "Content Rating", Genres, "Last Updated", "Age Restriction") 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    path = './database/output/processed_googleplaystore.csv'
    db.insert_values_apps(path, query)
    db.test_query('Main', True)

    #REVIEWS TABLE
