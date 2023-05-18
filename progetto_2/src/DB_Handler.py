import psycopg2
import csv
import pandas as pd

class DB_Handler:
    """A class for handling database operations."""
    def __init__(self, database, user, password, host, database_name):
        """
        Initialize the DB_Handler object.

        Args:
            database (str): The name of the default database.
            user (str): The username for accessing the database.
            password (str): The password for accessing the database.
            host (str): The host address of the database server.
            database_name (str): The name of the database to be created.

        Raises:
            psycopg2.Error: If there's an error creating the database.
        """
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.database_name = database_name
        try:
            self.conn = psycopg2.connect(database=database, user=user, password=password, host=host)
            self.cur = self.conn.cursor()
            self.cur.execute("ROLLBACK")
            self.cur.execute(f"DROP DATABASE IF EXISTS {database_name};")
            self.cur.execute(f"CREATE DATABASE {database_name};")
            self.conn.commit()
            print("Local Database created successfully")
        except psycopg2.Error as e:
            print("Error creating database:", e)
        finally:
            if self.conn is not None:
                self.cur.close()
                self.conn.close()

    def run_data_pipeline(self):
        """
        Execute the data pipeline to create tables and insert values.

        Raises:
            psycopg2.Error: If there's an error creating tables or inserting values.
        """
        categories_table_query = """
            CREATE TABLE categories (
                "Category ID" SERIAL PRIMARY KEY,
                Name VARCHAR(256) NOT NULL
            )
        """
        self.create_table(categories_table_query, 'categories')
        categories_insert_query = """
            INSERT INTO categories (Name)
            SELECT %s
            WHERE NOT EXISTS (
                SELECT 1 FROM categories WHERE Name = %s
            )
        """
        self.insert_values_categories('./database/output/processed_googleplaystore.csv', categories_insert_query)
        
        apps_table_query = """
            CREATE TABLE apps (
                "App ID" SERIAL PRIMARY KEY,
                name VARCHAR(256) NOT NULL
            )
        """
        self.create_table(apps_table_query, 'apps')
        apps_insert_query = """
            INSERT INTO apps (name)
            SELECT %s
            WHERE NOT EXISTS (
                SELECT 1 FROM apps WHERE name = %s
            )
        """
        self.insert_values_apps('./database/output/processed_googleplaystore.csv', apps_insert_query)
        
        main_table_query = """
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
            )
        """
        self.create_table(main_table_query, 'Main')
        main_insert_query = """
            INSERT INTO Main (
                "Index", "App ID", "Category ID", Rating, Reviews, Size, Installs, Type, Price, 
                "Content Rating", Genres, "Last Updated", "Age Restriction") 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        self.insert_values_main('./database/output/processed_googleplaystore.csv', main_insert_query)

    def create_table(self, query, table_name):
        """
        Create a table in the database.

        Args:
            query (str): The SQL query to create the table.
            table_name (str): The name of the table to be created.

        Raises:
            psycopg2.Error: If there's an error creating the table.
        """
        try:
            self.conn = psycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host)
            self.cur = self.conn.cursor()
            self.cur.execute(f"DROP TABLE IF EXISTS {table_name};")
            self.cur.execute(query)
            self.conn.commit()
            print("Table created successfully")
        except psycopg2.Error as e:
            print("Error creating table:", e)
        finally:
            if self.conn is not None:
                self.cur.close()
                self.conn.close()

    def insert_values_categories(self, path, query):
        """
        Insert values into the categories table from a CSV file.

        Args:
            path (str): The path to the CSV file containing the data.
            query (str): The SQL query to insert values into the table.

        Raises:
            psycopg2.Error: If there's an error inserting the data.
        """
        try:
            self.conn = psycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host)
            self.cur = self.conn.cursor()
            with open(path, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                next(reader)  
                for row in reader:
                    category = row[2]
                    self.cur.execute(query, (category, category))
            self.conn.commit()
            print("Data inserted successfully")
        except psycopg2.Error as e:
            print("Error inserting data:", e)
        finally:
            if self.conn is not None:
                self.cur.close()
                self.conn.close()
    
    def insert_values_apps(self, path, query):
        """
        Insert values into the apps table from a CSV file.

        Args:
            path (str): The path to the CSV file containing the data.
            query (str): The SQL query to insert values into the table.

        Raises:
            psycopg2.Error: If there's an error inserting the data.
        """
        try:
            self.conn = psycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host)
            self.cur = self.conn.cursor()
            with open(path, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                next(reader)  
                num_rows = 0
                for row in reader:
                    app_name = row[1]
                    self.cur.execute(query, (app_name, app_name))
                    num_rows += 1
                self.conn.commit()
                print("Data inserted successfully")
        except psycopg2.Error as e:
            print("Error inserting data:", e)
        finally:
            if self.conn is not None:
                self.cur.close()
                self.conn.close()


    def insert_values_main(self, path, query):
        """
        Insert values into the main table from a CSV file.

        Args:
            path (str): The path to the CSV file containing the data.
            query (str): The SQL query to insert values into the table.

        Raises:
            psycopg2.Error: If there's an error inserting the data.
        """
        try:
            self.conn = psycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host)
            self.cur = self.conn.cursor()
            with open(path, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                next(reader)  
                for row in reader:
                    Index, app_id, category_id, rating, reviews, size, installs, app_type, price, content_rating, genres, last_updated, age_restriction = row
                    app_id_query = """SELECT "App ID" FROM Apps WHERE "name" = %s"""
                    category_id_query = """SELECT "Category ID" FROM categories WHERE Name = %s"""
                    self.cur.execute(category_id_query, (category_id,))
                    category_id = self.cur.fetchone()[0]
                    self.cur.execute(app_id_query, (app_id,))
                    app_id = self.cur.fetchone()[0]
                    app_values = (Index, app_id, category_id, rating, reviews, size, installs, app_type, price, content_rating, genres, last_updated, age_restriction)
                    self.cur.execute(query, app_values)
            self.conn.commit()
            print("Data inserted successfully")
        except psycopg2.Error as e:
            print("Error inserting data:", e)
        finally:
            if self.conn is not None:
                self.cur.close()
                self.conn.close()

    def insert_values_reviews(self, path, query):
        """
        Insert values into the reviews table from a CSV file.

        Args:
            path (str): The path to the CSV file containing the data.
            query (str): The SQL query to insert values into the table.

        Raises:
            psycopg2.Error: If there's an error inserting the data.
        """
        try:
            self.conn = psycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host)
            self.cur = self.conn.cursor()
            with open(path, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                next(reader) 
                for row in reader:
                    app_id, translated_review = row
                    app_id_query = """SELECT "App ID" FROM Apps WHERE "name" = %s"""
                    self.cur.execute(app_id_query, (app_id,))
                    app_id = self.cur.fetchone()[0]
                    app_values = (app_id, translated_review)
                    self.cur.execute(query, app_values)
            print("Values inserted into table reviews successfully.")
        except psycopg2.Error as e:
            print("Error creating table:", e)
        finally:
            if self.conn is not None:
                self.cur.close()
                self.conn.close()

    def read_table(self, table_name):
        """
        Read and retrieve data from a table.

        Args:
            table_name (str): The name of the table to read.

        Returns:
            pandas.DataFrame: A DataFrame containing the retrieved data.

        Raises:
            psycopg2.Error: If there's an error executing the SELECT query.
        """
        try:
            conn = psycopg2.connect(database=self.database_name, user=self.user, password=self.password, host=self.host)
            cur = conn.cursor()
            cur.execute(f"SELECT * FROM {table_name} LIMIT 10;")
            data = cur.fetchall()
        except psycopg2.Error as e:
            print("Error executing SELECT query:", e)

        cols = []
        for elt in cur.description:
            cols.append(elt[0])

        return pd.DataFrame(data=data, columns=cols)