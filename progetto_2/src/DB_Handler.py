import psycopg2
import csv
import pandas as pd
import os


class DB_Handler:
    """A class for handling database operations."""
    def __init__(self, database, user, password, host, port, path:str):
        """
        Initialize the DB_Handler object.

        Args:
            database (str): The name of the database.
            user (str): The username for connecting to the database.
            password (str): The password for connecting to the database.
            host (str): The host address of the database server.
            port (int): The port number to connect to the database.
        """
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.path = path
    

    def open_connection(self):
        """
        Open a connection to the database.
        """
        try:
            self.conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
            self.cur = self.conn.cursor()
            self.cur.execute("ROLLBACK")
            print("Connected to database successfully")
        except psycopg2.Error as e:
            print("Error opening connection:", e)

    def create_database(self, database_name):
        """
        Create a new database.

        Args:
            database_name (str): The name of the database to create.
        """
        try:
            self.cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s;", (database_name,))
            database_exists = self.cur.fetchone()
            if not database_exists:
                self.cur.execute(f"CREATE DATABASE {database_name}")
                self.conn.commit()
                print(f"{database_name} created successfully")
            else:
                print(f"{database_name} already exists")
        except psycopg2.Error as e:
            print("Error creating database:", e)


    def run_data_pipeline(self):
        """
        Run the data pipeline to create tables and insert data into them.
        """
        try:
            self.open_connection()
        
            # Create categories table
            categories_table_query = """
                CREATE TABLE IF NOT EXISTS categories (
                    "Category ID" SERIAL PRIMARY KEY,
                    Name VARCHAR(256) NOT NULL
                )
            """
            self.create_table(categories_table_query, 'categories')
            
            # Insert values into categories table
            categories_insert_query = """
                INSERT INTO categories (Name)
                SELECT %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM categories WHERE Name = %s
                )
            """            

            self.insert_values_categories(self.path, categories_insert_query)
            
            # Create apps table
            apps_table_query = """
                CREATE TABLE IF NOT EXISTS apps (
                    "App ID" SERIAL PRIMARY KEY,
                    name VARCHAR(256) NOT NULL
                )
            """
            self.create_table(apps_table_query, 'apps')
            
            # Insert values into apps table
            apps_insert_query = """
                INSERT INTO apps (name)
                SELECT %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM apps WHERE name = %s
                )
            """
            self.insert_values_apps(self.path, apps_insert_query)
            
            # Create main table
            main_table_query = """
                CREATE TABLE IF NOT EXISTS main (
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
            self.create_table(main_table_query, 'main')
            
            # Insert values into main table
            main_insert_query = """
                INSERT INTO main (
                    "Index", "App ID", "Category ID", Rating, Reviews, Size, Installs, Type, Price, 
                    "Content Rating", Genres, "Last Updated", "Age Restriction") 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            self.insert_values_main(self.path, main_insert_query)

            self.conn.commit()
            print("Data pipeline executed successfully")
            
        except psycopg2.Error as e:
            print("Error creating database:", e)
   

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
            self.cur.execute(f"DROP TABLE IF EXISTS {table_name};")
            self.cur.execute(query)
            self.conn.commit()
            print("Table created successfully")
        except psycopg2.Error as e:
            print("Error creating table:", e)

    def insert_values_categories(self, path, query):
        """
        Insert values into the categories table.

        Args:
            path (str): The path to the CSV file containing the data.
            query (str): The SQL query to insert values into the table.

        Raises:
            psycopg2.Error: If there's an error inserting the values.
        """
        try:
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
    
    def insert_values_apps(self, path, query):
        """
        Insert values into the apps table.

        Args:
            path (str): The path to the CSV file containing the data.
            query (str): The SQL query to insert values into the table.

        Raises:
            psycopg2.Error: If there's an error inserting the values.
        """
        try:
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


    def insert_values_main(self, path, query):
        """
        Insert values into the main table.

        Args:
            path (str): The path to the CSV file containing the data.
            query (str): The SQL query to insert values into the table.

        Raises:
            psycopg2.Error: If there's an error inserting the values.
        """
        try:
            with open(path, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                next(reader)  # Skip header row
                for row in reader:
                    Index, app_id, category_id, rating, reviews, size, installs, app_type, price, content_rating, genres, last_updated, age_restriction = row
                    app_id_query = """SELECT "App ID" FROM apps WHERE "name" = %s"""
                    category_id_query = """SELECT "Category ID" FROM categories WHERE Name = %s"""
                    self.cur.execute(category_id_query, (category_id,))
                    category_id = self.cur.fetchone()[0]
                    self.cur.execute(app_id_query, (app_id,))
                    app_id = self.cur.fetchone()
                    app_values = (Index, app_id, category_id, rating, reviews, size, installs, app_type, price, content_rating, genres, last_updated, age_restriction)
                    self.cur.execute(query, app_values)
            self.conn.commit()
            print("Data inserted successfully")
        except psycopg2.Error as e:
            print("Error inserting data:", e)

    def insert_values_reviews(self, path, query):
        """
        Insert values into the reviews table.

        Args:
            path (str): The path to the CSV file containing the data.
            query (str): The SQL query to insert values into the table.

        Raises:
            psycopg2.Error: If there's an error inserting the values.
        """
        try:
            with open(path, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                next(reader) 
                for row in reader:
                    app_id, translated_review = row
                    app_id_query = """SELECT "App ID" FROM apps WHERE "name" = %s"""
                    self.cur.execute(app_id_query, (app_id,))
                    app_id = self.cur.fetchone()[0]
                    app_values = (app_id, translated_review)
                    self.cur.execute(query, app_values)
            print("Values inserted into table reviews successfully.")
        except psycopg2.Error as e:
            print("Error creating table:", e)

    def read_table(self, table_name):
        """
        Read data from a table.

        Args:
            table_name (str): The name of the table.

        Returns:
            pandas.DataFrame: The data read from the table.
        """
        try:
            self.cur.execute(f"SELECT * FROM {table_name} LIMIT 10;")
            data = self.cur.fetchall()
        except psycopg2.Error as e:
            print("Error executing SELECT query:", e)

        cols = []
        for elt in self.cur.description:
            cols.append(elt[0])

        return pd.DataFrame(data=data, columns=cols)
    
    def close_connection(self):
        """
        Close the database connection.
        """
        if self.conn is not None:
                self.cur.close()
                self.conn.close()
        print('Connection closed')
    
    def execute_query(self, query):
        """
        Execute a custom SQL query.

        Args:
            query (str): The SQL query to execute.
        """
        try:
            self.cur.execute(query)
            self.conn.commit()
        except Exception as e:
            self.connection.rollback()
            raise e
